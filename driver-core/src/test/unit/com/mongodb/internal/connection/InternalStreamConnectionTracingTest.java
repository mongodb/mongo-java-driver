/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.internal.connection;

import com.mongodb.MongoInternalException;
import com.mongodb.MongoNamespace;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.connection.MessageSequences.EmptyMessageSequences;
import com.mongodb.internal.observability.micrometer.Span;
import com.mongodb.internal.observability.micrometer.TraceContext;
import com.mongodb.internal.observability.micrometer.TracingManager;
import com.mongodb.internal.thread.AsyncClientExecutor;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.lang.Nullable;
import com.mongodb.observability.micrometer.MongodbObservationContext;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.ByteBuf;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.connection.ClusterConnectionMode.SINGLE;
import static com.mongodb.internal.connection.InternalStreamConnectionTestHelper.createOpenConnection;
import static com.mongodb.internal.connection.InternalStreamConnectionTestHelper.createPingCommand;
import static com.mongodb.internal.connection.InternalStreamConnectionTestHelper.createResponseBody;
import static com.mongodb.internal.connection.InternalStreamConnectionTestHelper.createValidResponseHeader;
import static com.mongodb.internal.connection.MessageHelper.findTelemetrySectionDocument;
import static com.mongodb.internal.mockito.MongoMockito.mock;
import static com.mongodb.internal.operation.ServerVersionHelper.LATEST_WIRE_VERSION;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Connection-level OTel trace-context propagation (DRIVERS-3454): the command span is created before encoding and
 * passed to the traced {@code encode}, so the sent OP_MSG carries the telemetry section, and the span ends exactly
 * once on both the sync and async paths.
 */
class InternalStreamConnectionTracingTest {

    private static final String TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    private static final BsonDocument EXPECTED_TELEMETRY_DOCUMENT =
            new BsonDocument("otel", new BsonDocument("traceparent", new BsonString(TRACEPARENT)));

    /**
     * Verifies the connection-level tracing wiring end to end on the wire bytes: the sync path must create the
     * command span before encoding and pass it to the traced {@code encode} overload, so the sent OP_MSG carries
     * the kind-3 telemetry section. A regression to the 2-arg {@code encode} (or creating the span after
     * encoding) would pass all message-level tests but silently stop emitting the section.
     */
    @Test
    @DisplayName("Sync: sent message carries telemetry section from the command span; span ends exactly once")
    void syncSendAndReceiveAttachesTelemetrySectionAndEndsSpanOnce() {
        AtomicInteger readCallCount = new AtomicInteger();
        CapturingTestStream stream = new CapturingTestStream() {
            @Override
            public ByteBuf read(final int numBytes, final OperationContext operationContext) {
                if (readCallCount.incrementAndGet() == 1) {
                    return createValidResponseHeader(getCommandMessageId());
                } else {
                    return createResponseBody(new BsonDocument("ok", new BsonInt32(1)));
                }
            }
        };
        InternalStreamConnection connection = createOpenConnection(stream);
        CommandMessage commandMessage = createPingCommand();
        stream.setCommandMessageId(commandMessage.getId());
        RecordingSpan span = new RecordingSpan();

        connection.sendAndReceive(commandMessage, new BsonDocumentCodec(), createTracedOperationContext(span));

        assertEquals(EXPECTED_TELEMETRY_DOCUMENT, findTelemetrySectionDocument(stream.getWrittenBytes()),
                "the sent message must carry the telemetry section with the span's traceparent");
        assertEquals(1, span.endCount.get(), "span must end exactly once");
        assertEquals(0, span.errorCount.get());
        assertEquals(span.openScopeCount.get(), span.closeScopeCount.get(), "scopes must be balanced");
        connection.close();
    }

    /**
     * Verifies exactly-once span termination when the sync send fails: the widened {@code catch (Throwable)}
     * must fire {@code error()}/{@code closeScope()}/{@code end()} once each, with no double-firing from the
     * receive path (which is never reached).
     */
    @Test
    @DisplayName("Sync: span errors and ends exactly once when the stream write fails")
    void syncSendFailureEndsSpanExactlyOnceWithError() {
        RuntimeException writeFailure = new RuntimeException("simulated write failure");
        CapturingTestStream stream = new CapturingTestStream() {
            @Override
            public void write(final List<ByteBuf> buffers, final OperationContext operationContext) {
                throw writeFailure;
            }
        };
        InternalStreamConnection connection = createOpenConnection(stream);
        CommandMessage commandMessage = createPingCommand();
        RecordingSpan span = new RecordingSpan();

        MongoInternalException thrown = assertThrows(MongoInternalException.class,
                () -> connection.sendAndReceive(commandMessage, new BsonDocumentCodec(), createTracedOperationContext(span)));
        assertSame(writeFailure, thrown.getCause());
        assertEquals(1, span.errorCount.get(), "span must record the failure exactly once");
        assertEquals(1, span.endCount.get(), "span must end exactly once");
        assertEquals(span.openScopeCount.get(), span.closeScopeCount.get(), "scopes must be balanced");
        connection.close();
    }

    /**
     * Async counterpart of {@link #syncSendAndReceiveAttachesTelemetrySectionAndEndsSpanOnce()}.
     */
    @Test
    @DisplayName("Async: sent message carries telemetry section from the command span; span ends exactly once")
    void asyncSendAndReceiveAttachesTelemetrySectionAndEndsSpanOnce() {
        AtomicInteger readAsyncCallCount = new AtomicInteger();
        CapturingTestStream stream = new CapturingTestStream() {
            @Override
            public void readAsync(final int numBytes, final OperationContext operationContext,
                    final AsyncCompletionHandler<ByteBuf> handler) {
                if (readAsyncCallCount.incrementAndGet() == 1) {
                    handler.completed(createValidResponseHeader(getCommandMessageId()));
                } else {
                    handler.completed(createResponseBody(new BsonDocument("ok", new BsonInt32(1))));
                }
            }
        };
        InternalStreamConnection connection = createOpenConnection(stream);
        CommandMessage commandMessage = createPingCommand();
        stream.setCommandMessageId(commandMessage.getId());
        RecordingSpan span = new RecordingSpan();

        FutureResultCallback<BsonDocument> callback = new FutureResultCallback<>();
        connection.sendAndReceiveAsync(commandMessage, new BsonDocumentCodec(), createTracedOperationContext(span), callback);
        callback.get(5, TimeUnit.SECONDS);

        assertEquals(EXPECTED_TELEMETRY_DOCUMENT, findTelemetrySectionDocument(stream.getWrittenBytes()),
                "the sent message must carry the telemetry section with the span's traceparent");
        assertEquals(1, span.endCount.get(), "span must end exactly once");
        assertEquals(0, span.errorCount.get());
        connection.close();
    }

    /**
     * Async counterpart of {@link #syncSendFailureEndsSpanExactlyOnceWithError()}: a failed async write must
     * error and end the span exactly once via the tracing callback.
     */
    @Test
    @DisplayName("Async: span errors and ends exactly once when the stream write fails")
    void asyncSendFailureEndsSpanExactlyOnceWithError() {
        RuntimeException writeFailure = new RuntimeException("simulated async write failure");
        CapturingTestStream stream = new CapturingTestStream() {
            @Override
            public void writeAsync(final List<ByteBuf> buffers, final OperationContext operationContext,
                    final AsyncCompletionHandler<Void> handler) {
                handler.failed(writeFailure);
            }
        };
        InternalStreamConnection connection = createOpenConnection(stream);
        CommandMessage commandMessage = createPingCommand();
        RecordingSpan span = new RecordingSpan();

        FutureResultCallback<BsonDocument> callback = new FutureResultCallback<>();
        connection.sendAndReceiveAsync(commandMessage, new BsonDocumentCodec(), createTracedOperationContext(span), callback);
        assertThrows(RuntimeException.class, () -> callback.get(5, TimeUnit.SECONDS));

        assertEquals(1, span.errorCount.get(), "span must record the failure exactly once");
        assertEquals(1, span.endCount.get(), "span must end exactly once");
        connection.close();
    }

    /**
     * Unacknowledged writes ({@code w:0}) are one-way: the message is sent with the {@code moreToCome} flag and no
     * reply is read. They still go through the traced send path, so the sent message must carry the telemetry
     * section, and the one-way branch (rather than the receive path) must end the span exactly once.
     */
    @Test
    @DisplayName("Sync: unacknowledged (w:0) message carries telemetry section; span ends exactly once")
    void syncUnacknowledgedSendAttachesTelemetrySectionAndEndsSpanOnce() {
        CapturingTestStream stream = new CapturingTestStream() {
        };
        InternalStreamConnection connection = createOpenConnection(stream);
        CommandMessage commandMessage = createUnacknowledgedInsertCommand();
        RecordingSpan span = new RecordingSpan();

        connection.sendAndReceive(commandMessage, new BsonDocumentCodec(), createTracedOperationContext(span));

        assertEquals(EXPECTED_TELEMETRY_DOCUMENT, findTelemetrySectionDocument(stream.getWrittenBytes()),
                "the sent message must carry the telemetry section with the span's traceparent");
        assertEquals(1, span.endCount.get(), "span must end exactly once");
        assertEquals(0, span.errorCount.get());
        assertEquals(span.openScopeCount.get(), span.closeScopeCount.get(), "scopes must be balanced");
        assertFalse(stream.hadUnexpectedCall(), "no reply must be read for an unacknowledged write");
        connection.close();
    }

    /**
     * Async counterpart of {@link #syncUnacknowledgedSendAttachesTelemetrySectionAndEndsSpanOnce()}.
     */
    @Test
    @DisplayName("Async: unacknowledged (w:0) message carries telemetry section; span ends exactly once")
    void asyncUnacknowledgedSendAttachesTelemetrySectionAndEndsSpanOnce() {
        CapturingTestStream stream = new CapturingTestStream() {
        };
        InternalStreamConnection connection = createOpenConnection(stream);
        CommandMessage commandMessage = createUnacknowledgedInsertCommand();
        RecordingSpan span = new RecordingSpan();

        FutureResultCallback<BsonDocument> callback = new FutureResultCallback<>();
        connection.sendAndReceiveAsync(commandMessage, new BsonDocumentCodec(), createTracedOperationContext(span), callback);
        assertNull(callback.get(5, TimeUnit.SECONDS));

        assertEquals(EXPECTED_TELEMETRY_DOCUMENT, findTelemetrySectionDocument(stream.getWrittenBytes()),
                "the sent message must carry the telemetry section with the span's traceparent");
        assertEquals(1, span.endCount.get(), "span must end exactly once");
        assertEquals(0, span.errorCount.get());
        assertFalse(stream.hadUnexpectedCall(), "no reply must be read for an unacknowledged write");
        connection.close();
    }

    private static CommandMessage createUnacknowledgedInsertCommand() {
        BsonDocument insert = new BsonDocument("insert", new BsonString("coll"))
                .append("documents", new BsonArray(singletonList(new BsonDocument("_id", new BsonInt32(1)))))
                .append("writeConcern", new BsonDocument("w", new BsonInt32(0)));
        return new CommandMessage("db", insert, NoOpFieldNameValidator.INSTANCE, primary(),
                MessageSettings.builder().maxWireVersion(LATEST_WIRE_VERSION).build(),
                false, EmptyMessageSequences.INSTANCE, SINGLE, null);
    }

    private OperationContext createTracedOperationContext(final Span span) {
        TracingManager tracingManager = mock(TracingManager.class, mocked -> {
            when(mocked.createTracingSpan(any(), any(), any(), any(), any())).thenReturn(span);
            when(mocked.isCommandPayloadEnabled()).thenReturn(false);
        });
        return new OperationContext(IgnorableRequestContext.INSTANCE, NoOpSessionContext.INSTANCE,
                new TimeoutContext(TimeoutSettings.DEFAULT), AsyncClientExecutor.NO_OP, tracingManager, null, null);
    }

    /** A {@link TestStream} that records every written buffer, for assertions on the raw wire bytes. */
    private abstract static class CapturingTestStream extends TestStream {
        private final ByteArrayOutputStream writtenBytes = new ByteArrayOutputStream();

        @Override
        public void write(final List<ByteBuf> buffers, final OperationContext operationContext) {
            for (ByteBuf buffer : buffers) {
                ByteBuf duplicate = buffer.duplicate();
                byte[] bytes = new byte[duplicate.remaining()];
                duplicate.get(bytes);
                writtenBytes.write(bytes, 0, bytes.length);
            }
        }

        @Override
        public void writeAsync(final List<ByteBuf> buffers, final OperationContext operationContext,
                final AsyncCompletionHandler<Void> handler) {
            write(buffers, operationContext);
            handler.completed(null);
        }

        byte[] getWrittenBytes() {
            return writtenBytes.toByteArray();
        }
    }

    /** A {@link Span} that counts lifecycle calls and exposes a fixed traceparent. */
    private static final class RecordingSpan implements Span {
        private final AtomicInteger openScopeCount = new AtomicInteger();
        private final AtomicInteger closeScopeCount = new AtomicInteger();
        private final AtomicInteger errorCount = new AtomicInteger();
        private final AtomicInteger endCount = new AtomicInteger();

        @Override
        public void openScope() {
            openScopeCount.incrementAndGet();
        }

        @Override
        public void closeScope() {
            closeScopeCount.incrementAndGet();
        }

        @Override
        public void setQueryText(final BsonDocument commandDocument) {
        }

        @Override
        public void event(final String event) {
        }

        @Override
        public void error(final Throwable throwable) {
            errorCount.incrementAndGet();
        }

        @Override
        public void end() {
            endCount.incrementAndGet();
        }

        @Override
        public TraceContext context() {
            return () -> TRACEPARENT;
        }

        @Override
        @Nullable
        public MongodbObservationContext getMongodbObservationContext() {
            return null;
        }

        @Override
        @Nullable
        public MongoNamespace getNamespace() {
            return null;
        }
    }
}
