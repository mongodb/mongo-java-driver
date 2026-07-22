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

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.bulk.InsertRequest;
import com.mongodb.internal.bulk.WriteRequestWithIndex;
import com.mongodb.internal.connection.MessageSequences.EmptyMessageSequences;
import com.mongodb.internal.observability.micrometer.Span;
import com.mongodb.internal.observability.micrometer.TraceContext;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

import static com.mongodb.internal.mockito.MongoMockito.mock;
import static com.mongodb.internal.operation.ServerVersionHelper.EIGHT_DOT_ZERO_WIRE_VERSION;
import static com.mongodb.internal.operation.ServerVersionHelper.NINE_DOT_ZERO_WIRE_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

class CommandMessageOtelTraceContextTest {

    private static final String TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    private static final BsonDocument EXPECTED_TELEMETRY_DOCUMENT =
            new BsonDocument("otel", new BsonDocument("traceparent", new BsonString(TRACEPARENT)));
    private static final MongoNamespace NAMESPACE = new MongoNamespace("db.test");
    private static final BsonDocument COMMAND = new BsonDocument("find", new BsonString(NAMESPACE.getCollectionName()));

    /**
     * Telemetry section is attached when supported and traced.
     */
    @Test
    void shouldWriteTelemetrySectionWhenWireVersionAtLeast29AndSpanActive() {
        CommandMessage message = buildCommandMessage(NINE_DOT_ZERO_WIRE_VERSION, EmptyMessageSequences.INSTANCE);
        Span span = spanWithTraceParent(TRACEPARENT);
        OperationContext operationContext = buildOperationContext();

        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            message.encode(output, operationContext, span);
            byte[] buffer = output.toByteArray();

            BsonDocument telemetry = readTelemetrySectionDocument(buffer);
            assertEquals(EXPECTED_TELEMETRY_DOCUMENT, telemetry);
        }
    }

    /**
     * Telemetry section is omitted for older servers
     */
    @Test
    void shouldNotWriteTelemetrySectionWhenWireVersionBelow29() {
        CommandMessage message = buildCommandMessage(EIGHT_DOT_ZERO_WIRE_VERSION, EmptyMessageSequences.INSTANCE);
        Span span = spanWithTraceParent(TRACEPARENT);
        OperationContext operationContext = buildOperationContext();

        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            message.encode(output, operationContext, span);
            byte[] buffer = output.toByteArray();

            assertNull(findTelemetrySectionDocument(buffer));
        }
    }

    /**
     * Telemetry section is omitted without an active span.
     */
    @Test
    void shouldNotWriteTelemetrySectionWhenNoSpan() {
        CommandMessage message = buildCommandMessage(NINE_DOT_ZERO_WIRE_VERSION, EmptyMessageSequences.INSTANCE);
        OperationContext operationContext = buildOperationContext();

        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            message.encode(output, operationContext, null);
            byte[] buffer = output.toByteArray();

            assertNull(findTelemetrySectionDocument(buffer));
        }
    }

    /**
     * Malformed trace context is never sent.
     */
    @Test
    void shouldNotWriteTelemetrySectionWhenTraceParentNull() {
        CommandMessage message = buildCommandMessage(NINE_DOT_ZERO_WIRE_VERSION, EmptyMessageSequences.INSTANCE);
        Span span = spanWithTraceParent(null);
        OperationContext operationContext = buildOperationContext();

        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            message.encode(output, operationContext, span);
            byte[] buffer = output.toByteArray();

            assertNull(findTelemetrySectionDocument(buffer));
        }
    }

    @Test
    void getCommandDocumentIgnoresTelemetrySection() {
        // Regression guard: InternalStreamConnection calls getCommandDocument() on every send (logging/monitoring/
        // compression). The trailing kind-3 section must not corrupt command-document reconstruction.
        CommandMessage message = buildCommandMessage(NINE_DOT_ZERO_WIRE_VERSION, EmptyMessageSequences.INSTANCE);
        Span span = spanWithTraceParent(TRACEPARENT);
        OperationContext operationContext = buildOperationContext();

        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            message.encode(output, operationContext, span);
            BsonDocument commandDocument = message.getCommandDocument(output);
            assertEquals("test", commandDocument.getString("find").getValue());
            assertEquals("db", commandDocument.getString("$db").getValue());
            // The reconstructed command is exactly the body section (find + $db); the trailing
            // kind-3 section must not leak into it.
            assertFalse(commandDocument.containsKey("otel"));
            assertEquals(2, commandDocument.size());
        }
    }

    @Test
    void shouldWriteTelemetrySectionAfterDocumentSequences() {
        SplittablePayload payload = new SplittablePayload(SplittablePayload.Type.INSERT,
                Collections.singletonList(new WriteRequestWithIndex(
                        new InsertRequest(new BsonDocument("_id", new BsonString("1"))), 0)),
                true, NoOpFieldNameValidator.INSTANCE);
        CommandMessage message = buildCommandMessage(NINE_DOT_ZERO_WIRE_VERSION, payload);
        Span span = spanWithTraceParent(TRACEPARENT);
        OperationContext operationContext = buildOperationContext();

        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            message.encode(output, operationContext, span);
            byte[] buffer = output.toByteArray();

            // Sequence section(s) precede the kind-3 telemetry section; scanning left-to-right and taking
            // the first kind-3 occurrence therefore validates ordering as well as content.
            BsonDocument telemetry = readTelemetrySectionDocument(buffer);
            assertEquals(EXPECTED_TELEMETRY_DOCUMENT, telemetry);

            BsonDocument commandDocument = message.getCommandDocument(output);
            assertEquals("test", commandDocument.getString("find").getValue());
        }
    }

    /**
     * Same guarantee as {@link #shouldWriteTelemetrySectionAfterDocumentSequences()}, but for the
     * {@link DualMessageSequences} branch (the {@code clientBulkWrite} path). It is the only branch that
     * assembles its two kind-1 sections via {@link ByteBufferBsonOutput#branch()} (out-of-order buffer
     * segments merged on close), so the trailing kind-3 section's placement is worth pinning separately.
     */
    @Test
    void shouldWriteTelemetrySectionAfterDualMessageSequences() {
        DualMessageSequences sequences = new DualMessageSequences(
                "ops", NoOpFieldNameValidator.INSTANCE, "nsInfo", NoOpFieldNameValidator.INSTANCE) {
            @Override
            public EncodeDocumentsResult encodeDocuments(final WritersProviderAndLimitsChecker writersProviderAndLimitsChecker) {
                writersProviderAndLimitsChecker.tryWrite((firstWriter, secondWriter) -> {
                    new BsonDocumentCodec().encode(firstWriter,
                            new BsonDocument("insert", new BsonInt32(0)),
                            EncoderContext.builder().build());
                    new BsonDocumentCodec().encode(secondWriter,
                            new BsonDocument("ns", new BsonString(NAMESPACE.getFullName())),
                            EncoderContext.builder().build());
                    return 1;
                });
                return new EncodeDocumentsResult(true, Collections.emptyList());
            }
        };
        CommandMessage message = buildCommandMessage(NINE_DOT_ZERO_WIRE_VERSION, sequences);
        Span span = spanWithTraceParent(TRACEPARENT);
        OperationContext operationContext = buildOperationContext();

        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            message.encode(output, operationContext, span);
            byte[] buffer = output.toByteArray();

            BsonDocument telemetry = readTelemetrySectionDocument(buffer);
            assertEquals(EXPECTED_TELEMETRY_DOCUMENT, telemetry);

            BsonDocument commandDocument = message.getCommandDocument(output);
            assertEquals("test", commandDocument.getString("find").getValue());
            assertFalse(commandDocument.containsKey("otel"));
        }
    }

    /**
     * NEW: The 2-arg {@code encode} inherited from {@code RequestMessage} never attaches the telemetry section,
     * even when the wire version supports it. This guards monitoring/compression/other callers that still use
     * the 2-arg overload.
     */
    @Test
    void shouldNotWriteTelemetrySectionViaTwoArgEncode() {
        CommandMessage message = buildCommandMessage(NINE_DOT_ZERO_WIRE_VERSION, EmptyMessageSequences.INSTANCE);
        OperationContext operationContext = buildOperationContext();

        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            message.encode(output, operationContext);
            byte[] buffer = output.toByteArray();

            assertNull(findTelemetrySectionDocument(buffer));
        }
    }

    // --- helpers ---

    private static Span spanWithTraceParent(final String traceParent) {
        TraceContext traceContext = () -> traceParent;
        return mock(Span.class, mock -> when(mock.context()).thenReturn(traceContext));
    }

    private static CommandMessage buildCommandMessage(final int maxWireVersion, final MessageSequences sequences) {
        return new CommandMessage(
                NAMESPACE.getDatabaseName(),
                COMMAND,
                NoOpFieldNameValidator.INSTANCE,
                ReadPreference.primary(),
                MessageSettings.builder()
                        .maxWireVersion(maxWireVersion)
                        .serverType(ServerType.REPLICA_SET_PRIMARY)
                        .sessionSupported(true)
                        .build(),
                true,
                sequences,
                ClusterConnectionMode.MULTIPLE,
                null);
    }

    private static OperationContext buildOperationContext() {
        SessionContext sessionContext = mock(SessionContext.class, mock -> {
            when(mock.getClusterTime()).thenReturn(null);
            when(mock.hasSession()).thenReturn(false);
            when(mock.getReadConcern()).thenReturn(ReadConcern.DEFAULT);
            when(mock.notifyMessageSent()).thenReturn(true);
            when(mock.hasActiveTransaction()).thenReturn(false);
            when(mock.isSnapshot()).thenReturn(false);
        });
        TimeoutContext timeoutContext = new TimeoutContext(TimeoutSettings.DEFAULT);
        return mock(OperationContext.class, mock -> {
            when(mock.getSessionContext()).thenReturn(sessionContext);
            when(mock.getTimeoutContext()).thenReturn(timeoutContext);
        });
    }

    /**
     * Walks the OP_MSG sections skip the type-0 body document, then for each subsequent section read the kind byte;
     * for kind 1 (document sequence) skip past the {@code int32} section size, and for kind 3 (telemetry) decode and
     * return the BSON document payload. Returns {@code null} if no kind-3 section is found.
     */
    private static BsonDocument findTelemetrySectionDocument(final byte[] buffer) {
        ByteBuf byteBuf = new ByteBufNIO(ByteBuffer.wrap(buffer));
        // MsgHeader (16 bytes) + flagBits (4 bytes) + payload type byte (1 byte) for the body section.
        int position = 16 + 4 + 1;
        byteBuf.position(position);
        int bodyLength = byteBuf.getInt(byteBuf.position());
        byteBuf.position(byteBuf.position() + bodyLength);

        while (byteBuf.hasRemaining()) {
            byte kind = byteBuf.get();
            if (kind == 1) {
                int sectionStart = byteBuf.position();
                int sectionSize = byteBuf.getInt();
                byteBuf.position(sectionStart + sectionSize);
            } else if (kind == 3) {
                BsonBinaryReader reader = new BsonBinaryReader(byteBuf.asNIO());
                try {
                    return new BsonDocumentCodec().decode(reader, DecoderContext.builder().build());
                } finally {
                    reader.close();
                }
            } else {
                throw new AssertionError("Unexpected section kind byte: " + kind);
            }
        }
        return null;
    }

    private static BsonDocument readTelemetrySectionDocument(final byte[] buffer) {
        BsonDocument telemetry = findTelemetrySectionDocument(buffer);
        if (telemetry == null) {
            throw new AssertionError("Expected a kind-3 telemetry section but none was found");
        }
        return telemetry;
    }
}
