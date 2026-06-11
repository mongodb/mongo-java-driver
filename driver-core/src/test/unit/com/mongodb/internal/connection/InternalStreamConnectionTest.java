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

import com.mongodb.MongoCommandException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.ServerAddress;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerType;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.lang.Nullable;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonSerializationException;
import org.bson.BsonString;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.connection.ClusterConnectionMode.SINGLE;
import static com.mongodb.connection.ConnectionDescription.getDefaultMaxMessageSize;
import static com.mongodb.connection.ConnectionDescription.getDefaultMaxWriteBatchSize;
import static com.mongodb.connection.ServerDescription.getDefaultMaxDocumentSize;
import static com.mongodb.internal.connection.MessageHeader.MESSAGE_HEADER_LENGTH;
import static com.mongodb.internal.operation.ServerVersionHelper.LATEST_WIRE_VERSION;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InternalStreamConnectionTest {

    private static final ServerId SERVER_ID = new ServerId(new ClusterId(), new ServerAddress());

    /**
     * Verifies that when {@code stream.readAsync()} throws an {@link OutOfMemoryError}
     * during body buffer allocation (after header read succeeds), the connection is closed.
     */
    @Test
    @DisplayName("Async: connection closed when readAsync throws Error during body read")
    void asyncClosesConnectionWhenReadAsyncThrowsError() {
        // Track whether readAsync is called for the body (second call)
        AtomicInteger readAsyncCallCount = new AtomicInteger();

        // A stream that succeeds on header read but throws OOM on body read
        TestStream stream = new TestStream() {
            @Override
            public void readAsync(final int numBytes, final OperationContext operationContext,
                    final AsyncCompletionHandler<ByteBuf> handler) {
                if (readAsyncCallCount.incrementAndGet() == 1) {
                    // First read: 16-byte header - complete successfully with a valid header
                    handler.completed(createValidResponseHeader(getCommandMessageId()));
                } else {
                    // Second read: body - throw OutOfMemoryError (simulates heap exhaustion
                    // during buffer allocation in AsynchronousChannelStream.readAsync)
                    throw new OutOfMemoryError("Java heap space");
                }
            }
        };

        InternalStreamConnection connection = createOpenConnection(stream);

        // Send a command asynchronously - the write will succeed, then readAsync for the body will throw OOM
        FutureResultCallback<BsonDocument> callback = new FutureResultCallback<>();
        CommandMessage commandMessage = createPingCommand();
        stream.setCommandMessageId(commandMessage.getId());

        connection.sendAndReceiveAsync(commandMessage, new BsonDocumentCodec(), createOperationContext(), callback);

        // The callback should receive an error
        assertThrows(MongoInternalException.class, () -> callback.get(5, TimeUnit.SECONDS));
        assertFalse(callback.wasInvokedMultipleTimes());

        assertTrue(connection.isClosed(),
                "Connection should be closed after Error in readAsync to prevent pool reuse with stale data");
        assertTrue(stream.wasClosed(),
                "Underlying stream should be closed to release socket resources");
    }

    /**
     * Verifies that when {@code stream.readAsync()} throws a {@link RuntimeException},
     * the connection is closed, just as for an {@link Error}.
     */
    @Test
    @DisplayName("Async: connection closed when readAsync throws Exception during body read")
    void asyncClosesConnectionWhenReadAsyncThrowsException() {
        AtomicInteger readAsyncCallCount = new AtomicInteger();

        TestStream stream = new TestStream() {
            @Override
            public void readAsync(final int numBytes, final OperationContext operationContext,
                    final AsyncCompletionHandler<ByteBuf> handler) {
                if (readAsyncCallCount.incrementAndGet() == 1) {
                    handler.completed(createValidResponseHeader(getCommandMessageId()));
                } else {
                    // a RuntimeException must close the connection, just like an Error
                    throw new RuntimeException("Simulated allocation failure");
                }
            }
        };

        InternalStreamConnection connection = createOpenConnection(stream);

        FutureResultCallback<BsonDocument> callback = new FutureResultCallback<>();
        CommandMessage commandMessage = createPingCommand();
        stream.setCommandMessageId(commandMessage.getId());

        connection.sendAndReceiveAsync(commandMessage, new BsonDocumentCodec(), createOperationContext(), callback);

        assertThrows(MongoInternalException.class, () -> callback.get(5, TimeUnit.SECONDS));
        assertFalse(callback.wasInvokedMultipleTimes());

        assertTrue(connection.isClosed(),
                "Connection should be closed when readAsync throws a regular Exception");
    }

    /**
     * Verifies that when header parsing throws (e.g., invalid message size), the connection
     * is closed. The header bytes have already been consumed, so the body remains unread.
     */
    @Test
    @DisplayName("Async: connection closed when header parsing throws in MessageHeaderCallback")
    void asyncClosesConnectionWhenHeaderParsingThrows() {
        AtomicInteger readAsyncCallCount = new AtomicInteger();
        List<String> order = new ArrayList<>();

        TestStream stream = new TestStream() {
            @Override
            public void readAsync(final int numBytes, final OperationContext operationContext,
                    final AsyncCompletionHandler<ByteBuf> handler) {
                if (readAsyncCallCount.incrementAndGet() == 1) {
                    // Return a header whose messageLength exceeds the maximum message size to
                    // trigger a MongoInternalException in the MessageHeader constructor
                    ByteBuffer buffer = ByteBuffer.allocate(MESSAGE_HEADER_LENGTH);
                    buffer.order(ByteOrder.LITTLE_ENDIAN);
                    buffer.putInt(getDefaultMaxMessageSize() + 1); // invalid messageLength
                    buffer.putInt(1);   // requestId
                    buffer.putInt(getCommandMessageId()); // responseTo
                    buffer.putInt(1);   // opCode = OP_REPLY
                    ((Buffer) buffer).flip();
                    handler.completed(recordingRelease(buffer, order, "headerRelease"));
                } else {
                    recordUnexpectedCall();
                    throw new UnsupportedOperationException("no second read expected after header parsing failure");
                }
            }

            @Override
            public void close() {
                order.add("close");
                super.close();
            }
        };

        InternalStreamConnection connection = createOpenConnection(stream);

        FutureResultCallback<BsonDocument> callback = new FutureResultCallback<>();
        CommandMessage commandMessage = createPingCommand();
        stream.setCommandMessageId(commandMessage.getId());

        connection.sendAndReceiveAsync(commandMessage, new BsonDocumentCodec(), createOperationContext(), callback);

        assertThrows(MongoInternalException.class, () -> callback.get(5, TimeUnit.SECONDS));
        assertFalse(callback.wasInvokedMultipleTimes());

        assertTrue(connection.isClosed(),
                "Connection should be closed when header parsing fails to prevent reuse with unread body data");
        assertEquals(asList("headerRelease", "close"), order,
                "Header buffer must be released before the stream is closed");
        assertFalse(stream.hadUnexpectedCall(),
                "No second read should be attempted after header parsing failure");
    }

    /**
     * Verifies that when {@code stream.writeAsync()} throws an {@link Error},
     * the connection is closed.
     */
    @Test
    @DisplayName("Async: connection closed when writeAsync throws Error")
    void asyncClosesConnectionWhenWriteAsyncThrowsError() {
        TestStream stream = new TestStream() {
            @Override
            public void readAsync(final int numBytes, final OperationContext operationContext,
                    final AsyncCompletionHandler<ByteBuf> handler) {
                recordUnexpectedCall();
                throw new UnsupportedOperationException("no read expected after write failure");
            }

            @Override
            public void writeAsync(final List<ByteBuf> buffers, final OperationContext operationContext,
                    final AsyncCompletionHandler<Void> handler) {
                throw new OutOfMemoryError("Java heap space");
            }
        };

        InternalStreamConnection connection = createOpenConnection(stream);

        FutureResultCallback<BsonDocument> callback = new FutureResultCallback<>();
        CommandMessage commandMessage = createPingCommand();

        connection.sendAndReceiveAsync(commandMessage, new BsonDocumentCodec(), createOperationContext(), callback);

        assertThrows(MongoInternalException.class, () -> callback.get(5, TimeUnit.SECONDS));
        assertFalse(callback.wasInvokedMultipleTimes());

        assertTrue(connection.isClosed(),
                "Connection should be closed after Error in writeAsync");
        assertFalse(stream.hadUnexpectedCall(),
                "No read should be attempted after write failure");
    }

    /**
     * Verifies that when the body read fails (handler receives an error), the connection is closed.
     * This exercises the {@code t != null} path in {@code MessageCallback.onResult}.
     */
    @Test
    @DisplayName("Async: connection closed when body read fails via callback error")
    void asyncClosesConnectionWhenBodyReadFailsViaCallback() {
        AtomicInteger readAsyncCallCount = new AtomicInteger();

        TestStream stream = new TestStream() {
            @Override
            public void readAsync(final int numBytes, final OperationContext operationContext,
                    final AsyncCompletionHandler<ByteBuf> handler) {
                if (readAsyncCallCount.incrementAndGet() == 1) {
                    handler.completed(createValidResponseHeader(getCommandMessageId()));
                } else {
                    handler.failed(new IOException("Connection reset by peer"));
                }
            }
        };

        InternalStreamConnection connection = createOpenConnection(stream);

        FutureResultCallback<BsonDocument> callback = new FutureResultCallback<>();
        CommandMessage commandMessage = createPingCommand();
        stream.setCommandMessageId(commandMessage.getId());

        connection.sendAndReceiveAsync(commandMessage, new BsonDocumentCodec(), createOperationContext(), callback);

        assertThrows(MongoSocketReadException.class, () -> callback.get(5, TimeUnit.SECONDS));
        assertFalse(callback.wasInvokedMultipleTimes());

        assertTrue(connection.isClosed(),
                "Connection should be closed when body read fails via callback error");
    }

    /**
     * Verifies that when body parsing throws (e.g., corrupt body data), the connection is closed.
     * This exercises the {@code catch (Throwable)} path in {@code MessageCallback.onResult}.
     */
    @Test
    @DisplayName("Async: connection closed when body parsing throws in MessageCallback")
    void asyncClosesConnectionWhenBodyParsingThrows() {
        AtomicInteger readAsyncCallCount = new AtomicInteger();
        List<String> order = new ArrayList<>();

        TestStream stream = new TestStream() {
            @Override
            public void readAsync(final int numBytes, final OperationContext operationContext,
                    final AsyncCompletionHandler<ByteBuf> handler) {
                if (readAsyncCallCount.incrementAndGet() == 1) {
                    handler.completed(createValidResponseHeader(getCommandMessageId()));
                } else {
                    // Return a reply header with an invalid numberReturned (2) to trigger
                    // a MongoInternalException during ReplyHeader construction
                    ByteBuffer buffer = ByteBuffer.allocate(20);
                    buffer.order(ByteOrder.LITTLE_ENDIAN);
                    buffer.putInt(0);     // responseFlags
                    buffer.putLong(0);    // cursorId
                    buffer.putInt(0);     // startingFrom
                    buffer.putInt(2);     // numberReturned (invalid: must be 1)
                    ((Buffer) buffer).flip();
                    handler.completed(recordingRelease(buffer, order, "bodyRelease"));
                }
            }

            @Override
            public void close() {
                order.add("close");
                super.close();
            }
        };

        InternalStreamConnection connection = createOpenConnection(stream);

        FutureResultCallback<BsonDocument> callback = new FutureResultCallback<>();
        CommandMessage commandMessage = createPingCommand();
        stream.setCommandMessageId(commandMessage.getId());

        connection.sendAndReceiveAsync(commandMessage, new BsonDocumentCodec(), createOperationContext(), callback);

        assertThrows(MongoInternalException.class, () -> callback.get(5, TimeUnit.SECONDS));
        assertFalse(callback.wasInvokedMultipleTimes());

        assertTrue(connection.isClosed(),
                "Connection should be closed when body parsing fails in MessageCallback");
        assertEquals(asList("bodyRelease", "close"), order,
                "Body buffer must be released before the stream is closed");
    }

    /**
     * Verifies that when {@code stream.write()} throws an {@link Error} in the synchronous path,
     * the connection is closed (mirrors the asynchronous writeAsync behavior).
     */
    @Test
    @DisplayName("Sync: connection closed when write throws Error")
    void syncClosesConnectionWhenWriteThrowsError() {
        TestStream stream = new TestStream() {
            @Override
            public void write(final List<ByteBuf> buffers, final OperationContext operationContext) {
                throw new OutOfMemoryError("Java heap space");
            }

            @Override
            public ByteBuf read(final int numBytes, final OperationContext operationContext) {
                recordUnexpectedCall();
                throw new UnsupportedOperationException("no read expected after write failure");
            }

            @Override
            public void readAsync(final int numBytes, final OperationContext operationContext,
                    final AsyncCompletionHandler<ByteBuf> handler) {
                recordUnexpectedCall();
                throw new UnsupportedOperationException("readAsync should not be called in the sync path");
            }
        };

        InternalStreamConnection connection = createOpenConnection(stream);
        CommandMessage commandMessage = createPingCommand();

        MongoInternalException thrown = assertThrows(MongoInternalException.class,
                () -> connection.sendAndReceive(commandMessage, new BsonDocumentCodec(), createOperationContext()));
        assertInstanceOf(OutOfMemoryError.class, thrown.getCause());
        assertTrue(connection.isClosed(),
                "Connection should be closed after Error in write to prevent pool reuse with a half-written message");
        assertTrue(stream.wasClosed(),
                "Underlying stream should be closed to release socket resources");
        assertFalse(stream.hadUnexpectedCall(),
                "No read should be attempted after write failure");
    }

    /**
     * Verifies that a responseTo mismatch in the synchronous path closes the connection.
     */
    @Test
    @DisplayName("Sync: connection closed on responseTo mismatch")
    void syncClosesConnectionOnResponseToMismatch() {
        AtomicInteger readCallCount = new AtomicInteger();

        TestStream stream = new TestStream() {
            @Override
            public ByteBuf read(final int numBytes, final OperationContext operationContext) {
                if (readCallCount.incrementAndGet() == 1) {
                    // Header: responseTo deliberately mismatches (commandId + 1)
                    return createValidResponseHeader(getCommandMessageId() + 1);
                } else {
                    return createResponseBody();
                }
            }

            @Override
            public void readAsync(final int numBytes, final OperationContext operationContext,
                    final AsyncCompletionHandler<ByteBuf> handler) {
                recordUnexpectedCall();
                throw new UnsupportedOperationException("readAsync should not be called in the sync path");
            }
        };

        InternalStreamConnection connection = createOpenConnection(stream);
        CommandMessage commandMessage = createPingCommand();
        stream.setCommandMessageId(commandMessage.getId());

        MongoInternalException thrown = assertThrows(MongoInternalException.class,
                () -> connection.sendAndReceive(commandMessage, new BsonDocumentCodec(), createOperationContext()));
        assertTrue(thrown.getMessage().contains("does not match the requestId"));
        assertTrue(connection.isClosed(),
                "Connection should be closed after responseTo mismatch");
        assertFalse(stream.hadUnexpectedCall());
    }

    /**
     * Verifies that a responseTo mismatch in the asynchronous path closes the connection.
     */
    @Test
    @DisplayName("Async: connection closed on responseTo mismatch")
    void asyncClosesConnectionOnResponseToMismatch() {
        AtomicInteger readAsyncCallCount = new AtomicInteger();

        TestStream stream = new TestStream() {
            @Override
            public void readAsync(final int numBytes, final OperationContext operationContext,
                    final AsyncCompletionHandler<ByteBuf> handler) {
                if (readAsyncCallCount.incrementAndGet() == 1) {
                    // Header: responseTo deliberately mismatches (commandId + 1)
                    handler.completed(createValidResponseHeader(getCommandMessageId() + 1));
                } else {
                    handler.completed(createResponseBody());
                }
            }
        };

        InternalStreamConnection connection = createOpenConnection(stream);
        CommandMessage commandMessage = createPingCommand();
        stream.setCommandMessageId(commandMessage.getId());

        FutureResultCallback<BsonDocument> callback = new FutureResultCallback<>();
        connection.sendAndReceiveAsync(commandMessage, new BsonDocumentCodec(), createOperationContext(), callback);

        MongoInternalException thrown = assertThrows(MongoInternalException.class, () -> callback.get(5, TimeUnit.SECONDS));
        assertTrue(thrown.getMessage().contains("does not match the requestId"));
        assertFalse(callback.wasInvokedMultipleTimes());

        assertTrue(connection.isClosed(),
                "Connection should be closed after responseTo mismatch");
    }

    /**
     * Verifies that a command error response (ok: 0) in the synchronous path does NOT close
     * the connection: the response was fully read, so the stream remains synchronized and reusable.
     */
    @Test
    @DisplayName("Sync: connection NOT closed on command error response")
    void syncDoesNotCloseConnectionOnCommandError() {
        AtomicInteger readCallCount = new AtomicInteger();
        BsonDocument errorResponse = createCommandErrorDocument();

        TestStream stream = new TestStream() {
            @Override
            public ByteBuf read(final int numBytes, final OperationContext operationContext) {
                if (readCallCount.incrementAndGet() == 1) {
                    return createValidResponseHeader(getCommandMessageId(), errorResponse);
                } else {
                    return createResponseBody(errorResponse);
                }
            }

            @Override
            public void readAsync(final int numBytes, final OperationContext operationContext,
                    final AsyncCompletionHandler<ByteBuf> handler) {
                recordUnexpectedCall();
                throw new UnsupportedOperationException("readAsync should not be called in the sync path");
            }
        };

        InternalStreamConnection connection = createOpenConnection(stream);
        CommandMessage commandMessage = createPingCommand();
        stream.setCommandMessageId(commandMessage.getId());

        MongoCommandException thrown = assertThrows(MongoCommandException.class,
                () -> connection.sendAndReceive(commandMessage, new BsonDocumentCodec(), createOperationContext()));
        assertEquals(112, thrown.getErrorCode());
        assertFalse(connection.isClosed(),
                "Connection should NOT be closed on a command error response");
        assertFalse(stream.wasClosed(),
                "Underlying stream should NOT be closed on a command error response");
        assertFalse(stream.hadUnexpectedCall());

        connection.close();
    }

    /**
     * Verifies that a command error response (ok: 0) in the asynchronous path does NOT close
     * the connection: the response was fully read, so the stream remains synchronized and reusable.
     */
    @Test
    @DisplayName("Async: connection NOT closed on command error response")
    void asyncDoesNotCloseConnectionOnCommandError() {
        AtomicInteger readAsyncCallCount = new AtomicInteger();
        BsonDocument errorResponse = createCommandErrorDocument();

        TestStream stream = new TestStream() {
            @Override
            public void readAsync(final int numBytes, final OperationContext operationContext,
                    final AsyncCompletionHandler<ByteBuf> handler) {
                if (readAsyncCallCount.incrementAndGet() == 1) {
                    handler.completed(createValidResponseHeader(getCommandMessageId(), errorResponse));
                } else {
                    handler.completed(createResponseBody(errorResponse));
                }
            }
        };

        InternalStreamConnection connection = createOpenConnection(stream);
        CommandMessage commandMessage = createPingCommand();
        stream.setCommandMessageId(commandMessage.getId());

        FutureResultCallback<BsonDocument> callback = new FutureResultCallback<>();
        connection.sendAndReceiveAsync(commandMessage, new BsonDocumentCodec(), createOperationContext(), callback);

        MongoCommandException thrown = assertThrows(MongoCommandException.class, () -> callback.get(5, TimeUnit.SECONDS));
        assertEquals(112, thrown.getErrorCode());
        assertFalse(callback.wasInvokedMultipleTimes());

        assertFalse(connection.isClosed(),
                "Connection should NOT be closed on a command error response");
        assertFalse(stream.wasClosed(),
                "Underlying stream should NOT be closed on a command error response");

        connection.close();
    }

    /**
     * Verifies that when a command error response (ok: 0) carries a mismatched responseTo in the
     * synchronous path, command monitoring receives exactly one started and one failed event.
     */
    @Test
    @DisplayName("Sync: monitoring receives started and failed events on responseTo mismatch in command error response")
    void syncSendsFailedEventOnResponseToMismatchInCommandError() {
        AtomicInteger readCallCount = new AtomicInteger();
        BsonDocument errorResponse = createCommandErrorDocument();

        TestStream stream = new TestStream() {
            @Override
            public ByteBuf read(final int numBytes, final OperationContext operationContext) {
                if (readCallCount.incrementAndGet() == 1) {
                    // Header: responseTo deliberately mismatches AND the body is a command error
                    return createValidResponseHeader(getCommandMessageId() + 1, errorResponse);
                } else {
                    return createResponseBody(errorResponse);
                }
            }

            @Override
            public void readAsync(final int numBytes, final OperationContext operationContext,
                    final AsyncCompletionHandler<ByteBuf> handler) {
                recordUnexpectedCall();
                throw new UnsupportedOperationException("readAsync should not be called in the sync path");
            }
        };

        TestCommandListener listener = new TestCommandListener();
        InternalStreamConnection connection = createOpenConnection(stream, listener);
        CommandMessage commandMessage = createPingCommand();
        stream.setCommandMessageId(commandMessage.getId());

        assertThrows(MongoInternalException.class,
                () -> connection.sendAndReceive(commandMessage, new BsonDocumentCodec(), createOperationContext()));
        assertStartedThenFailed(listener);
        assertTrue(connection.isClosed(),
                "Connection should be closed after responseTo mismatch");
        assertFalse(stream.hadUnexpectedCall());
    }

    /**
     * Verifies that when a command error response (ok: 0) carries a mismatched responseTo in the
     * asynchronous path, command monitoring receives exactly one started and one failed event.
     */
    @Test
    @DisplayName("Async: monitoring receives started and failed events on responseTo mismatch in command error response")
    void asyncSendsFailedEventOnResponseToMismatchInCommandError() {
        AtomicInteger readAsyncCallCount = new AtomicInteger();
        BsonDocument errorResponse = createCommandErrorDocument();

        TestStream stream = new TestStream() {
            @Override
            public void readAsync(final int numBytes, final OperationContext operationContext,
                    final AsyncCompletionHandler<ByteBuf> handler) {
                if (readAsyncCallCount.incrementAndGet() == 1) {
                    // Header: responseTo deliberately mismatches AND the body is a command error
                    handler.completed(createValidResponseHeader(getCommandMessageId() + 1, errorResponse));
                } else {
                    handler.completed(createResponseBody(errorResponse));
                }
            }
        };

        TestCommandListener listener = new TestCommandListener();
        InternalStreamConnection connection = createOpenConnection(stream, listener);
        CommandMessage commandMessage = createPingCommand();
        stream.setCommandMessageId(commandMessage.getId());

        FutureResultCallback<BsonDocument> callback = new FutureResultCallback<>();
        connection.sendAndReceiveAsync(commandMessage, new BsonDocumentCodec(), createOperationContext(), callback);

        assertThrows(MongoInternalException.class, () -> callback.get(5, TimeUnit.SECONDS));
        assertFalse(callback.wasInvokedMultipleTimes());
        assertStartedThenFailed(listener);
        assertTrue(connection.isClosed(),
                "Connection should be closed after responseTo mismatch");
    }

    /**
     * Verifies that when an ok: 1 response carries a mismatched responseTo in the synchronous
     * path, command monitoring receives exactly one started and one failed event: a succeeded
     * event must not be emitted for a response that belongs to a different request.
     */
    @Test
    @DisplayName("Sync: monitoring receives started and failed events on responseTo mismatch in ok response")
    void syncSendsFailedEventOnResponseToMismatchInOkResponse() {
        AtomicInteger readCallCount = new AtomicInteger();

        TestStream stream = new TestStream() {
            @Override
            public ByteBuf read(final int numBytes, final OperationContext operationContext) {
                if (readCallCount.incrementAndGet() == 1) {
                    // Header: responseTo deliberately mismatches; the body is a success (ok: 1)
                    return createValidResponseHeader(getCommandMessageId() + 1);
                } else {
                    return createResponseBody();
                }
            }

            @Override
            public void readAsync(final int numBytes, final OperationContext operationContext,
                    final AsyncCompletionHandler<ByteBuf> handler) {
                recordUnexpectedCall();
                throw new UnsupportedOperationException("readAsync should not be called in the sync path");
            }
        };

        TestCommandListener listener = new TestCommandListener();
        InternalStreamConnection connection = createOpenConnection(stream, listener);
        CommandMessage commandMessage = createPingCommand();
        stream.setCommandMessageId(commandMessage.getId());

        assertThrows(MongoInternalException.class,
                () -> connection.sendAndReceive(commandMessage, new BsonDocumentCodec(), createOperationContext()));
        assertStartedThenFailed(listener);
        assertTrue(connection.isClosed(),
                "Connection should be closed after responseTo mismatch");
        assertFalse(stream.hadUnexpectedCall());
    }

    /**
     * Verifies that when an ok: 1 response carries a mismatched responseTo in the asynchronous
     * path, command monitoring receives exactly one started and one failed event: a succeeded
     * event must not be emitted for a response that belongs to a different request.
     */
    @Test
    @DisplayName("Async: monitoring receives started and failed events on responseTo mismatch in ok response")
    void asyncSendsFailedEventOnResponseToMismatchInOkResponse() {
        AtomicInteger readAsyncCallCount = new AtomicInteger();

        TestStream stream = new TestStream() {
            @Override
            public void readAsync(final int numBytes, final OperationContext operationContext,
                    final AsyncCompletionHandler<ByteBuf> handler) {
                if (readAsyncCallCount.incrementAndGet() == 1) {
                    // Header: responseTo deliberately mismatches; the body is a success (ok: 1)
                    handler.completed(createValidResponseHeader(getCommandMessageId() + 1));
                } else {
                    handler.completed(createResponseBody());
                }
            }
        };

        TestCommandListener listener = new TestCommandListener();
        InternalStreamConnection connection = createOpenConnection(stream, listener);
        CommandMessage commandMessage = createPingCommand();
        stream.setCommandMessageId(commandMessage.getId());

        FutureResultCallback<BsonDocument> callback = new FutureResultCallback<>();
        connection.sendAndReceiveAsync(commandMessage, new BsonDocumentCodec(), createOperationContext(), callback);

        assertThrows(MongoInternalException.class, () -> callback.get(5, TimeUnit.SECONDS));
        assertFalse(callback.wasInvokedMultipleTimes());
        assertStartedThenFailed(listener);
        assertTrue(connection.isClosed(),
                "Connection should be closed after responseTo mismatch");
    }

    /**
     * Verifies that when the response document is corrupt (but fully read) in the synchronous
     * path, command monitoring receives a failed event and the connection stays open: the wire
     * framing is intact, so the stream is not desynchronized.
     */
    @Test
    @DisplayName("Sync: monitoring receives started and failed events when response document parsing fails")
    void syncSendsFailedEventWhenResponseDocumentParsingFails() {
        AtomicInteger readCallCount = new AtomicInteger();

        TestStream stream = new TestStream() {
            @Override
            public ByteBuf read(final int numBytes, final OperationContext operationContext) {
                if (readCallCount.incrementAndGet() == 1) {
                    return createValidResponseHeaderForBodyLength(getCommandMessageId(), CORRUPT_BODY_LENGTH);
                } else {
                    return createCorruptResponseBody();
                }
            }

            @Override
            public void readAsync(final int numBytes, final OperationContext operationContext,
                    final AsyncCompletionHandler<ByteBuf> handler) {
                recordUnexpectedCall();
                throw new UnsupportedOperationException("readAsync should not be called in the sync path");
            }
        };

        TestCommandListener listener = new TestCommandListener();
        InternalStreamConnection connection = createOpenConnection(stream, listener);
        CommandMessage commandMessage = createPingCommand();
        stream.setCommandMessageId(commandMessage.getId());

        assertThrows(BsonSerializationException.class,
                () -> connection.sendAndReceive(commandMessage, new BsonDocumentCodec(), createOperationContext()));
        assertStartedThenFailed(listener);
        assertFalse(connection.isClosed(),
                "Connection should NOT be closed when a fully-read response fails to parse");
        assertFalse(stream.hadUnexpectedCall());

        connection.close();
    }

    /**
     * Verifies that when the response document is corrupt (but fully read) in the asynchronous
     * path, command monitoring receives a failed event and the connection stays open: the wire
     * framing is intact, so the stream is not desynchronized.
     */
    @Test
    @DisplayName("Async: monitoring receives started and failed events when response document parsing fails")
    void asyncSendsFailedEventWhenResponseDocumentParsingFails() {
        AtomicInteger readAsyncCallCount = new AtomicInteger();

        TestStream stream = new TestStream() {
            @Override
            public void readAsync(final int numBytes, final OperationContext operationContext,
                    final AsyncCompletionHandler<ByteBuf> handler) {
                if (readAsyncCallCount.incrementAndGet() == 1) {
                    handler.completed(createValidResponseHeaderForBodyLength(getCommandMessageId(), CORRUPT_BODY_LENGTH));
                } else {
                    handler.completed(createCorruptResponseBody());
                }
            }
        };

        TestCommandListener listener = new TestCommandListener();
        InternalStreamConnection connection = createOpenConnection(stream, listener);
        CommandMessage commandMessage = createPingCommand();
        stream.setCommandMessageId(commandMessage.getId());

        FutureResultCallback<BsonDocument> callback = new FutureResultCallback<>();
        connection.sendAndReceiveAsync(commandMessage, new BsonDocumentCodec(), createOperationContext(), callback);

        assertThrows(BsonSerializationException.class, () -> callback.get(5, TimeUnit.SECONDS));
        assertFalse(callback.wasInvokedMultipleTimes());
        assertStartedThenFailed(listener);
        assertFalse(connection.isClosed(),
                "Connection should NOT be closed when a fully-read response fails to parse");

        connection.close();
    }

    /**
     * Verifies that on a responseTo mismatch in the asynchronous path, the response body buffer
     * is released BEFORE the stream is closed. NettyStream.close() requires all buffers it
     * handed out to have been released already; releasing after close corrupts reference counts.
     */
    @Test
    @DisplayName("Async: response buffer released before connection close on responseTo mismatch")
    void asyncReleasesResponseBufferBeforeCloseOnResponseToMismatch() {
        AtomicInteger readAsyncCallCount = new AtomicInteger();
        List<String> order = new ArrayList<>();

        TestStream stream = new TestStream() {
            @Override
            public void readAsync(final int numBytes, final OperationContext operationContext,
                    final AsyncCompletionHandler<ByteBuf> handler) {
                if (readAsyncCallCount.incrementAndGet() == 1) {
                    handler.completed(createValidResponseHeader(getCommandMessageId() + 1));
                } else {
                    handler.completed(recordingRelease(createResponseBodyBuffer(), order, "bodyRelease"));
                }
            }

            @Override
            public void close() {
                order.add("close");
                super.close();
            }
        };

        InternalStreamConnection connection = createOpenConnection(stream);
        CommandMessage commandMessage = createPingCommand();
        stream.setCommandMessageId(commandMessage.getId());

        FutureResultCallback<BsonDocument> callback = new FutureResultCallback<>();
        connection.sendAndReceiveAsync(commandMessage, new BsonDocumentCodec(), createOperationContext(), callback);

        assertThrows(MongoInternalException.class, () -> callback.get(5, TimeUnit.SECONDS));
        assertTrue(connection.isClosed(),
                "Connection should be closed after responseTo mismatch");
        assertEquals(asList("bodyRelease", "close"), order,
                "Response body buffer must be released before the stream is closed");
    }

    private InternalStreamConnection createOpenConnection(final TestStream stream) {
        return createOpenConnection(stream, null);
    }

    private InternalStreamConnection createOpenConnection(final TestStream stream, @Nullable final CommandListener commandListener) {
        StreamFactory streamFactory = serverAddress -> stream;
        ConnectionDescription connectionDescription = new ConnectionDescription(
                new ConnectionId(SERVER_ID, 1, 1L), LATEST_WIRE_VERSION,
                ServerType.STANDALONE, getDefaultMaxWriteBatchSize(),
                getDefaultMaxDocumentSize(), getDefaultMaxMessageSize(), Collections.emptyList());
        ServerDescription serverDescription = ServerDescription.builder()
                .ok(true)
                .state(ServerConnectionState.CONNECTED)
                .type(ServerType.STANDALONE)
                .address(new ServerAddress())
                .build();
        InternalConnectionInitializationDescription initDesc =
                new InternalConnectionInitializationDescription(connectionDescription, serverDescription);

        InternalConnectionInitializer initializer = new InternalConnectionInitializer() {
            @Override
            public InternalConnectionInitializationDescription startHandshake(final InternalConnection connection,
                    final OperationContext operationContext) {
                return initDesc;
            }

            @Override
            public InternalConnectionInitializationDescription finishHandshake(final InternalConnection connection,
                    final InternalConnectionInitializationDescription description, final OperationContext operationContext) {
                return initDesc;
            }

            @Override
            public void startHandshakeAsync(final InternalConnection connection, final OperationContext operationContext,
                    final SingleResultCallback<InternalConnectionInitializationDescription> callback) {
                callback.onResult(initDesc, null);
            }

            @Override
            public void finishHandshakeAsync(final InternalConnection connection,
                    final InternalConnectionInitializationDescription description, final OperationContext operationContext,
                    final SingleResultCallback<InternalConnectionInitializationDescription> callback) {
                callback.onResult(initDesc, null);
            }
        };

        InternalStreamConnection connection = new InternalStreamConnection(
                SINGLE, SERVER_ID, new TestConnectionGenerationSupplier(),
                streamFactory, Collections.emptyList(), commandListener, initializer);
        connection.open(createOperationContext());
        return connection;
    }

    private CommandMessage createPingCommand() {
        return new CommandMessage(
                "admin",
                new BsonDocument("ping", new BsonInt32(1)),
                NoOpFieldNameValidator.INSTANCE,
                primary(),
                MessageSettings.builder().maxWireVersion(LATEST_WIRE_VERSION).build(),
                SINGLE,
                null);
    }

    private OperationContext createOperationContext() {
        return OperationContext.simpleOperationContext(new TimeoutContext(TimeoutSettings.DEFAULT));
    }

    /**
     * Creates a generic command error response document (ok: 0).
     */
    private static BsonDocument createCommandErrorDocument() {
        return new BsonDocument("ok", new BsonInt32(0))
                .append("errmsg", new BsonString("WriteConflict error"))
                .append("code", new BsonInt32(112))
                .append("codeName", new BsonString("WriteConflict"));
    }

    /**
     * Creates a valid OP_REPLY response body: reply header (20 bytes) + BSON {ok: 1}.
     */
    private static ByteBuf createResponseBody() {
        return createResponseBody(new BsonDocument("ok", new BsonInt32(1)));
    }

    /**
     * Creates a valid OP_REPLY response body: reply header (20 bytes) + the given BSON document.
     */
    private static ByteBuf createResponseBody(final BsonDocument document) {
        return new ByteBufNIO(createResponseBodyBuffer(document));
    }

    /**
     * Creates the raw buffer for a valid OP_REPLY response body: reply header (20 bytes) + {ok: 1}.
     */
    private static ByteBuffer createResponseBodyBuffer() {
        return createResponseBodyBuffer(new BsonDocument("ok", new BsonInt32(1)));
    }

    /**
     * Creates the raw buffer for a valid OP_REPLY response body: reply header (20 bytes) + the given BSON document.
     */
    private static ByteBuffer createResponseBodyBuffer(final BsonDocument document) {
        byte[] bson = toBson(document);
        ByteBuffer buffer = ByteBuffer.allocate(20 + bson.length);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(0);     // responseFlags
        buffer.putLong(0);    // cursorId
        buffer.putInt(0);     // startingFrom
        buffer.putInt(1);     // numberReturned
        buffer.put(bson);
        ((Buffer) buffer).flip();
        return buffer;
    }

    private static final int CORRUPT_BODY_LENGTH = 28; // reply header (20) + truncated BSON (8)

    /**
     * Creates an OP_REPLY response body with a valid reply header but a corrupt BSON document:
     * the document declares a length far larger than the bytes that follow.
     */
    private static ByteBuf createCorruptResponseBody() {
        ByteBuffer buffer = ByteBuffer.allocate(CORRUPT_BODY_LENGTH);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(0);     // responseFlags
        buffer.putLong(0);    // cursorId
        buffer.putInt(0);     // startingFrom
        buffer.putInt(1);     // numberReturned
        buffer.putInt(1000);  // BSON document length (invalid: only 4 more bytes follow)
        buffer.putInt(0);
        ((Buffer) buffer).flip();
        return new ByteBufNIO(buffer);
    }

    /**
     * Creates a valid 16-byte wire protocol header (OP_REPLY) for the given responseTo.
     */
    private static ByteBuf createValidResponseHeader(final int responseTo) {
        return createValidResponseHeader(responseTo, new BsonDocument("ok", new BsonInt32(1)));
    }

    /**
     * Creates a valid 16-byte wire protocol header (OP_REPLY) for the given responseTo,
     * sized for a body containing the given BSON document.
     */
    private static ByteBuf createValidResponseHeader(final int responseTo, final BsonDocument document) {
        return createValidResponseHeaderForBodyLength(responseTo, 20 + toBson(document).length);
    }

    /**
     * Creates a valid 16-byte wire protocol header (OP_REPLY) for the given responseTo and body length.
     */
    private static ByteBuf createValidResponseHeaderForBodyLength(final int responseTo, final int bodyLength) {
        ByteBuffer buffer = ByteBuffer.allocate(MESSAGE_HEADER_LENGTH);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(MESSAGE_HEADER_LENGTH + bodyLength); // messageLength
        buffer.putInt(1);            // requestId (server's response message ID)
        buffer.putInt(responseTo);   // responseTo (echoes client's requestId)
        buffer.putInt(1);            // opCode = OP_REPLY
        ((Buffer) buffer).flip();

        return new ByteBufNIO(buffer);
    }

    /**
     * Creates a ByteBuf over the given buffer whose release is recorded in the given order list.
     */
    private static ByteBuf recordingRelease(final ByteBuffer buffer, final List<String> order, final String label) {
        return new ByteBufNIO(buffer) {
            @Override
            public void release() {
                order.add(label);
                super.release();
            }
        };
    }

    private static byte[] toBson(final BsonDocument document) {
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        new BsonDocumentCodec().encode(
                new BsonBinaryWriter(outputBuffer), document,
                EncoderContext.builder().build());
        return outputBuffer.toByteArray();
    }

    /**
     * Asserts that command monitoring received exactly one started and one failed event.
     */
    private static void assertStartedThenFailed(final TestCommandListener listener) {
        List<CommandEvent> events = listener.getEvents();
        assertEquals(2, events.size(),
                "Monitoring must receive exactly one started and one failed event but received: " + events);
        assertInstanceOf(CommandStartedEvent.class, events.get(0));
        assertInstanceOf(CommandFailedEvent.class, events.get(1));
    }

    /**
     * A test Stream implementation that handles writes (no-op) and delegates reads to subclasses.
     * Unexpected calls must be recorded via {@link #recordUnexpectedCall()} rather than JUnit's
     * {@code fail()}: the production code under test catches {@code Throwable}, so an assertion
     * error thrown inside a stub is swallowed and translated into the very exception a test expects.
     */
    private abstract static class TestStream implements Stream {
        private int commandMessageId;
        private boolean closed;
        private final AtomicBoolean unexpectedCall = new AtomicBoolean();

        void setCommandMessageId(final int id) {
            this.commandMessageId = id;
        }

        int getCommandMessageId() {
            return commandMessageId;
        }

        boolean wasClosed() {
            return closed;
        }

        void recordUnexpectedCall() {
            unexpectedCall.set(true);
        }

        boolean hadUnexpectedCall() {
            return unexpectedCall.get();
        }

        @Override
        public void open(final OperationContext operationContext) {
        }

        @Override
        public void openAsync(final OperationContext operationContext,
                final AsyncCompletionHandler<Void> handler) {
            handler.completed(null);
        }

        @Override
        public void write(final List<ByteBuf> buffers, final OperationContext operationContext) {
        }

        @Override
        public void writeAsync(final List<ByteBuf> buffers, final OperationContext operationContext,
                final AsyncCompletionHandler<Void> handler) {
            handler.completed(null);
        }

        @Override
        public ByteBuf read(final int numBytes, final OperationContext operationContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteBuf getBuffer(final int size) {
            return new ByteBufNIO(ByteBuffer.allocate(size));
        }

        @Override
        public ServerAddress getAddress() {
            return new ServerAddress();
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public boolean isClosed() {
            return closed;
        }
    }
}
