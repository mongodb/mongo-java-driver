/*
 * Copyright 2013-2016 MongoDB, Inc.
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

package com.mongodb.connection;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoSocketClosedException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.MongoSocketWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.CommandListener;
import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
import org.bson.ByteBuf;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;
import org.bson.codecs.RawBsonDocumentCodec;
import org.bson.io.ByteBufferBsonInput;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ByteBufBsonDocument.createOne;
import static com.mongodb.connection.ProtocolHelper.getCommandFailureException;
import static com.mongodb.connection.ProtocolHelper.isCommandOk;
import static com.mongodb.connection.ProtocolHelper.sendCommandFailedEvent;
import static com.mongodb.connection.ProtocolHelper.sendCommandStartedEvent;
import static com.mongodb.connection.ProtocolHelper.sendCommandSucceededEvent;
import static com.mongodb.connection.ReplyHeader.REPLY_HEADER_LENGTH;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static java.lang.String.format;
import static java.util.Arrays.asList;

@NotThreadSafe
class InternalStreamConnection implements InternalConnection {

    private static final Set<String> SECURITY_SENSITIVE_COMMANDS = new HashSet<String>(asList(
            "authenticate",
            "saslStart",
            "saslContinue",
            "getnonce",
            "createUser",
            "updateUser",
            "copydbgetnonce",
            "copydbsaslstart",
            "copydb"));

    private static final Logger LOGGER = Loggers.getLogger("connection");

    private final ServerId serverId;
    private final StreamFactory streamFactory;
    private final InternalConnectionInitializer connectionInitializer;

    private volatile ConnectionDescription description;
    private volatile Stream stream;

    private final AtomicBoolean isClosed = new AtomicBoolean();
    private final AtomicBoolean opened = new AtomicBoolean();

    private final CommandListener commandListener;

    InternalStreamConnection(final ServerId serverId, final StreamFactory streamFactory,
                             final CommandListener commandListener, final InternalConnectionInitializer connectionInitializer) {
        this.serverId = notNull("serverId", serverId);
        this.streamFactory = notNull("streamFactory", streamFactory);
        this.commandListener = commandListener;
        this.connectionInitializer = notNull("connectionInitializer", connectionInitializer);
        description = new ConnectionDescription(serverId);
    }

    @Override
    public ConnectionDescription getDescription() {
        return description;
    }

    @Override
    public void open() {
        isTrue("Open already called", stream == null);
        stream = streamFactory.create(serverId.getAddress());
        try {
            stream.open();
            description = connectionInitializer.initialize(this);
            opened.set(true);
            LOGGER.info(format("Opened connection [%s] to %s", getId(), serverId.getAddress()));
        } catch (Throwable t) {
            close();
            if (t instanceof MongoException) {
                throw (MongoException) t;
            } else {
                throw new MongoException(t.toString(), t);
            }
        }
    }

    @Override
    public void openAsync(final SingleResultCallback<Void> callback) {
        isTrue("Open already called", stream == null, callback);
        try {
            stream = streamFactory.create(serverId.getAddress());
        } catch (Throwable t) {
            callback.onResult(null, t);
            return;
        }
        stream.openAsync(new AsyncCompletionHandler<Void>() {
            @Override
            public void completed(final Void aVoid) {
                connectionInitializer.initializeAsync(InternalStreamConnection.this, new SingleResultCallback<ConnectionDescription>() {
                    @Override
                    public void onResult(final ConnectionDescription result, final Throwable t) {
                        if (t != null) {
                            close();
                            callback.onResult(null, t);
                        } else {
                            description = result;
                            opened.set(true);
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info(format("Opened connection [%s] to %s", getId(), serverId.getAddress()));
                            }
                            callback.onResult(null, null);
                        }
                    }
                });
            }

            @Override
            public void failed(final Throwable t) {
                callback.onResult(null, t);
            }
        });
    }

    @Override
    public void close() {
        // All but the first call is a no-op
        if (!isClosed.getAndSet(true)) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(String.format("Closing connection %s", getId()));
            }
            if (stream != null) {
                stream.close();
            }
        }
    }

    @Override
    public boolean opened() {
        return opened.get();
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    @Override
    public ResponseBuffers sendAndReceive(final CommandMessage message) {
        long startTimeNanos = System.nanoTime();

        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(this);
        String commandName = null;
        try {
            try {
                message.encode(bsonOutput);
                commandName = sendStartedEvent(bsonOutput, message);
                sendMessage(bsonOutput.getByteBuffers(), message.getId());
            } finally {
                bsonOutput.close();
            }

            ResponseBuffers responseBuffers = receiveMessage(message.getId());
            boolean commandOk = isCommandOk(new BsonBinaryReader(new ByteBufferBsonInput(responseBuffers.getBodyByteBuffer())));
            responseBuffers.reset();
            if (!commandOk) {
                throw getCommandFailureException(getResponseDocument(responseBuffers, message, new BsonDocumentCodec()),
                        description.getServerAddress());
            }

            sendSucceededEvent(startTimeNanos, message, commandName, getResponseDocument(responseBuffers, message,
                    new RawBsonDocumentCodec()));

            return responseBuffers;
        } catch (RuntimeException e) {
            sendFailedEvent(startTimeNanos, message, commandName, e);
            throw e;
        }
    }

    @Override
    public void sendAndReceiveAsync(final CommandMessage message, final SingleResultCallback<ResponseBuffers> callback) {
        final long startTimeNanos = System.nanoTime();

        final ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(this);
        message.encode(bsonOutput);
        final String commandName = sendStartedEvent(bsonOutput, message);
        sendMessageAsync(bsonOutput.getByteBuffers(), message.getId(), new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                bsonOutput.close();
                if (t != null) {
                    sendFailedEvent(startTimeNanos, message, commandName, t);
                    callback.onResult(null, t);
                } else {
                    receiveMessageAsync(message.getId(), new SingleResultCallback<ResponseBuffers>() {
                        @Override
                        public void onResult(final ResponseBuffers responseBuffers, final Throwable throwableFromCallback) {
                            try {
                                if (throwableFromCallback != null) {
                                    throw throwableFromCallback;
                                } else {
                                    boolean commandOk =
                                            isCommandOk(new BsonBinaryReader(new ByteBufferBsonInput(responseBuffers.getBodyByteBuffer())));
                                    responseBuffers.reset();
                                    if (!commandOk) {
                                        throw getCommandFailureException(getResponseDocument(responseBuffers, message,
                                                new BsonDocumentCodec()), description.getServerAddress());
                                    }
                                    sendSucceededEvent(startTimeNanos, message, commandName,
                                            getResponseDocument(responseBuffers, message, new RawBsonDocumentCodec()));
                                    callback.onResult(responseBuffers, null);
                                }
                            } catch (Throwable t) {
                                if (responseBuffers != null) {
                                    responseBuffers.close();
                                }
                                sendFailedEvent(startTimeNanos, message, commandName, t);
                                callback.onResult(null, t);
                            }
                        }
                    });
                }
            }
        });
    }

    @Override
    public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
        notNull("stream is open", stream);

        if (isClosed()) {
            throw new MongoSocketClosedException("Cannot write to a closed stream", getServerAddress());
        }

        try {
            stream.write(byteBuffers);
        } catch (Exception e) {
            close();
            throw translateWriteException(e);
        }
    }

    @Override
    public ResponseBuffers receiveMessage(final int responseTo) {
        notNull("stream is open", stream);
        if (isClosed()) {
            throw new MongoSocketClosedException("Cannot read from a closed stream", getServerAddress());
        }

        try {
            return receiveResponseBuffers();
        } catch (Throwable t) {
            close();
            throw translateReadException(t);
        }
    }

    @Override
    public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
        notNull("stream is open", stream, callback);

        if (isClosed()) {
            callback.onResult(null, new MongoSocketClosedException("Can not read from a closed socket", getServerAddress()));
            return;
        }

        writeAsync(byteBuffers, errorHandlingCallback(callback, LOGGER));
    }

    private void writeAsync(final List<ByteBuf> byteBuffers, final SingleResultCallback<Void> callback) {
        stream.writeAsync(byteBuffers, new AsyncCompletionHandler<Void>() {
            @Override
            public void completed(final Void v) {
                callback.onResult(null, null);
            }

            @Override
            public void failed(final Throwable t) {
                callback.onResult(null, translateWriteException(t));
                close();
            }
        });
    }

    @Override
    public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
        isTrue("stream is open", stream != null, callback);

        if (isClosed()) {
            callback.onResult(null, new MongoSocketClosedException("Can not read from a closed socket", getServerAddress()));
            return;
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("Start receiving response on %s", getId()));
        }
        receiveResponseAsync(callback);
    }

    private void receiveResponseAsync(final SingleResultCallback<ResponseBuffers> callback) {
        readAsync(REPLY_HEADER_LENGTH, errorHandlingCallback(new ResponseHeaderCallback(callback), LOGGER));
    }

    private void readAsync(final int numBytes, final SingleResultCallback<ByteBuf> callback) {
        if (isClosed()) {
            callback.onResult(null, new MongoSocketClosedException("Cannot read from a closed stream", getServerAddress()));
            return;
        }

        try {
            stream.readAsync(numBytes, new AsyncCompletionHandler<ByteBuf>() {
                @Override
                public void completed(final ByteBuf buffer) {
                    callback.onResult(buffer, null);
                }

                @Override
                public void failed(final Throwable t) {
                    close();
                    callback.onResult(null, translateReadException(t));
                }
            });
        } catch (Exception e) {
            callback.onResult(null, translateReadException(e));
        }
    }

    private ConnectionId getId() {
        return description.getConnectionId();
    }

    private ServerAddress getServerAddress() {
        return description.getServerAddress();
    }

    private MongoException translateWriteException(final Throwable e) {
        if (e instanceof MongoException) {
            return (MongoException) e;
        } else if (e instanceof IOException) {
            return new MongoSocketWriteException("Exception sending message", getServerAddress(), e);
        } else if (e instanceof InterruptedException) {
            return new MongoInternalException("Thread interrupted exception", e);
        } else {
            return new MongoInternalException("Unexpected exception", e);
        }
    }

    private MongoException translateReadException(final Throwable e) {
        if (e instanceof MongoException) {
            return (MongoException) e;
        } else if (e instanceof SocketTimeoutException) {
            return new MongoSocketReadTimeoutException("Timeout while receiving message", getServerAddress(), e);
        } else if (e instanceof InterruptedIOException) {
            return new MongoInterruptedException("Interrupted while receiving message", (InterruptedIOException) e);
        } else if (e instanceof ClosedByInterruptException) {
            return new MongoInterruptedException("Interrupted while receiving message", (ClosedByInterruptException) e);
        } else if (e instanceof IOException) {
            return new MongoSocketReadException("Exception receiving message", getServerAddress(), e);
        } else if (e instanceof RuntimeException) {
            return new MongoInternalException("Unexpected runtime exception", e);
        } else if (e instanceof InterruptedException) {
            return new MongoInternalException("Interrupted exception", e);
        } else {
            return new MongoInternalException("Unexpected exception", e);
        }
    }

    private ResponseBuffers receiveResponseBuffers() throws IOException {
        ByteBuf headerByteBuffer = stream.read(REPLY_HEADER_LENGTH);
        ReplyHeader replyHeader;
        ByteBufferBsonInput headerInputBuffer = new ByteBufferBsonInput(headerByteBuffer);
        try {
            replyHeader = new ReplyHeader(headerInputBuffer, description.getMaxMessageSize());
        } finally {
            headerInputBuffer.close();
        }

        ByteBuf bodyByteBuffer = null;

        if (replyHeader.getNumberReturned() > 0) {
            bodyByteBuffer = stream.read(replyHeader.getMessageLength() - REPLY_HEADER_LENGTH);
        }
        return new ResponseBuffers(replyHeader, bodyByteBuffer);
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        notNull("open", stream);
        return stream.getBuffer(size);
    }

    private class ResponseHeaderCallback implements SingleResultCallback<ByteBuf> {
        private final SingleResultCallback<ResponseBuffers> callback;

        ResponseHeaderCallback(final SingleResultCallback<ResponseBuffers> callback) {
            this.callback = callback;
        }

        @Override
        public void onResult(final ByteBuf result, final Throwable throwableFromCallback) {
            if (throwableFromCallback != null) {
                callback.onResult(null, throwableFromCallback);
            } else {
                try {
                    ReplyHeader replyHeader;
                    ByteBufferBsonInput headerInputBuffer = new ByteBufferBsonInput(result);
                    try {
                        replyHeader = new ReplyHeader(headerInputBuffer, description.getMaxMessageSize());
                    } finally {
                        headerInputBuffer.close();
                    }

                    if (replyHeader.getMessageLength() == REPLY_HEADER_LENGTH) {
                        onSuccess(new ResponseBuffers(replyHeader, null));
                    } else {
                        readAsync(replyHeader.getMessageLength() - REPLY_HEADER_LENGTH,
                                  new ResponseBodyCallback(replyHeader));
                    }
                } catch (Throwable t) {
                    close();
                    callback.onResult(null, t);
                }
            }
        }

        private void onSuccess(final ResponseBuffers responseBuffers) {

            if (responseBuffers == null) {
                callback.onResult(null, new MongoException("Unexpected empty response buffers"));
                return;
            }

            try {
                callback.onResult(responseBuffers, null);
            } catch (Throwable t) {
                LOGGER.warn("Exception calling callback", t);
            }
        }

        private class ResponseBodyCallback implements SingleResultCallback<ByteBuf> {
            private final ReplyHeader replyHeader;

            ResponseBodyCallback(final ReplyHeader replyHeader) {
                this.replyHeader = replyHeader;
            }

            @Override
            public void onResult(final ByteBuf result, final Throwable t) {
                if (t != null) {
                    try {
                        callback.onResult(new ResponseBuffers(replyHeader, result), t);
                    } catch (Throwable tr) {
                        LOGGER.warn("Exception calling callback", tr);
                    }
                } else {
                    onSuccess(new ResponseBuffers(replyHeader, result));
                }
            }
        }
    }

    private String sendStartedEvent(final ByteBufferBsonOutput bsonOutput, final CommandMessage message) {
        if (commandListener != null) {
            String commandName;
            ByteBufBsonDocument byteBufBsonDocument = createOne(bsonOutput, message.getEncodingMetadata().getFirstDocumentPosition());
            BsonDocument commandDocument;
            if (byteBufBsonDocument.containsKey("$query")) {
                commandDocument = byteBufBsonDocument.getDocument("$query");
                commandName = commandDocument.keySet().iterator().next();
            } else {
                commandDocument = byteBufBsonDocument;
                commandName = byteBufBsonDocument.getFirstKey();
            }
            BsonDocument commandDocumentForEvent = (SECURITY_SENSITIVE_COMMANDS.contains(commandName))
                                                           ? new BsonDocument() : commandDocument;
            sendCommandStartedEvent(message, new MongoNamespace(message.getCollectionName()).getDatabaseName(), commandName,
                    commandDocumentForEvent, getDescription(), commandListener);
            return commandName;
        }
        return null;
    }

    private void sendSucceededEvent(final long startTimeNanos, final CommandMessage commandMessage, final String commandName,
                                    final BsonDocument response) {
        if (commandListener != null) {
            BsonDocument responseDocumentForEvent = (SECURITY_SENSITIVE_COMMANDS.contains(commandName)) ? new BsonDocument() : response;
            sendCommandSucceededEvent(commandMessage, commandName, responseDocumentForEvent, description, startTimeNanos,
                    commandListener);
        }
    }

    private void sendFailedEvent(final long startTimeNanos, final CommandMessage commandMessage, final String commandName,
                                 final Throwable t) {
        if (commandListener != null) {
            Throwable commandEventException = t;
            if (t instanceof MongoCommandException && (SECURITY_SENSITIVE_COMMANDS.contains(commandName))) {
                commandEventException = new MongoCommandException(new BsonDocument(), description.getServerAddress());
            }
            sendCommandFailedEvent(commandMessage, commandName, description, startTimeNanos, commandEventException, commandListener);
        }
    }

    private static <T extends BsonDocument> T getResponseDocument(final ResponseBuffers responseBuffers,
                                                                  final CommandMessage commandMessage, final Decoder<T> decoder) {
        ReplyMessage<T> replyMessage = new ReplyMessage<T>(responseBuffers, decoder, commandMessage.getId());
        responseBuffers.reset();
        return replyMessage.getDocuments().get(0);
    }
}
