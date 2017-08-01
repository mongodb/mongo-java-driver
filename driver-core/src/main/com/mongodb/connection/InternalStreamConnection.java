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

import com.mongodb.MongoClientException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCompressor;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ByteBufBsonDocument.createOne;
import static com.mongodb.connection.CompressedHeader.COMPRESSED_HEADER_LENGTH;
import static com.mongodb.connection.ProtocolHelper.getCommandFailureException;
import static com.mongodb.connection.ProtocolHelper.getMessageSettings;
import static com.mongodb.connection.ProtocolHelper.isCommandOk;
import static com.mongodb.connection.ProtocolHelper.sendCommandFailedEvent;
import static com.mongodb.connection.ProtocolHelper.sendCommandStartedEvent;
import static com.mongodb.connection.ProtocolHelper.sendCommandSucceededEvent;
import static com.mongodb.connection.ReplyHeader.REPLY_HEADER_LENGTH;
import static com.mongodb.connection.ReplyHeader.TOTAL_REPLY_HEADER_LENGTH;
import static com.mongodb.connection.RequestMessage.OpCode.OP_COMPRESSED;
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

    private final List<MongoCompressor> compressorList;
    private final CommandListener commandListener;
    private volatile Compressor sendCompressor;
    private volatile Map<Byte, Compressor> compressorMap;

    InternalStreamConnection(final ServerId serverId, final StreamFactory streamFactory,
                             final List<MongoCompressor> compressorList, final CommandListener commandListener,
                             final InternalConnectionInitializer connectionInitializer) {
        this.serverId = notNull("serverId", serverId);
        this.streamFactory = notNull("streamFactory", streamFactory);
        this.compressorList = notNull("compressorList", compressorList);
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
            initializeCompressors(description);
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
                            initializeCompressors(description);
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

    private void initializeCompressors(final ConnectionDescription description) {
        if (description.getCompressors().isEmpty()) {
            return;
        }

        compressorMap = new HashMap<Byte, Compressor>(description.getCompressors().size());

        String firstCompressorName = description.getCompressors().get(0);

        for (MongoCompressor mongoCompressor : compressorList) {
            if (!description.getCompressors().contains(mongoCompressor.getName())) {
                continue;
            }
            if (mongoCompressor.getName().equals("zlib")) {
                Compressor compressor = new ZlibCompressor(mongoCompressor);
                compressorMap.put(compressor.getId(), compressor);
                if (mongoCompressor.getName().equals(firstCompressorName)) {
                    sendCompressor = compressor;
                }
            } else {
                throw new MongoClientException("Unsupported compressor " + firstCompressorName);
            }
        }
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
        String commandName;
        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(this);
        try {
            message.encode(bsonOutput);
            commandName = sendStartedEvent(bsonOutput, message);
        } catch (RuntimeException e) {
            bsonOutput.close();
            throw e;
        }

        long startTimeNanos = System.nanoTime();

        try {
            sendCommandMessage(message, commandName, bsonOutput);
            return receiveCommandMessageResponse(message, startTimeNanos, commandName);
        } catch (RuntimeException e) {
            sendFailedEvent(startTimeNanos, message, commandName, e);
            throw e;
        }
    }

    private void sendCommandMessage(final CommandMessage message, final String commandName, final ByteBufferBsonOutput bsonOutput) {
        try {
            if (sendCompressor == null || SECURITY_SENSITIVE_COMMANDS.contains(commandName)) {
                sendMessage(bsonOutput.getByteBuffers(), message.getId());
            } else {
                CompressedMessage compressedMessage = new CompressedMessage(RequestMessage.OpCode.OP_QUERY, bsonOutput.getByteBuffers(),
                                                                                   sendCompressor, getMessageSettings(description));
                ByteBufferBsonOutput compressedBsonOutput = new ByteBufferBsonOutput(this);
                compressedMessage.encode(compressedBsonOutput);
                try {
                    sendMessage(compressedBsonOutput.getByteBuffers(), message.getId());
                } finally {
                    compressedBsonOutput.close();
                }
            }
        } finally {
            bsonOutput.close();
        }
    }

    private ResponseBuffers receiveCommandMessageResponse(final CommandMessage message, final long startTimeNanos,
                                                          final String commandName) {
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
    }

    @Override
    public void sendAndReceiveAsync(final CommandMessage message, final SingleResultCallback<ResponseBuffers> callback) {
        notNull("stream is open", stream, callback);

        if (isClosed()) {
            callback.onResult(null, new MongoSocketClosedException("Can not read from a closed socket", getServerAddress()));
            return;
        }

        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(this);
        ByteBufferBsonOutput compressedBsonOutput = new ByteBufferBsonOutput(this);

        try {
            message.encode(bsonOutput);
            String commandName = sendStartedEvent(bsonOutput, message);

            long startTimeNanos = System.nanoTime();
            if (sendCompressor == null || SECURITY_SENSITIVE_COMMANDS.contains(commandName)) {
                sendCommandMessageAsync(message, callback, bsonOutput, commandName, startTimeNanos);
            } else {
                CompressedMessage compressedMessage = new CompressedMessage(RequestMessage.OpCode.OP_QUERY, bsonOutput.getByteBuffers(),
                                                                                   sendCompressor, getMessageSettings(description));
                compressedMessage.encode(compressedBsonOutput);
                bsonOutput.close();
                sendCommandMessageAsync(message, callback, compressedBsonOutput, commandName, startTimeNanos);
            }
        } catch (RuntimeException e) {
            bsonOutput.close();
            compressedBsonOutput.close();
            callback.onResult(null, e);
        }
    }

    private void sendCommandMessageAsync(final CommandMessage message, final SingleResultCallback<ResponseBuffers> callback,
                                         final ByteBufferBsonOutput bsonOutput, final String commandName, final long startTimeNanos) {
        sendMessageAsync(bsonOutput.getByteBuffers(), message.getId(), new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                bsonOutput.close();
                if (t != null) {
                    sendFailedEvent(startTimeNanos, message, commandName, t);
                    callback.onResult(null, t);
                } else {
                    readAsync(MessageHeader.MESSAGE_HEADER_LENGTH, new SingleResultCallback<ByteBuf>() {
                        @Override
                        public void onResult(final ByteBuf result, final Throwable t) {
                            if (t != null) {
                                sendFailedEvent(startTimeNanos, message, commandName, t);
                                callback.onResult(null, t);
                                return;
                            }
                            ByteBufferBsonInput headerInputBuffer = new ByteBufferBsonInput(result);
                            final MessageHeader messageHeader;
                            try {
                                messageHeader = new MessageHeader(headerInputBuffer, description.getMaxMessageSize());
                            } finally {
                                headerInputBuffer.close();
                            }
                            if (messageHeader.getOpCode() == OP_COMPRESSED.getValue()) {
                                readAsync(CompressedHeader.COMPRESSED_HEADER_LENGTH, new SingleResultCallback<ByteBuf>() {
                                    @Override
                                    public void onResult(final ByteBuf result, final Throwable t) {
                                        if (t != null) {
                                            sendFailedEvent(startTimeNanos, message, commandName, t);
                                            callback.onResult(null, t);
                                            return;
                                        }
                                        ByteBufferBsonInput headerInputBuffer = new ByteBufferBsonInput(result);
                                        final CompressedHeader compressedHeader;
                                        try {
                                            compressedHeader = new CompressedHeader(headerInputBuffer, messageHeader);
                                        } finally {
                                            headerInputBuffer.close();
                                        }
                                        readAsync(compressedHeader.getCompressedSize(), new SingleResultCallback<ByteBuf>() {
                                            @Override
                                            public void onResult(final ByteBuf result, final Throwable t) {
                                                if (t != null) {
                                                    sendFailedEvent(startTimeNanos, message, commandName, t);
                                                    callback.onResult(null, t);
                                                    return;
                                                }
                                                Compressor compressor = getCompressor(compressedHeader);
                                                ByteBuf buffer = getBuffer(compressedHeader.getUncompressedSize());
                                                compressor.uncompress(result, buffer);

                                                buffer.flip();
                                                // Don't close the buffer, as it doesn't own the buffer
                                                ReplyHeader replyHeader = new ReplyHeader(compressedHeader,
                                                                                                 new ByteBufferBsonInput(buffer));

                                                handleReplyAsync(buffer, replyHeader);
                                            }
                                        });
                                    }
                                });
                            } else {
                                readAsync(REPLY_HEADER_LENGTH, new SingleResultCallback<ByteBuf>() {
                                    @Override
                                    public void onResult(final ByteBuf result, final Throwable t) {
                                        if (t != null) {
                                            sendFailedEvent(startTimeNanos, message, commandName, t);
                                            callback.onResult(null, t);
                                            return;
                                        }
                                        ByteBufferBsonInput input = new ByteBufferBsonInput(result);
                                        final ReplyHeader replyHeader;
                                        try {
                                            replyHeader = new ReplyHeader(input, messageHeader);
                                        } finally {
                                            input.close();
                                        }
                                        readAsync(replyHeader.getMessageLength() - TOTAL_REPLY_HEADER_LENGTH,
                                                new SingleResultCallback<ByteBuf>() {
                                                    @Override
                                                    public void onResult(final ByteBuf result, final Throwable t) {
                                                        if (t != null) {
                                                            sendFailedEvent(startTimeNanos, message, commandName, t);
                                                            callback.onResult(null, t);
                                                            return;
                                                        }
                                                        handleReplyAsync(result, replyHeader);
                                                    }
                                                });
                                    }
                                });
                            }
                        }
                    });
                }
            }

            private void handleReplyAsync(final ByteBuf buffer, final ReplyHeader replyHeader) {
                ResponseBuffers responseBuffers = new ResponseBuffers(replyHeader, buffer);
                boolean commandOk =
                        isCommandOk(new BsonBinaryReader(new ByteBufferBsonInput(responseBuffers.getBodyByteBuffer())));
                responseBuffers.reset();
                if (!commandOk) {
                    MongoException commandFailureException = getCommandFailureException(getResponseDocument(responseBuffers, message,
                            new BsonDocumentCodec()), description.getServerAddress());
                    sendFailedEvent(startTimeNanos, message, commandName, commandFailureException);
                    callback.onResult(null, commandFailureException);
                } else {
                    sendSucceededEvent(startTimeNanos, message, commandName,
                            getResponseDocument(responseBuffers, message, new RawBsonDocumentCodec()));
                    callback.onResult(responseBuffers, null);
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
        readAsync(TOTAL_REPLY_HEADER_LENGTH, errorHandlingCallback(new ResponseHeaderCallback(callback), LOGGER));
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
        ByteBuf messageHeaderBytBuf = stream.read(MessageHeader.MESSAGE_HEADER_LENGTH);
        ByteBufferBsonInput headerInputBuffer = new ByteBufferBsonInput(messageHeaderBytBuf);
        MessageHeader messageHeader;
        try {
            messageHeader = new MessageHeader(headerInputBuffer, description.getMaxMessageSize());
        } finally {
            headerInputBuffer.close();
        }

        if (messageHeader.getOpCode() == OP_COMPRESSED.getValue()) {
            ByteBuf headerByteBuffer = stream.read(COMPRESSED_HEADER_LENGTH);
            CompressedHeader compressedHeader;
            ByteBufferBsonInput compressedHeaderInputBuffer = new ByteBufferBsonInput(headerByteBuffer);
            try {
                compressedHeader = new CompressedHeader(compressedHeaderInputBuffer, messageHeader);
            } finally {
                compressedHeaderInputBuffer.close();
            }

            ByteBuf compressedByteBuffer = stream.read(compressedHeader.getCompressedSize());
            Compressor compressor = getCompressor(compressedHeader);

            ByteBuf buffer = getBuffer(compressedHeader.getUncompressedSize());
            compressor.uncompress(compressedByteBuffer, buffer);

            buffer.flip();
            // Don't close the input, as it doesn't own the buffer
            ReplyHeader replyHeader = new ReplyHeader(compressedHeader, new ByteBufferBsonInput(buffer));

            return new ResponseBuffers(replyHeader, buffer);

        } else {
            ByteBuf headerByteBuffer = stream.read(REPLY_HEADER_LENGTH);
            ReplyHeader replyHeader;
            ByteBufferBsonInput replyHeaderInputBuffer = new ByteBufferBsonInput(headerByteBuffer);
            try {
                replyHeader = new ReplyHeader(replyHeaderInputBuffer, messageHeader);
            } finally {
                replyHeaderInputBuffer.close();
            }

            ByteBuf bodyByteBuffer = null;

            if (replyHeader.getNumberReturned() > 0) {
                bodyByteBuffer = stream.read(replyHeader.getMessageLength() - TOTAL_REPLY_HEADER_LENGTH);
            }
            return new ResponseBuffers(replyHeader, bodyByteBuffer);
        }
    }

    private Compressor getCompressor(final CompressedHeader compressedHeader) {
        Compressor compressor = compressorMap.get(compressedHeader.getCompressorId());
        if (compressor == null) {
            throw new MongoClientException("Unsupported compressor with identifier " + compressedHeader.getCompressorId());
        }
        return compressor;
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
                        MessageHeader messageHeader = new MessageHeader(headerInputBuffer, description.getMaxMessageSize());
                        replyHeader = new ReplyHeader(headerInputBuffer, messageHeader);
                    } finally {
                        headerInputBuffer.close();
                    }

                    if (replyHeader.getMessageLength() == TOTAL_REPLY_HEADER_LENGTH) {
                        onSuccess(new ResponseBuffers(replyHeader, null));
                    } else {
                        readAsync(replyHeader.getMessageLength() - TOTAL_REPLY_HEADER_LENGTH,
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

    // This is a bit of a hack, but is designed to save work for the normal path where there is no command listener or send compressor
    // The command name is returned but is a bit expensive to compute, so we only want to do it if necessary and if so do it only once.
    // So this method returns the command name only if necessary, otherwise it returns null and the caller has to not care.  And the caller
    // does not care because the command name is only needed to determine if the command is security-sensitive and therefore should not
    // be compressed.
    private String sendStartedEvent(final ByteBufferBsonOutput bsonOutput, final CommandMessage message) {
        if (commandListener != null || sendCompressor != null) {
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
            if (commandListener != null) {
                BsonDocument commandDocumentForEvent = (SECURITY_SENSITIVE_COMMANDS.contains(commandName))
                                                               ? new BsonDocument() : commandDocument;
                sendCommandStartedEvent(message, new MongoNamespace(message.getCollectionName()).getDatabaseName(), commandName,
                        commandDocumentForEvent, getDescription(), commandListener);
            }
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
