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

import com.mongodb.LoggerSettings;
import com.mongodb.MongoClientException;
import com.mongodb.MongoCompressor;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoSocketClosedException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.MongoSocketWriteException;
import com.mongodb.RequestContext;
import com.mongodb.ServerAddress;
import com.mongodb.UnixServerAddress;
import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerType;
import com.mongodb.connection.Stream;
import com.mongodb.connection.StreamFactory;
import com.mongodb.event.CommandListener;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.internal.logging.StructuredLogger;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;
import com.mongodb.spi.dns.InetAddressResolver;
import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
import org.bson.ByteBuf;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;
import org.bson.io.ByteBufferBsonInput;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.connection.CommandHelper.HELLO;
import static com.mongodb.internal.connection.CommandHelper.LEGACY_HELLO;
import static com.mongodb.internal.connection.CommandHelper.LEGACY_HELLO_LOWER;
import static com.mongodb.internal.connection.MessageHeader.MESSAGE_HEADER_LENGTH;
import static com.mongodb.internal.connection.OpCode.OP_COMPRESSED;
import static com.mongodb.internal.connection.ProtocolHelper.createSpecialWriteConcernException;
import static com.mongodb.internal.connection.ProtocolHelper.getClusterTime;
import static com.mongodb.internal.connection.ProtocolHelper.getCommandFailureException;
import static com.mongodb.internal.connection.ProtocolHelper.getMessageSettings;
import static com.mongodb.internal.connection.ProtocolHelper.getOperationTime;
import static com.mongodb.internal.connection.ProtocolHelper.getRecoveryToken;
import static com.mongodb.internal.connection.ProtocolHelper.getSnapshotTimestamp;
import static com.mongodb.internal.connection.ProtocolHelper.isCommandOk;
import static com.mongodb.internal.logging.StructuredLogMessage.Level.DEBUG;
import static java.util.Arrays.asList;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
@NotThreadSafe
public class InternalStreamConnection implements InternalConnection {

    private static final Set<String> SECURITY_SENSITIVE_COMMANDS = new HashSet<>(asList(
            "authenticate",
            "saslStart",
            "saslContinue",
            "getnonce",
            "createUser",
            "updateUser",
            "copydbgetnonce",
            "copydbsaslstart",
            "copydb"));

    private static final Set<String> SECURITY_SENSITIVE_HELLO_COMMANDS = new HashSet<>(asList(
            HELLO,
            LEGACY_HELLO,
            LEGACY_HELLO_LOWER));

    private static final Logger LOGGER = Loggers.getLogger("connection");

    private final ClusterConnectionMode clusterConnectionMode;
    private final boolean isMonitoringConnection;
    private final ServerId serverId;
    private final ConnectionGenerationSupplier connectionGenerationSupplier;
    private final StreamFactory streamFactory;
    private final InternalConnectionInitializer connectionInitializer;
    private final InetAddressResolver inetAddressResolver;

    private volatile ConnectionDescription description;
    private volatile ServerDescription initialServerDescription;
    private volatile Stream stream;

    private final AtomicBoolean isClosed = new AtomicBoolean();
    private final AtomicBoolean opened = new AtomicBoolean();

    private final List<MongoCompressor> compressorList;
    private final LoggerSettings loggerSettings;
    private final CommandListener commandListener;
    @Nullable private volatile Compressor sendCompressor;
    private final Map<Byte, Compressor> compressorMap;
    private volatile boolean hasMoreToCome;
    private volatile int responseTo;
    private int generation = NOT_INITIALIZED_GENERATION;

    // Package-level access provided to avoid duplicating the list in test code
    static Set<String> getSecuritySensitiveCommands() {
        return Collections.unmodifiableSet(SECURITY_SENSITIVE_COMMANDS);
    }

    // Package-level access provided to avoid duplicating the list in test code
    static Set<String> getSecuritySensitiveHelloCommands() {
        return Collections.unmodifiableSet(SECURITY_SENSITIVE_HELLO_COMMANDS);
    }

    @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
    public InternalStreamConnection(final ClusterConnectionMode clusterConnectionMode, final ServerId serverId,
            final ConnectionGenerationSupplier connectionGenerationSupplier,
            final StreamFactory streamFactory, final List<MongoCompressor> compressorList,
            final CommandListener commandListener, final InternalConnectionInitializer connectionInitializer,
            @Nullable final InetAddressResolver inetAddressResolver) {
        this(clusterConnectionMode, false, serverId, connectionGenerationSupplier, streamFactory, compressorList,
                LoggerSettings.builder().build(), commandListener, connectionInitializer, inetAddressResolver);
    }

    public InternalStreamConnection(final ClusterConnectionMode clusterConnectionMode, final boolean isMonitoringConnection,
            final ServerId serverId,
            final ConnectionGenerationSupplier connectionGenerationSupplier,
            final StreamFactory streamFactory, final List<MongoCompressor> compressorList,
            final LoggerSettings loggerSettings,
            final CommandListener commandListener, final InternalConnectionInitializer connectionInitializer,
            @Nullable final InetAddressResolver inetAddressResolver) {
        this.clusterConnectionMode = clusterConnectionMode;
        this.isMonitoringConnection = isMonitoringConnection;
        this.serverId = notNull("serverId", serverId);
        this.connectionGenerationSupplier = notNull("connectionGeneration", connectionGenerationSupplier);
        this.streamFactory = notNull("streamFactory", streamFactory);
        this.compressorList = notNull("compressorList", compressorList);
        this.compressorMap = createCompressorMap(compressorList);
        this.loggerSettings = loggerSettings;
        this.commandListener = commandListener;
        this.connectionInitializer = notNull("connectionInitializer", connectionInitializer);
        description = new ConnectionDescription(serverId);
        initialServerDescription = ServerDescription.builder()
                .address(serverId.getAddress())
                .type(ServerType.UNKNOWN)
                .state(ServerConnectionState.CONNECTING)
                .build();
        this.inetAddressResolver = inetAddressResolver;
        if (clusterConnectionMode != ClusterConnectionMode.LOAD_BALANCED) {
            generation = connectionGenerationSupplier.getGeneration();
        }
    }

    @Override
    public ConnectionDescription getDescription() {
        return description;
    }

    @Override
    public ServerDescription getInitialServerDescription() {
       return initialServerDescription;
    }

    @Override
    public int getGeneration() {
        return generation;
    }

    @Override
    public void open() {
        isTrue("Open already called", stream == null);
        stream = streamFactory.create(getServerAddressWithResolver());
        try {
            stream.open();

            InternalConnectionInitializationDescription initializationDescription = connectionInitializer.startHandshake(this);
            initAfterHandshakeStart(initializationDescription);

            initializationDescription = connectionInitializer.finishHandshake(this, initializationDescription);
            initAfterHandshakeFinish(initializationDescription);
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
            stream = streamFactory.create(getServerAddressWithResolver());
            stream.openAsync(new AsyncCompletionHandler<Void>() {
                @Override
                public void completed(@Nullable final Void aVoid) {
                    connectionInitializer.startHandshakeAsync(InternalStreamConnection.this,
                            (initialResult, initialException) -> {
                                    if (initialException != null) {
                                        close();
                                        callback.onResult(null, initialException);
                                    } else {
                                        assertNotNull(initialResult);
                                        initAfterHandshakeStart(initialResult);
                                        connectionInitializer.finishHandshakeAsync(InternalStreamConnection.this,
                                                initialResult, (completedResult, completedException) ->  {
                                                        if (completedException != null) {
                                                            close();
                                                            callback.onResult(null, completedException);
                                                        } else {
                                                            assertNotNull(completedResult);
                                                            initAfterHandshakeFinish(completedResult);
                                                            callback.onResult(null, null);
                                                        }
                                                });
                                    }
                            });
                }

                @Override
                public void failed(final Throwable t) {
                    close();
                    callback.onResult(null, t);
                }
            });
        } catch (Throwable t) {
            close();
            callback.onResult(null, t);
        }
    }

    private ServerAddress getServerAddressWithResolver() {
        if (serverId.getAddress() instanceof UnixServerAddress) {
            return serverId.getAddress();
        } else {
            return new ServerAddressWithResolver(serverId.getAddress(), inetAddressResolver);
        }
    }

    private void initAfterHandshakeStart(final InternalConnectionInitializationDescription initializationDescription) {
        description = initializationDescription.getConnectionDescription();
        initialServerDescription = initializationDescription.getServerDescription();

        if (clusterConnectionMode == ClusterConnectionMode.LOAD_BALANCED) {
            generation = connectionGenerationSupplier.getGeneration(assertNotNull(description.getServiceId()));
        }
    }

    private void initAfterHandshakeFinish(final InternalConnectionInitializationDescription initializationDescription) {
        description = initializationDescription.getConnectionDescription();
        initialServerDescription = initializationDescription.getServerDescription();
        opened.set(true);
        sendCompressor = findSendCompressor(description);
    }

    private Map<Byte, Compressor> createCompressorMap(final List<MongoCompressor> compressorList) {
        Map<Byte, Compressor> compressorMap = new HashMap<>(this.compressorList.size());

        for (MongoCompressor mongoCompressor : compressorList) {
            Compressor compressor = createCompressor(mongoCompressor);
            compressorMap.put(compressor.getId(), compressor);
        }
        return compressorMap;
    }

    @Nullable
    private Compressor findSendCompressor(final ConnectionDescription description) {
        if (description.getCompressors().isEmpty()) {
            return null;
        }

        String firstCompressorName = description.getCompressors().get(0);

        for (Compressor compressor : compressorMap.values()) {
            if (compressor.getName().equals(firstCompressorName)) {
                return compressor;
            }
        }

        throw new MongoInternalException("Unexpected compressor negotiated: " + firstCompressorName);
    }

    private Compressor createCompressor(final MongoCompressor mongoCompressor) {
        switch (mongoCompressor.getName()) {
            case "zlib":
                return new ZlibCompressor(mongoCompressor);
            case "snappy":
                return new SnappyCompressor();
            case "zstd":
                return new ZstdCompressor();
            default:
                throw new MongoClientException("Unsupported compressor " + mongoCompressor.getName());
        }
    }

    @Override
    public void close() {
        // All but the first call is a no-op
        if (!isClosed.getAndSet(true) && (stream != null)) {
                stream.close();
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

    @Nullable
    @Override
    public <T> T sendAndReceive(final CommandMessage message, final Decoder<T> decoder, final SessionContext sessionContext,
                                final RequestContext requestContext, final OperationContext operationContext) {
        CommandEventSender commandEventSender;

        try (ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(this)) {
            message.encode(bsonOutput, sessionContext);
            commandEventSender = createCommandEventSender(message, bsonOutput, requestContext, operationContext);
            commandEventSender.sendStartedEvent();
            try {
                sendCommandMessage(message, bsonOutput, sessionContext);
            } catch (Exception e) {
                commandEventSender.sendFailedEvent(e);
                throw e;
            }
        }

        if (message.isResponseExpected()) {
            return receiveCommandMessageResponse(decoder, commandEventSender, sessionContext, 0);
        } else {
            commandEventSender.sendSucceededEventForOneWayCommand();
            return null;
        }
    }

    @Override
    public <T> void send(final CommandMessage message, final Decoder<T> decoder, final SessionContext sessionContext) {
        try (ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(this)) {
            message.encode(bsonOutput, sessionContext);
            sendCommandMessage(message, bsonOutput, sessionContext);
            if (message.isResponseExpected()) {
                hasMoreToCome = true;
            }
        }
    }

    @Override
    public <T> T receive(final Decoder<T> decoder, final SessionContext sessionContext) {
        isTrue("Response is expected", hasMoreToCome);
        return receiveCommandMessageResponse(decoder, new NoOpCommandEventSender(), sessionContext, 0);
    }

    @Override
    public boolean supportsAdditionalTimeout() {
        return stream.supportsAdditionalTimeout();
    }

    @Override
    public <T> T receive(final Decoder<T> decoder, final SessionContext sessionContext, final int additionalTimeout) {
        isTrue("Response is expected", hasMoreToCome);
        return receiveCommandMessageResponse(decoder, new NoOpCommandEventSender(), sessionContext, additionalTimeout);
    }

    @Override
    public boolean hasMoreToCome() {
        return hasMoreToCome;
    }

    private void sendCommandMessage(final CommandMessage message,
                                    final ByteBufferBsonOutput bsonOutput, final SessionContext sessionContext) {

        Compressor localSendCompressor = sendCompressor;
        if (localSendCompressor == null || SECURITY_SENSITIVE_COMMANDS.contains(message.getCommandDocument(bsonOutput).getFirstKey())) {
            try {
                sendMessage(bsonOutput.getByteBuffers(), message.getId());
            } finally {
                bsonOutput.close();
            }
        } else {
            List<ByteBuf> byteBuffers = bsonOutput.getByteBuffers();
            ByteBufferBsonOutput compressedBsonOutput;
            try {
                CompressedMessage compressedMessage = new CompressedMessage(message.getOpCode(), byteBuffers, localSendCompressor,
                        getMessageSettings(description));
                compressedBsonOutput = new ByteBufferBsonOutput(this);
                compressedMessage.encode(compressedBsonOutput, sessionContext);
            } finally {
                releaseAllBuffers(byteBuffers);
                bsonOutput.close();
            }
            try {
                sendMessage(compressedBsonOutput.getByteBuffers(), message.getId());
            } finally {
                compressedBsonOutput.close();
            }
        }
        responseTo = message.getId();
    }

    private <T> T receiveCommandMessageResponse(final Decoder<T> decoder,
                                                final CommandEventSender commandEventSender, final SessionContext sessionContext,
                                                final int additionalTimeout) {
        boolean commandSuccessful = false;
        try (ResponseBuffers responseBuffers = receiveMessageWithAdditionalTimeout(additionalTimeout)) {
            updateSessionContext(sessionContext, responseBuffers);
            if (!isCommandOk(responseBuffers)) {
                throw getCommandFailureException(responseBuffers.getResponseDocument(responseTo,
                        new BsonDocumentCodec()), description.getServerAddress());
            }

            commandSuccessful = true;
            commandEventSender.sendSucceededEvent(responseBuffers);

            T commandResult = getCommandResult(decoder, responseBuffers, responseTo);
            hasMoreToCome = responseBuffers.getReplyHeader().hasMoreToCome();
            if (hasMoreToCome) {
                responseTo = responseBuffers.getReplyHeader().getRequestId();
            } else {
                responseTo = 0;
            }

            return commandResult;
        } catch (Exception e) {
            if (!commandSuccessful) {
                commandEventSender.sendFailedEvent(e);
            }
            throw e;
    }
    }

    @Override
    public <T> void sendAndReceiveAsync(final CommandMessage message, final Decoder<T> decoder, final SessionContext sessionContext,
            final RequestContext requestContext, final OperationContext operationContext, final SingleResultCallback<T> callback) {
        notNull("stream is open", stream, callback);

        if (isClosed()) {
            callback.onResult(null, new MongoSocketClosedException("Can not read from a closed socket", getServerAddress()));
            return;
        }

        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(this);
        ByteBufferBsonOutput compressedBsonOutput = new ByteBufferBsonOutput(this);

        try {
            message.encode(bsonOutput, sessionContext);
            CommandEventSender commandEventSender = createCommandEventSender(message, bsonOutput, requestContext, operationContext);
            commandEventSender.sendStartedEvent();
            Compressor localSendCompressor = sendCompressor;
            if (localSendCompressor == null || SECURITY_SENSITIVE_COMMANDS.contains(message.getCommandDocument(bsonOutput).getFirstKey())) {
                sendCommandMessageAsync(message.getId(), decoder, sessionContext, callback, bsonOutput, commandEventSender,
                        message.isResponseExpected());
            } else {
                List<ByteBuf> byteBuffers = bsonOutput.getByteBuffers();
                try {
                    CompressedMessage compressedMessage = new CompressedMessage(message.getOpCode(), byteBuffers, localSendCompressor,
                            getMessageSettings(description));
                    compressedMessage.encode(compressedBsonOutput, sessionContext);
                } finally {
                    releaseAllBuffers(byteBuffers);
                    bsonOutput.close();
                }
                sendCommandMessageAsync(message.getId(), decoder, sessionContext, callback, compressedBsonOutput, commandEventSender,
                        message.isResponseExpected());
            }
        } catch (Throwable t) {
            bsonOutput.close();
            compressedBsonOutput.close();
            callback.onResult(null, t);
        }
    }

    private void releaseAllBuffers(final List<ByteBuf> byteBuffers) {
        for (ByteBuf cur : byteBuffers) {
            cur.release();
        }
    }

    private <T> void sendCommandMessageAsync(final int messageId, final Decoder<T> decoder, final SessionContext sessionContext,
                                             final SingleResultCallback<T> callback, final ByteBufferBsonOutput bsonOutput,
                                             final CommandEventSender commandEventSender, final boolean responseExpected) {
        sendMessageAsync(bsonOutput.getByteBuffers(), messageId, (result, t) -> {
            bsonOutput.close();
            if (t != null) {
                commandEventSender.sendFailedEvent(t);
                callback.onResult(null, t);
            } else if (!responseExpected) {
                commandEventSender.sendSucceededEventForOneWayCommand();
                callback.onResult(null, null);
            } else {
                readAsync(MESSAGE_HEADER_LENGTH, new MessageHeaderCallback((responseBuffers, t1) -> {
                    if (t1 != null) {
                        commandEventSender.sendFailedEvent(t1);
                        callback.onResult(null, t1);
                        return;
                    }
                    assertNotNull(responseBuffers);
                    try {
                        updateSessionContext(sessionContext, responseBuffers);
                        boolean commandOk =
                                isCommandOk(new BsonBinaryReader(new ByteBufferBsonInput(responseBuffers.getBodyByteBuffer())));
                        responseBuffers.reset();
                        if (!commandOk) {
                            MongoException commandFailureException = getCommandFailureException(
                                    responseBuffers.getResponseDocument(messageId, new BsonDocumentCodec()),
                                    description.getServerAddress());
                            commandEventSender.sendFailedEvent(commandFailureException);
                            throw commandFailureException;
                        }
                        commandEventSender.sendSucceededEvent(responseBuffers);

                        T result1 = getCommandResult(decoder, responseBuffers, messageId);
                        callback.onResult(result1, null);
                    } catch (Throwable localThrowable) {
                        callback.onResult(null, localThrowable);
                    } finally {
                        responseBuffers.close();
                    }
                }));
            }
        });
    }

    private <T> T getCommandResult(final Decoder<T> decoder, final ResponseBuffers responseBuffers, final int messageId) {
        T result = new ReplyMessage<>(responseBuffers, decoder, messageId).getDocuments().get(0);
        MongoException writeConcernBasedError = createSpecialWriteConcernException(responseBuffers, description.getServerAddress());
        if (writeConcernBasedError != null) {
            throw new MongoWriteConcernWithResponseException(writeConcernBasedError, result);
        }
        return result;
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

        return receiveMessageWithAdditionalTimeout(0);
    }

    private ResponseBuffers receiveMessageWithAdditionalTimeout(final int additionalTimeout) {
        try {
            return receiveResponseBuffers(additionalTimeout);
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
        try {
            stream.writeAsync(byteBuffers, new AsyncCompletionHandler<Void>() {
                @Override
                public void completed(@Nullable final Void v) {
                    callback.onResult(null, null);
                }

                @Override
                public void failed(final Throwable t) {
                    close();
                    callback.onResult(null, translateWriteException(t));
                }
            });
        } catch (Throwable t) {
            close();
            callback.onResult(null, t);
        }
    }

    @Override
    public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
        isTrue("stream is open", stream != null, callback);

        if (isClosed()) {
            callback.onResult(null, new MongoSocketClosedException("Can not read from a closed socket", getServerAddress()));
            return;
        }

        readAsync(MESSAGE_HEADER_LENGTH, new MessageHeaderCallback((result, t) -> {
            if (t != null) {
                close();
                callback.onResult(null, t);
            } else {
                callback.onResult(result, null);
            }
        }));
    }

    private void readAsync(final int numBytes, final SingleResultCallback<ByteBuf> callback) {
        if (isClosed()) {
            callback.onResult(null, new MongoSocketClosedException("Cannot read from a closed stream", getServerAddress()));
            return;
        }

        try {
            stream.readAsync(numBytes, new AsyncCompletionHandler<ByteBuf>() {
                @Override
                public void completed(@Nullable final ByteBuf buffer) {
                    callback.onResult(buffer, null);
                }

                @Override
                public void failed(final Throwable t) {
                    close();
                    callback.onResult(null, translateReadException(t));
                }
            });
        } catch (Exception e) {
            close();
            callback.onResult(null, translateReadException(e));
        }
    }

    private ConnectionId getId() {
        return description.getConnectionId();
    }

    private ServerAddress getServerAddress() {
        return description.getServerAddress();
    }

    private void updateSessionContext(final SessionContext sessionContext, final ResponseBuffers responseBuffers) {
        sessionContext.advanceOperationTime(getOperationTime(responseBuffers));
        sessionContext.advanceClusterTime(getClusterTime(responseBuffers));
        sessionContext.setSnapshotTimestamp(getSnapshotTimestamp(responseBuffers));
        if (sessionContext.hasActiveTransaction()) {
            BsonDocument recoveryToken = getRecoveryToken(responseBuffers);
            if (recoveryToken != null) {
                sessionContext.setRecoveryToken(recoveryToken);
            }
        }
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

    private ResponseBuffers receiveResponseBuffers(final int additionalTimeout) throws IOException {
        ByteBuf messageHeaderBuffer = stream.read(MESSAGE_HEADER_LENGTH, additionalTimeout);
        MessageHeader messageHeader;
        try {
            messageHeader = new MessageHeader(messageHeaderBuffer, description.getMaxMessageSize());
        } finally {
            messageHeaderBuffer.release();
        }

        ByteBuf messageBuffer = stream.read(messageHeader.getMessageLength() - MESSAGE_HEADER_LENGTH, additionalTimeout);
        boolean releaseMessageBuffer = true;
        try {
            if (messageHeader.getOpCode() == OP_COMPRESSED.getValue()) {
                CompressedHeader compressedHeader = new CompressedHeader(messageBuffer, messageHeader);

                Compressor compressor = getCompressor(compressedHeader);

                ByteBuf buffer = getBuffer(compressedHeader.getUncompressedSize());
                compressor.uncompress(messageBuffer, buffer);

                buffer.flip();
                return new ResponseBuffers(new ReplyHeader(buffer, compressedHeader), buffer);
            } else {
                ResponseBuffers responseBuffers = new ResponseBuffers(new ReplyHeader(messageBuffer, messageHeader), messageBuffer);
                releaseMessageBuffer = false;
                return responseBuffers;
            }
        } finally {
            if (releaseMessageBuffer) {
                messageBuffer.release();
            }
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

    private class MessageHeaderCallback implements SingleResultCallback<ByteBuf> {
        private final SingleResultCallback<ResponseBuffers> callback;

        MessageHeaderCallback(final SingleResultCallback<ResponseBuffers> callback) {
            this.callback = callback;
        }

        @Override
        public void onResult(@Nullable final ByteBuf result, @Nullable final Throwable t) {
            if (t != null) {
                callback.onResult(null, t);
                return;
            }
            try {
                assertNotNull(result);
                MessageHeader messageHeader = new MessageHeader(result, description.getMaxMessageSize());
                readAsync(messageHeader.getMessageLength() - MESSAGE_HEADER_LENGTH, new MessageCallback(messageHeader));
            } catch (Throwable localThrowable) {
                callback.onResult(null, localThrowable);
            } finally {
                if (result != null) {
                    result.release();
                }
            }
        }

        private class MessageCallback implements SingleResultCallback<ByteBuf> {
            private final MessageHeader messageHeader;

            MessageCallback(final MessageHeader messageHeader) {
                this.messageHeader = messageHeader;
            }

            @Override
            public void onResult(@Nullable final ByteBuf result, @Nullable final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                    return;
                }
                boolean releaseResult = true;
                assertNotNull(result);
                try {
                    ReplyHeader replyHeader;
                    ByteBuf responseBuffer;
                    if (messageHeader.getOpCode() == OP_COMPRESSED.getValue()) {
                        try {
                            CompressedHeader compressedHeader = new CompressedHeader(result, messageHeader);
                            Compressor compressor = getCompressor(compressedHeader);
                            ByteBuf buffer = getBuffer(compressedHeader.getUncompressedSize());
                            compressor.uncompress(result, buffer);

                            buffer.flip();
                            replyHeader = new ReplyHeader(buffer, compressedHeader);
                            responseBuffer = buffer;
                        } finally {
                            releaseResult = false;
                            result.release();
                        }
                    } else {
                        replyHeader = new ReplyHeader(result, messageHeader);
                        responseBuffer = result;
                        releaseResult = false;
                    }
                    callback.onResult(new ResponseBuffers(replyHeader, responseBuffer), null);
                } catch (Throwable localThrowable) {
                    callback.onResult(null, localThrowable);
                } finally {
                    if (releaseResult) {
                        result.release();
                    }
                }
            }
        }
    }

    private static final StructuredLogger COMMAND_PROTOCOL_LOGGER = new StructuredLogger("protocol.command");

    private CommandEventSender createCommandEventSender(final CommandMessage message, final ByteBufferBsonOutput bsonOutput,
            final RequestContext requestContext, final OperationContext operationContext) {
        if (!isMonitoringConnection && opened() && (commandListener != null || COMMAND_PROTOCOL_LOGGER.isRequired(DEBUG, getClusterId()))) {
            return new LoggingCommandEventSender(SECURITY_SENSITIVE_COMMANDS, SECURITY_SENSITIVE_HELLO_COMMANDS, description,
                    commandListener, requestContext, operationContext, message, bsonOutput, COMMAND_PROTOCOL_LOGGER, loggerSettings);
        } else {
            return new NoOpCommandEventSender();
        }
    }

    private ClusterId getClusterId() {
        return description.getConnectionId().getServerId().getClusterId();
    }
}
