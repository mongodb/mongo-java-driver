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
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCompressor;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.MongoSocketClosedException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.MongoSocketWriteException;
import com.mongodb.MongoSocketWriteTimeoutException;
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
import com.mongodb.event.CommandListener;
import com.mongodb.internal.ResourceUtil;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.async.AsyncSupplier;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.internal.logging.StructuredLogger;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.time.Timeout;
import com.mongodb.internal.tracing.Span;
import com.mongodb.internal.tracing.TracingManager;
import com.mongodb.lang.Nullable;
import io.micrometer.common.KeyValues;
import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
import org.bson.ByteBuf;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;
import org.bson.io.ByteBufferBsonInput;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertNull;
import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.TimeoutContext.createMongoTimeoutException;
import static com.mongodb.internal.async.AsyncRunnable.beginAsync;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.connection.Authenticator.shouldAuthenticate;
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
import static com.mongodb.internal.logging.LogMessage.Level.DEBUG;
import static com.mongodb.internal.thread.InterruptionUtil.translateInterruptedException;
import static com.mongodb.tracing.MongodbObservation.HighCardinalityKeyNames.QUERY_TEXT;
import static com.mongodb.tracing.MongodbObservation.LowCardinalityKeyNames.CLIENT_CONNECTION_ID;
import static com.mongodb.tracing.MongodbObservation.LowCardinalityKeyNames.COLLECTION;
import static com.mongodb.tracing.MongodbObservation.LowCardinalityKeyNames.COMMAND_NAME;
import static com.mongodb.tracing.MongodbObservation.LowCardinalityKeyNames.CURSOR_ID;
import static com.mongodb.tracing.MongodbObservation.LowCardinalityKeyNames.NAMESPACE;
import static com.mongodb.tracing.MongodbObservation.LowCardinalityKeyNames.NETWORK_TRANSPORT;
import static com.mongodb.tracing.MongodbObservation.LowCardinalityKeyNames.QUERY_SUMMARY;
import static com.mongodb.tracing.MongodbObservation.LowCardinalityKeyNames.RESPONSE_STATUS_CODE;
import static com.mongodb.tracing.MongodbObservation.LowCardinalityKeyNames.SERVER_ADDRESS;
import static com.mongodb.tracing.MongodbObservation.LowCardinalityKeyNames.SERVER_CONNECTION_ID;
import static com.mongodb.tracing.MongodbObservation.LowCardinalityKeyNames.SERVER_PORT;
import static com.mongodb.tracing.MongodbObservation.LowCardinalityKeyNames.SERVER_TYPE;
import static com.mongodb.tracing.MongodbObservation.LowCardinalityKeyNames.SESSION_ID;
import static com.mongodb.tracing.MongodbObservation.LowCardinalityKeyNames.SYSTEM;
import static com.mongodb.tracing.MongodbObservation.LowCardinalityKeyNames.TRANSACTION_NUMBER;
import static java.util.Arrays.asList;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
@NotThreadSafe
public class InternalStreamConnection implements InternalConnection {

    private static volatile boolean recordEverything = false;

    /**
     * Will attempt to record events to the command listener that are usually
     * suppressed.
     *
     * @param recordEverything whether to attempt to record everything
     */
    @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
    public static void setRecordEverything(final boolean recordEverything) {
        InternalStreamConnection.recordEverything = recordEverything;
    }

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
    @Nullable
    private final Authenticator authenticator;
    private final boolean isMonitoringConnection;
    private final ServerId serverId;
    private final ConnectionGenerationSupplier connectionGenerationSupplier;
    private final StreamFactory streamFactory;
    private final InternalConnectionInitializer connectionInitializer;
    private volatile ConnectionDescription description;
    private volatile ServerDescription initialServerDescription;
    private volatile Stream stream;

    private final AtomicBoolean isClosed = new AtomicBoolean();
    private final AtomicBoolean opened = new AtomicBoolean();
    private final AtomicBoolean authenticated = new AtomicBoolean();

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
            final CommandListener commandListener, final InternalConnectionInitializer connectionInitializer) {
        this(clusterConnectionMode, null, false, serverId, connectionGenerationSupplier, streamFactory, compressorList,
                LoggerSettings.builder().build(), commandListener, connectionInitializer);
    }

    public InternalStreamConnection(final ClusterConnectionMode clusterConnectionMode,
            @Nullable final Authenticator authenticator,
            final boolean isMonitoringConnection,
            final ServerId serverId,
            final ConnectionGenerationSupplier connectionGenerationSupplier,
            final StreamFactory streamFactory, final List<MongoCompressor> compressorList,
            final LoggerSettings loggerSettings,
            final CommandListener commandListener, final InternalConnectionInitializer connectionInitializer) {
        this.clusterConnectionMode = clusterConnectionMode;
        this.authenticator = authenticator;
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
    public void open(final OperationContext originalOperationContext) {
        isTrue("Open already called", stream == null);
        stream = streamFactory.create(serverId.getAddress());
        try {
            OperationContext operationContext = originalOperationContext
                    .withTimeoutContext(originalOperationContext.getTimeoutContext().withComputedServerSelectionTimeoutContext());

            stream.open(operationContext);

            InternalConnectionInitializationDescription initializationDescription = connectionInitializer.startHandshake(this, operationContext);
            initAfterHandshakeStart(initializationDescription);

            initializationDescription = connectionInitializer.finishHandshake(this, initializationDescription, operationContext);
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
    public void openAsync(final OperationContext originalOperationContext, final SingleResultCallback<Void> callback) {
        assertNull(stream);
        try {
            OperationContext operationContext = originalOperationContext
                    .withTimeoutContext(originalOperationContext.getTimeoutContext().withComputedServerSelectionTimeoutContext());

            stream = streamFactory.create(serverId.getAddress());
            stream.openAsync(operationContext, new AsyncCompletionHandler<Void>() {

                @Override
                public void completed(@Nullable final Void aVoid) {
                    connectionInitializer.startHandshakeAsync(InternalStreamConnection.this, operationContext,
                            (initialResult, initialException) -> {
                                    if (initialException != null) {
                                        close();
                                        callback.onResult(null, initialException);
                                    } else {
                                        assertNotNull(initialResult);
                                        initAfterHandshakeStart(initialResult);
                                        connectionInitializer.finishHandshakeAsync(InternalStreamConnection.this,
                                                initialResult, operationContext, (completedResult, completedException) ->  {
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
        authenticated.set(true);
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
    public <T> T sendAndReceive(final CommandMessage message, final Decoder<T> decoder, final OperationContext operationContext) {
        Supplier<T> sendAndReceiveInternal = () -> sendAndReceiveInternal(
                message, decoder, operationContext);
        try {
            return sendAndReceiveInternal.get();
        } catch (MongoCommandException e) {
            if (reauthenticationIsTriggered(e)) {
                return reauthenticateAndRetry(sendAndReceiveInternal, operationContext);
            }
            throw e;
        }
    }

    @Override
    public <T> void sendAndReceiveAsync(final CommandMessage message, final Decoder<T> decoder,
                                        final OperationContext operationContext,
                                        final SingleResultCallback<T> callback) {

        AsyncSupplier<T> sendAndReceiveAsyncInternal = c -> sendAndReceiveAsyncInternal(
                message, decoder, operationContext, c);
        beginAsync().<T>thenSupply(c -> {
            sendAndReceiveAsyncInternal.getAsync(c);
        }).onErrorIf(e -> reauthenticationIsTriggered(e), (t, c) -> {
            reauthenticateAndRetryAsync(sendAndReceiveAsyncInternal, operationContext, c);
        }).finish(callback);
    }

    private <T> T reauthenticateAndRetry(final Supplier<T> operation, final OperationContext operationContext) {
        authenticated.set(false);
        assertNotNull(authenticator).reauthenticate(this, operationContext);
        authenticated.set(true);
        return operation.get();
    }

    private <T> void reauthenticateAndRetryAsync(final AsyncSupplier<T> operation,
            final OperationContext operationContext,
            final SingleResultCallback<T> callback) {
        beginAsync().thenRun(c -> {
            authenticated.set(false);
            assertNotNull(authenticator).reauthenticateAsync(this, operationContext, c);
        }).<T>thenSupply((c) -> {
            authenticated.set(true);
            operation.getAsync(c);
        }).finish(callback);
    }

    public boolean reauthenticationIsTriggered(@Nullable final Throwable t) {
        if (!shouldAuthenticate(authenticator, this.description)) {
            return false;
        }
        if (t instanceof MongoCommandException) {
            MongoCommandException e = (MongoCommandException) t;
            return e.getErrorCode() == 391;
        }
        return false;
    }

    @Nullable
    private <T> T sendAndReceiveInternal(final CommandMessage message, final Decoder<T> decoder,
            final OperationContext operationContext) {
        CommandEventSender commandEventSender;
        Span tracingSpan;
        try (ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(this)) {
            message.encode(bsonOutput, operationContext);
            tracingSpan = createTracingSpan(message, operationContext, bsonOutput);

            boolean isLoggingCommandNeeded = isLoggingCommandNeeded();
            boolean isTracingCommandPayloadNeeded = tracingSpan != null && operationContext.getTracingManager().isCommandPayloadEnabled();

            // Only hydrate the command document if necessary
            BsonDocument commandDocument = null;
            if (isLoggingCommandNeeded || isTracingCommandPayloadNeeded) {
                commandDocument = message.getCommandDocument(bsonOutput);
            }
            if (isLoggingCommandNeeded) {
                commandEventSender = new LoggingCommandEventSender(
                        SECURITY_SENSITIVE_COMMANDS, SECURITY_SENSITIVE_HELLO_COMMANDS, description, commandListener,
                        operationContext, message, commandDocument,
                        COMMAND_PROTOCOL_LOGGER, loggerSettings);
                commandEventSender.sendStartedEvent();
            } else {
                commandEventSender = new NoOpCommandEventSender();
            }
            if (isTracingCommandPayloadNeeded) {
                tracingSpan.tagHighCardinality(QUERY_TEXT.withValue(commandDocument.toJson()));
            }

            try {
                sendCommandMessage(message, bsonOutput, operationContext);
            } catch (Exception e) {
                if (tracingSpan != null) {
                    tracingSpan.error(e);
                }
                commandEventSender.sendFailedEvent(e);
                throw e;
            }
        }

        if (message.isResponseExpected()) {
            return receiveCommandMessageResponse(decoder, commandEventSender, operationContext, tracingSpan);
        } else {
            commandEventSender.sendSucceededEventForOneWayCommand();
            if (tracingSpan != null) {
                tracingSpan.end();
            }
            return null;
        }
    }

    @Override
    public <T> void send(final CommandMessage message, final Decoder<T> decoder, final OperationContext operationContext) {
        try (ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(this)) {
            message.encode(bsonOutput, operationContext);
            sendCommandMessage(message, bsonOutput, operationContext);
            if (message.isResponseExpected()) {
                hasMoreToCome = true;
            }
        }
    }

    @Override
    public <T> T receive(final Decoder<T> decoder, final OperationContext operationContext) {
        isTrue("Response is expected", hasMoreToCome);
        return receiveCommandMessageResponse(decoder, new NoOpCommandEventSender(), operationContext, null);
    }

    @Override
    public boolean hasMoreToCome() {
        return hasMoreToCome;
    }

    private void sendCommandMessage(final CommandMessage message, final ByteBufferBsonOutput bsonOutput,
            final OperationContext operationContext) {

        Compressor localSendCompressor = sendCompressor;
        if (localSendCompressor == null || SECURITY_SENSITIVE_COMMANDS.contains(message.getCommandDocument(bsonOutput).getFirstKey())) {
            trySendMessage(message, bsonOutput, operationContext);
        } else {
            ByteBufferBsonOutput compressedBsonOutput;
            List<ByteBuf> byteBuffers = bsonOutput.getByteBuffers();
            try {
                CompressedMessage compressedMessage = new CompressedMessage(message.getOpCode(), byteBuffers, localSendCompressor,
                        getMessageSettings(description, initialServerDescription));
                compressedBsonOutput = new ByteBufferBsonOutput(this);
                compressedMessage.encode(compressedBsonOutput, operationContext);
            } finally {
                ResourceUtil.release(byteBuffers);
                bsonOutput.close();
            }
            trySendMessage(message, compressedBsonOutput, operationContext);
        }
        responseTo = message.getId();
    }

    private void trySendMessage(final CommandMessage message, final ByteBufferBsonOutput bsonOutput,
            final OperationContext operationContext) {
        Timeout.onExistsAndExpired(operationContext.getTimeoutContext().timeoutIncludingRoundTrip(), () -> {
            throw TimeoutContext.createMongoRoundTripTimeoutException();
        });
        List<ByteBuf> byteBuffers = bsonOutput.getByteBuffers();
        try {
            sendMessage(byteBuffers, message.getId(), operationContext);
        } finally {
            ResourceUtil.release(byteBuffers);
            bsonOutput.close();
        }
    }

    private <T> T receiveCommandMessageResponse(final Decoder<T> decoder, final CommandEventSender commandEventSender,
            final OperationContext operationContext, @Nullable final Span tracingSpan) {
        boolean commandSuccessful = false;
        try (ResponseBuffers responseBuffers = receiveResponseBuffers(operationContext)) {
            updateSessionContext(operationContext.getSessionContext(), responseBuffers);
            if (!isCommandOk(responseBuffers)) {
                throw getCommandFailureException(responseBuffers.getResponseDocument(responseTo,
                        new BsonDocumentCodec()), description.getServerAddress(), operationContext.getTimeoutContext());
            }

            commandSuccessful = true;
            commandEventSender.sendSucceededEvent(responseBuffers);

            T commandResult = getCommandResult(decoder, responseBuffers, responseTo, operationContext.getTimeoutContext());
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
            if (tracingSpan != null) {
                if (e instanceof MongoCommandException) {
                    tracingSpan.tagLowCardinality(RESPONSE_STATUS_CODE.withValue(String.valueOf(((MongoCommandException) e).getErrorCode())));
                }
                tracingSpan.error(e);
            }
            throw e;
        } finally {
            if (tracingSpan != null) {
                tracingSpan.end();
            }
        }
    }

    private <T> void sendAndReceiveAsyncInternal(final CommandMessage message, final Decoder<T> decoder,
                                                 final OperationContext operationContext, final SingleResultCallback<T> callback) {
        if (isClosed()) {
            callback.onResult(null, new MongoSocketClosedException("Can not read from a closed socket", getServerAddress()));
            return;
        }

        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(this);
        ByteBufferBsonOutput compressedBsonOutput = new ByteBufferBsonOutput(this);

        try {
            message.encode(bsonOutput, operationContext);

            CommandEventSender commandEventSender;
            if (isLoggingCommandNeeded()) {
                BsonDocument commandDocument = message.getCommandDocument(bsonOutput);
                commandEventSender = new LoggingCommandEventSender(
                        SECURITY_SENSITIVE_COMMANDS, SECURITY_SENSITIVE_HELLO_COMMANDS, description, commandListener,
                        operationContext, message, commandDocument,
                        COMMAND_PROTOCOL_LOGGER, loggerSettings);
            } else {
                commandEventSender = new NoOpCommandEventSender();
            }

            commandEventSender.sendStartedEvent();
            Compressor localSendCompressor = sendCompressor;
            if (localSendCompressor == null || SECURITY_SENSITIVE_COMMANDS.contains(message.getCommandDocument(bsonOutput).getFirstKey())) {
                sendCommandMessageAsync(message.getId(), decoder, operationContext, callback, bsonOutput, commandEventSender,
                        message.isResponseExpected());
            } else {
                List<ByteBuf> byteBuffers = bsonOutput.getByteBuffers();
                try {
                    CompressedMessage compressedMessage = new CompressedMessage(message.getOpCode(), byteBuffers, localSendCompressor,
                            getMessageSettings(description, initialServerDescription));
                    compressedMessage.encode(compressedBsonOutput, operationContext);
                } finally {
                    ResourceUtil.release(byteBuffers);
                    bsonOutput.close();
                }
                sendCommandMessageAsync(message.getId(), decoder, operationContext, callback, compressedBsonOutput, commandEventSender,
                        message.isResponseExpected());
            }
        } catch (Throwable t) {
            bsonOutput.close();
            compressedBsonOutput.close();
            callback.onResult(null, t);
        }
    }

    private <T> void sendCommandMessageAsync(final int messageId, final Decoder<T> decoder, final OperationContext operationContext,
                                             final SingleResultCallback<T> callback, final ByteBufferBsonOutput bsonOutput,
                                             final CommandEventSender commandEventSender, final boolean responseExpected) {
        boolean[] shouldReturn = {false};
        Timeout.onExistsAndExpired(operationContext.getTimeoutContext().timeoutIncludingRoundTrip(), () -> {
            bsonOutput.close();
            MongoOperationTimeoutException operationTimeoutException = TimeoutContext.createMongoRoundTripTimeoutException();
            commandEventSender.sendFailedEvent(operationTimeoutException);
            callback.onResult(null, operationTimeoutException);
            shouldReturn[0] = true;
        });
        if (shouldReturn[0]) {
            return;
        }

        List<ByteBuf> byteBuffers = bsonOutput.getByteBuffers();
        sendMessageAsync(byteBuffers, messageId, operationContext, (result, t) -> {
            ResourceUtil.release(byteBuffers);
            bsonOutput.close();
            if (t != null) {
                commandEventSender.sendFailedEvent(t);
                callback.onResult(null, t);
            } else if (!responseExpected) {
                commandEventSender.sendSucceededEventForOneWayCommand();
                callback.onResult(null, null);
            } else {
                readAsync(MESSAGE_HEADER_LENGTH, operationContext, new MessageHeaderCallback(operationContext, (responseBuffers, t1) -> {
                    if (t1 != null) {
                        commandEventSender.sendFailedEvent(t1);
                        callback.onResult(null, t1);
                        return;
                    }
                    assertNotNull(responseBuffers);
                    T commandResult;
                    try {
                        updateSessionContext(operationContext.getSessionContext(), responseBuffers);
                        boolean commandOk =
                                isCommandOk(new BsonBinaryReader(new ByteBufferBsonInput(responseBuffers.getBodyByteBuffer())));
                        responseBuffers.reset();
                        if (!commandOk) {
                            MongoException commandFailureException = getCommandFailureException(
                                    responseBuffers.getResponseDocument(messageId, new BsonDocumentCodec()),
                                    description.getServerAddress(), operationContext.getTimeoutContext());
                            commandEventSender.sendFailedEvent(commandFailureException);
                            throw commandFailureException;
                        }
                        commandEventSender.sendSucceededEvent(responseBuffers);

                        commandResult = getCommandResult(decoder, responseBuffers, messageId, operationContext.getTimeoutContext());
                    } catch (Throwable localThrowable) {
                        callback.onResult(null, localThrowable);
                        return;
                    } finally {
                        responseBuffers.close();
                    }
                    callback.onResult(commandResult, null);
                }));
            }
        });
    }

    private <T> T getCommandResult(final Decoder<T> decoder,
                                   final ResponseBuffers responseBuffers,
                                   final int messageId,
                                   final TimeoutContext timeoutContext) {
        T result = new ReplyMessage<>(responseBuffers, decoder, messageId).getDocument();
        MongoException writeConcernBasedError = createSpecialWriteConcernException(responseBuffers,
                description.getServerAddress(),
                timeoutContext);
        if (writeConcernBasedError instanceof MongoOperationTimeoutException) {
            throw writeConcernBasedError;
        }
        if (writeConcernBasedError != null) {
            throw new MongoWriteConcernWithResponseException(writeConcernBasedError, result);
        }
        return result;
    }

    @Override
    public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId, final OperationContext operationContext) {
        notNull("stream is open", stream);
        if (isClosed()) {
            throw new MongoSocketClosedException("Cannot write to a closed stream", getServerAddress());
        }
        try {
            stream.write(byteBuffers, operationContext);
        } catch (Exception e) {
            close();
            throwTranslatedWriteException(e, operationContext);
        }
    }

    @Override
    public void sendMessageAsync(
            final List<ByteBuf> byteBuffers,
            final int lastRequestId,
            final OperationContext operationContext,
            final SingleResultCallback<Void> callback) {
        beginAsync().thenRun((c) -> {
            notNull("stream is open", stream);
            if (isClosed()) {
                throw new MongoSocketClosedException("Cannot write to a closed stream", getServerAddress());
            }
            c.complete(c);
        }).thenRunTryCatchAsyncBlocks(c -> {
            stream.writeAsync(byteBuffers, operationContext, c.asHandler());
        }, Exception.class, (e, c) -> {
            try {
                close();
                throwTranslatedWriteException(e, operationContext);
            } catch (Throwable translatedException) {
                c.completeExceptionally(translatedException);
            }
        }).finish(errorHandlingCallback(callback, LOGGER));
    }

    @Override
    public ResponseBuffers receiveMessage(final int responseTo, final OperationContext operationContext) {
        assertNotNull(stream);
        if (isClosed()) {
            throw new MongoSocketClosedException("Cannot read from a closed stream", getServerAddress());
        }

        return receiveResponseBuffers(operationContext);
    }

    @Override
    public void receiveMessageAsync(final int responseTo, final OperationContext operationContext,
            final SingleResultCallback<ResponseBuffers> callback) {
        assertNotNull(stream);

        if (isClosed()) {
            callback.onResult(null, new MongoSocketClosedException("Can not read from a closed socket", getServerAddress()));
            return;
        }

        readAsync(MESSAGE_HEADER_LENGTH, operationContext, new MessageHeaderCallback(operationContext, (result, t) -> {
            if (t != null) {
                close();
                callback.onResult(null, t);
            } else {
                callback.onResult(result, null);
            }
        }));
    }

    private void readAsync(final int numBytes, final OperationContext operationContext, final SingleResultCallback<ByteBuf> callback) {
        if (isClosed()) {
            callback.onResult(null, new MongoSocketClosedException("Cannot read from a closed stream", getServerAddress()));
            return;
        }

        try {
            stream.readAsync(numBytes, operationContext, new AsyncCompletionHandler<ByteBuf>() {
                @Override
                public void completed(@Nullable final ByteBuf buffer) {
                    callback.onResult(buffer, null);
                }

                @Override
                public void failed(final Throwable t) {
                    close();
                    callback.onResult(null, translateReadException(t, operationContext));
                }
            });
        } catch (Exception e) {
            close();
            callback.onResult(null, translateReadException(e, operationContext));
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

    private void throwTranslatedWriteException(final Throwable e, final OperationContext operationContext) {
        if (e instanceof MongoSocketWriteTimeoutException && operationContext.getTimeoutContext().hasTimeoutMS()) {
            throw createMongoTimeoutException(e);
        }

        if (e instanceof MongoException) {
            throw (MongoException) e;
        }
        Optional<MongoInterruptedException> interruptedException = translateInterruptedException(e, "Interrupted while sending message");
        if (interruptedException.isPresent()) {
            throw interruptedException.get();
        } else if (e instanceof IOException) {
            throw new MongoSocketWriteException("Exception sending message", getServerAddress(), e);
        } else {
            throw new MongoInternalException("Unexpected exception", e);
        }
    }

    private MongoException translateReadException(final Throwable e, final OperationContext operationContext) {
        if (operationContext.getTimeoutContext().hasTimeoutMS()) {
            if (e instanceof SocketTimeoutException) {
                return createMongoTimeoutException(createReadTimeoutException((SocketTimeoutException) e));
            } else if (e instanceof MongoSocketReadTimeoutException) {
                return createMongoTimeoutException((e));
            }
        }

        if (e instanceof MongoException) {
            return (MongoException) e;
        }
        Optional<MongoInterruptedException> interruptedException = translateInterruptedException(e, "Interrupted while receiving message");
        if (interruptedException.isPresent()) {
            return interruptedException.get();
        } else if (e instanceof SocketTimeoutException) {
            return createReadTimeoutException((SocketTimeoutException) e);
        } else if (e instanceof IOException) {
            return new MongoSocketReadException("Exception receiving message", getServerAddress(), e);
        } else if (e instanceof RuntimeException) {
            return new MongoInternalException("Unexpected runtime exception", e);
        } else {
            return new MongoInternalException("Unexpected exception", e);
        }
    }

    private  MongoSocketReadTimeoutException createReadTimeoutException(final SocketTimeoutException e) {
        return new MongoSocketReadTimeoutException("Timeout while receiving message",
                getServerAddress(), e);
    }

    private ResponseBuffers receiveResponseBuffers(final OperationContext operationContext) {
        try {
            ByteBuf messageHeaderBuffer = stream.read(MESSAGE_HEADER_LENGTH, operationContext);
            MessageHeader messageHeader;
            try {
                messageHeader = new MessageHeader(messageHeaderBuffer, description.getMaxMessageSize());
            } finally {
                messageHeaderBuffer.release();
            }

            ByteBuf messageBuffer = stream.read(messageHeader.getMessageLength() - MESSAGE_HEADER_LENGTH, operationContext);
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
        } catch (Throwable t) {
            close();
            throw translateReadException(t, operationContext);
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
        private final OperationContext operationContext;
        private final SingleResultCallback<ResponseBuffers> callback;

        MessageHeaderCallback(final OperationContext operationContext, final SingleResultCallback<ResponseBuffers> callback) {
            this.operationContext = operationContext;
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
                readAsync(messageHeader.getMessageLength() - MESSAGE_HEADER_LENGTH, operationContext,
                        new MessageCallback(messageHeader));
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

    private boolean isLoggingCommandNeeded() {
        boolean listensOrLogs = commandListener != null || COMMAND_PROTOCOL_LOGGER.isRequired(DEBUG, getClusterId());
        return recordEverything || (!isMonitoringConnection && opened() && authenticated.get() && listensOrLogs);
    }

    private ClusterId getClusterId() {
        return description.getConnectionId().getServerId().getClusterId();
    }

    /**
     * Creates a tracing span for the given command message.
     * <p>
     * The span is only created if tracing is enabled and the command is not security-sensitive.
     * It attaches various tags to the span, such as database system, namespace, query summary, opcode,
     * server address, port, server type, client and server connection IDs, and, if applicable,
     * transaction number and session ID. For cursor fetching commands, the parent context is retrieved using the cursor ID.
     * If command payload tracing is enabled, the command document is also attached as a tag.
     *
     * @param message          the command message to trace
     * @param operationContext the operation context containing tracing and session information
     * @param bsonOutput       the BSON output used to serialize the command
     * @return the created {@link Span}, or {@code null} if tracing is not enabled or the command is security-sensitive
     */
    @Nullable
    private Span createTracingSpan(final CommandMessage message, final OperationContext operationContext, final ByteBufferBsonOutput bsonOutput) {

        TracingManager tracingManager = operationContext.getTracingManager();
        BsonDocument command = message.getCommandDocument(bsonOutput);

        String commandName = command.getFirstKey();

        if (!tracingManager.isEnabled()
                || SECURITY_SENSITIVE_COMMANDS.contains(commandName)
                || SECURITY_SENSITIVE_HELLO_COMMANDS.contains(commandName)) {
            return null;
        }

         Span operationSpan = operationContext.getTracingSpan();
         Span span = tracingManager
                .addSpan(commandName,  operationSpan != null ? operationSpan.context() : null);

        if (command.containsKey("getMore")) {
            long cursorId = command.getInt64("getMore").longValue();
            span.tagLowCardinality(CURSOR_ID.withValue(String.valueOf(cursorId)));
            if (operationSpan != null) {
                operationSpan.tagLowCardinality(CURSOR_ID.withValue(String.valueOf(cursorId)));
            }
        }

        tagNamespace(span, operationSpan, message, commandName);
        tagServerAndConnectionInfo(span, message);
        tagSessionAndTransactionInfo(span, operationContext);

        return span;
    }

    private void tagNamespace(final Span span, @Nullable final Span parentSpan, final CommandMessage message, final String commandName) {
        String namespace;
        String collection;
        if (parentSpan != null) {
            MongoNamespace parentNamespace = parentSpan.getNamespace();
            if (parentNamespace != null) {
                namespace = parentNamespace.getDatabaseName();
                collection =
                        MongoNamespace.COMMAND_COLLECTION_NAME.equalsIgnoreCase(parentNamespace.getCollectionName()) ? ""
                                : parentNamespace.getCollectionName();
            } else {
                namespace = message.getNamespace().getDatabaseName();
                collection = message.getCollectionName().contains("$cmd") ? "" : message.getCollectionName();
            }
        } else {
            namespace = message.getNamespace().getDatabaseName();
            collection = message.getCollectionName().contains("$cmd") ? "" : message.getCollectionName();
        }
        String summary = commandName + " " + namespace + (collection.isEmpty() ? "" : "." + collection);

        KeyValues keyValues = KeyValues.of(
                SYSTEM.withValue("mongodb"),
                NAMESPACE.withValue(namespace),
                QUERY_SUMMARY.withValue(summary),
                COMMAND_NAME.withValue(commandName));

        if (!collection.isEmpty()) {
            keyValues = keyValues.and(COLLECTION.withValue(collection));
        }
        span.tagLowCardinality(keyValues);
    }

    private void tagServerAndConnectionInfo(final Span span, final CommandMessage message) {
        span.tagLowCardinality(KeyValues.of(
                SERVER_ADDRESS.withValue(serverId.getAddress().getHost()),
                SERVER_PORT.withValue(String.valueOf(serverId.getAddress().getPort())),
                SERVER_TYPE.withValue(message.getSettings().getServerType().name()),
                CLIENT_CONNECTION_ID.withValue(String.valueOf(this.description.getConnectionId().getLocalValue())),
                SERVER_CONNECTION_ID.withValue(String.valueOf(this.description.getConnectionId().getServerValue())),
                NETWORK_TRANSPORT.withValue(getServerAddress() instanceof UnixServerAddress ? "unix" : "tcp")
        ));
    }

    private void tagSessionAndTransactionInfo(final Span span, final OperationContext operationContext) {
        SessionContext sessionContext = operationContext.getSessionContext();
        if (sessionContext.hasSession() && !sessionContext.isImplicitSession()) {
            span.tagLowCardinality(KeyValues.of(
                    TRANSACTION_NUMBER.withValue(String.valueOf(sessionContext.getTransactionNumber())),
                    SESSION_ID.withValue(String.valueOf(sessionContext.getSessionId()
                            .get(sessionContext.getSessionId().getFirstKey())
                            .asBinary().asUuid()))
            ));
        }
    }
}
