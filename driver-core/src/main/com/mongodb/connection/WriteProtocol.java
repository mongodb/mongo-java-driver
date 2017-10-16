/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

import com.mongodb.MongoInternalException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernException;
import com.mongodb.WriteConcernResult;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.event.CommandListener;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;
import org.bson.io.OutputBuffer;

import static com.mongodb.MongoNamespace.COMMAND_COLLECTION_NAME;
import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.connection.ProtocolHelper.encodeMessageWithMetadata;
import static com.mongodb.connection.ProtocolHelper.getMessageSettings;
import static com.mongodb.connection.ProtocolHelper.getWriteResult;
import static com.mongodb.connection.ProtocolHelper.sendCommandFailedEvent;
import static com.mongodb.connection.ProtocolHelper.sendCommandStartedEvent;
import static com.mongodb.connection.ProtocolHelper.sendCommandSucceededEvent;

/**
 * Base class for legacy wire protocol messages that perform unacknowledged writes.
 */
abstract class WriteProtocol implements LegacyProtocol<WriteConcernResult> {

    private final MongoNamespace namespace;
    private final boolean ordered;
    private final WriteConcern writeConcern;
    private CommandListener commandListener;

    WriteProtocol(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern) {
        isTrue("write concern is unacknowledged", !writeConcern.isAcknowledged());
        this.namespace = namespace;
        this.ordered = ordered;
        this.writeConcern = writeConcern;
    }

    @Override
    public void setCommandListener(final CommandListener commandListener) {
        this.commandListener = commandListener;
    }

    @Override
    public WriteConcernResult execute(final InternalConnection connection) {
        RequestMessage requestMessage = null;
        do {
            long startTimeNanos = System.nanoTime();
            RequestMessage.EncodingMetadata encodingMetadata;
            int messageId;
            boolean sentCommandStartedEvent = false;
            ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
            try {
                if (requestMessage == null) {
                    requestMessage = createRequestMessage(getMessageSettings(connection.getDescription()));
                }
                requestMessage.encode(bsonOutput, NoOpSessionContext.INSTANCE);
                encodingMetadata = requestMessage.getEncodingMetadata();
                sendStartedEvent(connection, requestMessage, requestMessage.getEncodingMetadata(), bsonOutput);
                sentCommandStartedEvent = true;

                messageId = requestMessage.getId();
                if (shouldAcknowledge(requestMessage.getEncodingMetadata().getNextMessage())) {
                    CommandMessage getLastErrorMessage = getLastErrorCommandMessage(connection);
                    getLastErrorMessage.encode(bsonOutput, NoOpSessionContext.INSTANCE);
                    messageId = getLastErrorMessage.getId();
                }

                connection.sendMessage(bsonOutput.getByteBuffers(), messageId);
            } catch (RuntimeException e) {
                sendFailedEvent(connection, requestMessage, sentCommandStartedEvent, e, startTimeNanos);
                throw e;
            } finally {
                bsonOutput.close();
            }

            if (shouldAcknowledge(encodingMetadata.getNextMessage())) {
                ResponseBuffers responseBuffers = null;
                try {
                    responseBuffers = connection.receiveMessage(messageId);
                    ReplyMessage<BsonDocument> replyMessage = new ReplyMessage<BsonDocument>(responseBuffers, new BsonDocumentCodec(),
                            messageId);
                    getWriteResult(replyMessage.getDocuments().get(0), connection.getDescription().getServerAddress());
                } catch (WriteConcernException e) {
                    sendSucceededEvent(connection, requestMessage, startTimeNanos);
                    if (ordered) {
                        break;
                    }
                } catch (RuntimeException e) {
                    sendFailedEvent(connection, requestMessage, true, e, startTimeNanos);
                    throw e;
                } finally {
                    if (responseBuffers != null) {
                        responseBuffers.close();
                    }
                }
            }

            sendSucceededEvent(connection, requestMessage, startTimeNanos);

            requestMessage = encodingMetadata.getNextMessage();
        } while (requestMessage != null);

        return WriteConcernResult.unacknowledged();
    }

    @Override
    public void executeAsync(final InternalConnection connection, final SingleResultCallback<WriteConcernResult> callback) {
        executeAsync(createRequestMessage(getMessageSettings(connection.getDescription())), connection, callback);
    }

    private void executeAsync(final RequestMessage requestMessage, final InternalConnection connection,
                             final SingleResultCallback<WriteConcernResult> callback) {
        long startTimeNanos = System.nanoTime();
        boolean sentCommandStartedEvent = false;
        try {
            ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);

            RequestMessage.EncodingMetadata encodingMetadata = encodeMessageWithMetadata(requestMessage, bsonOutput);
            sendStartedEvent(connection, requestMessage, encodingMetadata, bsonOutput);
            sentCommandStartedEvent = true;

            if (shouldAcknowledge(encodingMetadata.getNextMessage())) {
                CommandMessage getLastErrorMessage = getLastErrorCommandMessage(connection);
                getLastErrorMessage.encode(bsonOutput, NoOpSessionContext.INSTANCE);
                SingleResultCallback<ResponseBuffers> receiveCallback = new WriteResultCallback(callback,
                        new BsonDocumentCodec(),
                        requestMessage,
                        encodingMetadata.getNextMessage(),
                        getLastErrorMessage.getId(),
                        connection, startTimeNanos);
                connection.sendMessageAsync(bsonOutput.getByteBuffers(), getLastErrorMessage.getId(),
                        new SendMessageCallback<WriteConcernResult>(connection,
                                bsonOutput,
                                requestMessage,
                                getLastErrorMessage.getId(),
                                getCommandName(requestMessage),
                                startTimeNanos,
                                commandListener,
                                callback,
                                receiveCallback));
            } else {
                connection.sendMessageAsync(bsonOutput.getByteBuffers(), requestMessage.getId(),
                        new UnacknowledgedWriteResultCallback(callback,
                                requestMessage,
                                encodingMetadata.getNextMessage(),
                                bsonOutput,
                                connection, startTimeNanos));
            }
        } catch (Throwable t) {
            sendFailedEvent(connection, requestMessage, sentCommandStartedEvent, t, startTimeNanos);
            callback.onResult(null, t);
        }
    }

    protected abstract BsonDocument getAsWriteCommand(ByteBufferBsonOutput bsonOutput, int firstDocumentPosition);

    protected BsonDocument getBaseCommandDocument(final String commandName) {
        BsonDocument baseCommandDocument = new BsonDocument(commandName, new BsonString(getNamespace().getCollectionName()))
                .append("ordered", BsonBoolean.valueOf(isOrdered()));
        if (!writeConcern.isServerDefault()) {
            baseCommandDocument.append("writeConcern", writeConcern.asDocument());
        }
        return baseCommandDocument;
    }

    protected String getCommandName(final RequestMessage message) {
        switch (message.getOpCode()) {
            case OP_INSERT:
                return "insert";
            case OP_UPDATE:
                return "update";
            case OP_DELETE:
                return "delete";
            default:
                throw new MongoInternalException("Unexpected op code for write: " + message.getOpCode());
        }
    }


    private CommandMessage getLastErrorCommandMessage(final InternalConnection connection) {
        return new CommandMessage(new MongoNamespace(getNamespace().getDatabaseName(), COMMAND_COLLECTION_NAME),
                createGetLastErrorCommandDocument(), new NoOpFieldNameValidator(), ReadPreference.primary(),
                getMessageSettings(connection.getDescription()));
    }

    private void sendStartedEvent(final InternalConnection connection, final RequestMessage message,
                                  final RequestMessage.EncodingMetadata encodingMetadata, final ByteBufferBsonOutput bsonOutput) {
        if (commandListener != null) {
            sendCommandStartedEvent(message, namespace.getDatabaseName(), getCommandName(message),
                    getAsWriteCommand(bsonOutput, encodingMetadata.getFirstDocumentPosition()),
                    connection.getDescription(), commandListener);
        }
    }

    private void sendSucceededEvent(final InternalConnection connection, final RequestMessage message,
                                    final long startTimeNanos) {
        if (commandListener != null) {
            sendSucceededEvent(connection, message, getResponseDocument(), startTimeNanos);
        }
    }

    private void sendSucceededEvent(final InternalConnection connection, final RequestMessage message,
                                    final BsonDocument responseDocument, final long startTimeNanos) {
        if (commandListener != null) {
            sendCommandSucceededEvent(message, getCommandName(message), responseDocument, connection.getDescription(), startTimeNanos,
                    commandListener);
        }
    }

    private void sendFailedEvent(final InternalConnection connection, final RequestMessage message,
                                 final boolean sentCommandStartedEvent, final Throwable t, final long startTimeNanos) {
        if (commandListener != null && sentCommandStartedEvent) {
            sendCommandFailedEvent(message, getCommandName(message), connection.getDescription(), startTimeNanos, t, commandListener);
        }
    }

    private BsonDocument getResponseDocument() {
        return new BsonDocument("ok", new BsonInt32(1));
    }

    private boolean shouldAcknowledge(final RequestMessage nextMessage) {
        return isOrdered() && nextMessage != null;
    }

    private BsonDocument createGetLastErrorCommandDocument() {
        return new BsonDocument("getlasterror", new BsonInt32(1));
    }

    /**
     * Create the initial request message for the write.
     *
     * @param settings the message settings
     * @return the request message
     */
    protected abstract RequestMessage createRequestMessage(MessageSettings settings);

    /**
     * Gets the namespace.
     *
     * @return the namespace
     */
    protected MongoNamespace getNamespace() {
        return namespace;
    }

    /**
     * Gets whether the writes are ordered.
     *
     * @return true if ordered
     */
    protected boolean isOrdered() {
        return ordered;
    }

    /**
     * Gets the write concern.
     *
     * @return the write concern
     */
    protected WriteConcern getWriteConcern() {
        return writeConcern;
    }

    /**
     * Gets the logger.
     *
     * @return the logger
     */
    protected abstract Logger getLogger();

    private final class WriteResultCallback extends CommandResultBaseCallback<BsonDocument> {
        private final SingleResultCallback<WriteConcernResult> callback;
        private final RequestMessage message;
        private final RequestMessage nextMessage;
        private final InternalConnection connection;
        private final long startTimeNanos;

        WriteResultCallback(final SingleResultCallback<WriteConcernResult> callback, final Decoder<BsonDocument> decoder,
                            final RequestMessage message, final RequestMessage nextMessage, final long requestId,
                            final InternalConnection connection, final long startTimeNanos) {
            super(decoder, requestId, connection.getDescription().getServerAddress());
            this.callback = callback;
            this.message = message;
            this.nextMessage = nextMessage;
            this.connection = connection;
            this.startTimeNanos = startTimeNanos;
        }

        @Override
        protected void callCallback(final BsonDocument result, final Throwable throwableFromCallback) {
            if (throwableFromCallback != null) {
                sendFailedEvent(connection, message, true, throwableFromCallback, startTimeNanos);
                callback.onResult(null, throwableFromCallback);
            } else {
                try {
                    try {
                        WriteConcernResult writeConcernResult = null;
                        boolean shouldWriteNextMessage = true;
                        try {
                            writeConcernResult = getWriteResult(result, connection.getDescription().getServerAddress());
                        } catch (WriteConcernException e) {
                            if (ordered) {
                                shouldWriteNextMessage = false;
                            }
                        }

                        sendSucceededEvent(connection, message, startTimeNanos);

                        if (shouldWriteNextMessage && nextMessage != null) {
                            executeAsync(nextMessage, connection, callback);
                        } else {
                            callback.onResult(writeConcernResult, null);
                        }
                    } catch (WriteConcernException e) {
                        sendSucceededEvent(connection, message, startTimeNanos);
                        throw e;
                    } catch (RuntimeException e) {
                        sendFailedEvent(connection, message, true, e, startTimeNanos);
                        throw e;
                    }
                } catch (Throwable t) {
                    callback.onResult(null, t);
                }
            }
        }
    }

    private final class UnacknowledgedWriteResultCallback implements SingleResultCallback<Void> {
        private final SingleResultCallback<WriteConcernResult> callback;
        private final RequestMessage message;
        private final RequestMessage nextMessage;
        private final OutputBuffer writtenBuffer;
        private final InternalConnection connection;
        private final long startTimeNanos;

        UnacknowledgedWriteResultCallback(final SingleResultCallback<WriteConcernResult> callback,
                                          final RequestMessage message,
                                          final RequestMessage nextMessage,
                                          final OutputBuffer writtenBuffer,
                                          final InternalConnection connection, final long startTimeNanos) {
            this.callback = callback;
            this.message = message;
            this.nextMessage = nextMessage;
            this.connection = connection;
            this.writtenBuffer = writtenBuffer;
            this.startTimeNanos = startTimeNanos;
        }

        @Override
        public void onResult(final Void result, final Throwable t) {
            writtenBuffer.close();
            if (t != null) {
                sendFailedEvent(connection, message, true, t, startTimeNanos);
                callback.onResult(null, t);
            } else {
                sendSucceededEvent(connection, message, startTimeNanos);

                if (nextMessage != null) {
                    executeAsync(nextMessage, connection, callback);
                } else {
                    callback.onResult(WriteConcernResult.unacknowledged(), null);
                }
            }
        }
    }
}
