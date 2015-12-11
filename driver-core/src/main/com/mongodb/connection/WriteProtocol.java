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

import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernException;
import com.mongodb.WriteConcernResult;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.event.CommandListener;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;

import static com.mongodb.MongoNamespace.COMMAND_COLLECTION_NAME;
import static com.mongodb.connection.ProtocolHelper.encodeMessage;
import static com.mongodb.connection.ProtocolHelper.getMessageSettings;
import static com.mongodb.connection.ProtocolHelper.sendCommandFailedEvent;
import static com.mongodb.connection.ProtocolHelper.sendCommandStartedEvent;
import static com.mongodb.connection.ProtocolHelper.sendCommandSucceededEvent;
import static java.util.Collections.singletonList;

/**
 * Base class for wire protocol messages that perform writes.  In particular, it handles the write followed by the getlasterror command to
 * apply the write concern.
 *
 */
abstract class WriteProtocol implements Protocol<WriteConcernResult> {

    private final MongoNamespace namespace;
    private final boolean ordered;
    private final WriteConcern writeConcern;
    private CommandListener commandListener;

    /**
     * Construct a new instance.
     *
     * @param namespace    the namespace
     * @param ordered      whether the inserts are ordered
     * @param writeConcern the write concern
     */
    public WriteProtocol(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern) {
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
        WriteConcernResult writeConcernResult = null;
        RequestMessage nextMessage = null;
        do {
            long startTimeNanos = System.nanoTime();
            RequestMessage.EncodingMetadata encodingMetadata;
            int messageId;
            boolean sentCommandStartedEvent = false;
            ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
            try {
                if (nextMessage == null) {
                    nextMessage = createRequestMessage(getMessageSettings(connection.getDescription()));
                }
                encodingMetadata = nextMessage.encodeWithMetadata(bsonOutput);

                if (commandListener != null) {
                    sendCommandStartedEvent(nextMessage, namespace.getDatabaseName(), getCommandName(),
                                            getAsWriteCommand(bsonOutput, encodingMetadata.getFirstDocumentPosition()),
                                            connection.getDescription(), commandListener);
                    sentCommandStartedEvent = true;
                }
                messageId = nextMessage.getId();
                if (shouldAcknowledge(encodingMetadata.getNextMessage())) {
                    CommandMessage getLastErrorMessage = new CommandMessage(new MongoNamespace(getNamespace().getDatabaseName(),
                                                                                               COMMAND_COLLECTION_NAME).getFullName(),
                                                                            createGetLastErrorCommandDocument(), false,
                                                                            getMessageSettings(connection.getDescription()));
                    getLastErrorMessage.encode(bsonOutput);
                    messageId = getLastErrorMessage.getId();
                }

                connection.sendMessage(bsonOutput.getByteBuffers(), messageId);
            } catch (RuntimeException e) {
                if (commandListener != null && sentCommandStartedEvent) {
                    sendCommandFailedEvent(nextMessage, getCommandName(), connection.getDescription(), 0, e, commandListener);
                }
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
                    writeConcernResult = ProtocolHelper.getWriteResult(replyMessage.getDocuments().get(0),
                                                                       connection.getDescription().getServerAddress());
                } catch (WriteConcernException e) {
                    if (commandListener != null) {
                        sendCommandSucceededEvent(nextMessage, getCommandName(), getResponseDocument(nextMessage,
                                                                                                     encodingMetadata.getNextMessage(),
                                                                                                     e.getWriteConcernResult(),
                                                                                                     e),
                                                  connection.getDescription(), 0, commandListener);
                    }
                    if (writeConcern.isAcknowledged()) {
                        throw e;
                    } else if (ordered) {
                        break;
                    }
                } catch (RuntimeException e) {
                    if (commandListener != null) {
                        sendCommandFailedEvent(nextMessage, getCommandName(), connection.getDescription(), 0, e, commandListener);
                    }
                    throw e;
                } finally {
                    if (responseBuffers != null) {
                        responseBuffers.close();
                    }
                }
            }

            if (commandListener != null) {
                sendCommandSucceededEvent(nextMessage, getCommandName(), getResponseDocument(nextMessage, encodingMetadata.getNextMessage(),
                                                                                             writeConcernResult, null),
                                          connection.getDescription(), startTimeNanos, commandListener);
            }

            nextMessage = encodingMetadata.getNextMessage();
        } while (nextMessage != null);

        return writeConcern.isAcknowledged() ? writeConcernResult : WriteConcernResult.unacknowledged();
    }

    private BsonDocument getResponseDocument(final RequestMessage curMessage, final RequestMessage nextMessage,
                                             final WriteConcernResult writeConcernResult,
                                             final WriteConcernException writeConcernException) {
        BsonDocument response = new BsonDocument("ok", new BsonInt32(1));
        if (writeConcern.isAcknowledged()) {
            if (writeConcernException == null) {
                appendToWriteCommandResponseDocument(curMessage, nextMessage, writeConcernResult, response);
            } else {
                response.put("n", new BsonInt32(0));
                BsonDocument writeErrorDocument = new BsonDocument("index", new BsonInt32(0))
                                                  .append("code", new BsonInt32(writeConcernException.getErrorCode()));
                if (writeConcernException.getErrorMessage() != null) {
                    writeErrorDocument.append("errmsg", new BsonString(writeConcernException.getErrorMessage()));
                }
                response.put("writeErrors", new BsonArray(singletonList(writeErrorDocument)));
            }
        }
        return response;
    }

    protected abstract void appendToWriteCommandResponseDocument(final RequestMessage curMessage, final RequestMessage nextMessage,
                                                                 final WriteConcernResult writeConcernResult, final BsonDocument response);

    @Override
    public void executeAsync(final InternalConnection connection, final SingleResultCallback<WriteConcernResult> callback) {
        try {
            ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
            RequestMessage requestMessage = createRequestMessage(getMessageSettings(connection.getDescription()));
            RequestMessage nextMessage = encodeMessage(requestMessage, bsonOutput);
            if (shouldAcknowledge(nextMessage)) {
                CommandMessage getLastErrorMessage = new CommandMessage(new MongoNamespace(getNamespace().getDatabaseName(),
                                                                                           COMMAND_COLLECTION_NAME).getFullName(),
                                                                        createGetLastErrorCommandDocument(), false,
                                                                        getMessageSettings(connection.getDescription()));
                encodeMessage(getLastErrorMessage, bsonOutput);
                SingleResultCallback<ResponseBuffers> recieveCallback = new WriteResultCallback(callback,
                                                                                                new BsonDocumentCodec(),
                                                                                                getNamespace(),
                                                                                                nextMessage,
                                                                                                ordered,
                                                                                                writeConcern,
                                                                                                getLastErrorMessage.getId(),
                                                                                                connection);
                connection.sendMessageAsync(bsonOutput.getByteBuffers(), getLastErrorMessage.getId(),
                                            new SendMessageCallback<WriteConcernResult>(connection,
                                                                                        bsonOutput,
                                                                                        getLastErrorMessage.getId(),
                                                                                        callback,
                                                                                        recieveCallback));
            } else {
                connection.sendMessageAsync(bsonOutput.getByteBuffers(), requestMessage.getId(),
                                            new UnacknowledgedWriteResultCallback(callback,
                                                                                  getNamespace(),
                                                                                  nextMessage,
                                                                                  ordered,
                                                                                  bsonOutput,
                                                                                  connection));
            }
        } catch (Throwable t) {
            callback.onResult(null, t);
        }
    }


    protected abstract BsonDocument getAsWriteCommand(ByteBufferBsonOutput bsonOutput, int firstDocumentPosition);

    protected BsonDocument getBaseCommandDocument() {
        BsonDocument baseCommandDocument = new BsonDocument(getCommandName(), new BsonString(getNamespace().getCollectionName()))
                               .append("ordered", BsonBoolean.valueOf(isOrdered()));
        if (!writeConcern.isServerDefault()) {
            baseCommandDocument.append("writeConcern", writeConcern.asDocument());
        }
        return baseCommandDocument;
    }

    protected abstract String getCommandName();


    private boolean shouldAcknowledge(final RequestMessage nextMessage) {
        return writeConcern.isAcknowledged() || (isOrdered() && nextMessage != null);
    }

    private BsonDocument createGetLastErrorCommandDocument() {
        BsonDocument command = new BsonDocument("getlasterror", new BsonInt32(1));
        command.putAll(writeConcern.asDocument());
        return command;
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
}
