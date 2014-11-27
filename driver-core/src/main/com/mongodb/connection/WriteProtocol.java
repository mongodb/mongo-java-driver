/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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
import com.mongodb.WriteConcernResult;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.diagnostics.logging.Logger;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.codecs.BsonDocumentCodec;

import static com.mongodb.MongoNamespace.COMMAND_COLLECTION_NAME;
import static com.mongodb.connection.ProtocolHelper.encodeMessage;
import static com.mongodb.connection.ProtocolHelper.getMessageSettings;
import static java.lang.String.format;

/**
 * Base class for wire protocol messages that perform writes.  In particular, it handles the write followed by the getlasterror command to
 * apply the write concern.
 *
 */
abstract class WriteProtocol implements Protocol<WriteConcernResult> {

    private final MongoNamespace namespace;
    private final boolean ordered;
    private final WriteConcern writeConcern;

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
    public WriteConcernResult execute(final InternalConnection connection) {
        return receiveMessage(connection, sendMessage(connection));
    }

    @Override
    public void executeAsync(final InternalConnection connection, final SingleResultCallback<WriteConcernResult> callback) {
        try {
            ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
            RequestMessage requestMessage = createRequestMessage(getMessageSettings(connection.getDescription()));
            RequestMessage nextMessage = encodeMessage(requestMessage, bsonOutput);
            if (writeConcern.isAcknowledged()) {
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


    private CommandMessage sendMessage(final InternalConnection connection) {
        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
        try {
            RequestMessage lastMessage = createRequestMessage(getMessageSettings(connection.getDescription()));
            RequestMessage nextMessage = lastMessage.encode(bsonOutput);
            int batchNum = 1;
            if (nextMessage != null) {
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug(format("Sending batch %d", batchNum));
                }
            }

            while (nextMessage != null) {
                batchNum++;
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug(format("Sending batch %d", batchNum));
                }
                lastMessage = nextMessage;
                nextMessage = nextMessage.encode(bsonOutput);
            }
            CommandMessage getLastErrorMessage = null;
            if (writeConcern.isAcknowledged()) {
                getLastErrorMessage = new CommandMessage(new MongoNamespace(getNamespace().getDatabaseName(),
                                                                            COMMAND_COLLECTION_NAME).getFullName(),
                                                         createGetLastErrorCommandDocument(), false,
                                                         getMessageSettings(connection.getDescription()));
                getLastErrorMessage.encode(bsonOutput);
                lastMessage = getLastErrorMessage;
            }
            connection.sendMessage(bsonOutput.getByteBuffers(), lastMessage.getId());
            return getLastErrorMessage;
        } finally {
            bsonOutput.close();
        }
    }

    private BsonDocument createGetLastErrorCommandDocument() {
        BsonDocument command = new BsonDocument("getlasterror", new BsonInt32(1));
        command.putAll(writeConcern.asDocument());
        return command;
    }

    private WriteConcernResult receiveMessage(final InternalConnection connection, final RequestMessage requestMessage) {
        if (requestMessage == null) {
            return WriteConcernResult.unacknowledged();
        }
        ResponseBuffers responseBuffers = connection.receiveMessage(requestMessage.getId());
        try {
            ReplyMessage<BsonDocument> replyMessage = new ReplyMessage<BsonDocument>(responseBuffers, new BsonDocumentCodec(),
                                                                                     requestMessage.getId());
            return ProtocolHelper.getWriteResult(replyMessage.getDocuments().get(0), connection.getDescription().getServerAddress());
        } finally {
            responseBuffers.close();
        }
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
