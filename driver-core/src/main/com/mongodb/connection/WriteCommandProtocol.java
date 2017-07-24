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

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.WriteRequest;
import com.mongodb.event.CommandListener;
import com.mongodb.internal.connection.IndexMap;
import org.bson.BsonDocument;
import org.bson.codecs.BsonDocumentCodec;

import static com.mongodb.connection.ProtocolHelper.getMessageSettings;
import static com.mongodb.connection.WriteCommandResultHelper.getBulkWriteException;
import static com.mongodb.connection.WriteCommandResultHelper.getBulkWriteResult;
import static java.lang.String.format;

/**
 * A base class for implementations of the bulk write commands.
 */
abstract class WriteCommandProtocol implements Protocol<BulkWriteResult> {
    private final MongoNamespace namespace;
    private final boolean ordered;
    private final WriteConcern writeConcern;
    private final Boolean bypassDocumentValidation;

    WriteCommandProtocol(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                         final Boolean bypassDocumentValidation) {
        this.namespace = namespace;
        this.ordered = ordered;
        this.writeConcern = writeConcern;
        this.bypassDocumentValidation = bypassDocumentValidation;
    }

    @Override
    public void setCommandListener(final CommandListener commandListener) {
    }

    /**
     * Gets the write concern.
     *
     * @return the write concern
     */
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    /**
     * Gets the bypass document validation flag
     *
     * @return the bypass document validation flag
     */
    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    @Override
    public BulkWriteResult execute(final InternalConnection connection) {
        BaseWriteCommandMessage message = createRequestMessage(getMessageSettings(connection.getDescription()));
        BulkWriteBatchCombiner bulkWriteBatchCombiner = new BulkWriteBatchCombiner(connection.getDescription().getServerAddress(),
                                                                                          ordered, writeConcern);
        int batchNum = 1;
        int currentRangeStartIndex = 0;
        do {
            BaseWriteCommandMessage nextMessage = null;
            batchNum++;
            if (getLogger().isDebugEnabled()) {
                getLogger().debug(format("Sending batch %d", batchNum));
            }
            ResponseBuffers responseBuffers = connection.sendAndReceive(message);
            BsonDocument result = createResultDocument(message, responseBuffers);
            nextMessage = (BaseWriteCommandMessage) message.getEncodingMetadata().getNextMessage();
            int itemCount = nextMessage != null ? message.getItemCount() - nextMessage.getItemCount() : message.getItemCount();
            IndexMap indexMap = IndexMap.create(currentRangeStartIndex, itemCount);

            if (getLogger().isDebugEnabled()) {
                getLogger().debug(format("Received response for batch %d", batchNum));
            }

            if (WriteCommandResultHelper.hasError(result)) {
                MongoBulkWriteException bulkWriteException = getBulkWriteException(getType(), result,
                        connection.getDescription().getServerAddress());
                bulkWriteBatchCombiner.addErrorResult(bulkWriteException, indexMap);
            } else {
                bulkWriteBatchCombiner.addResult(getBulkWriteResult(getType(), result), indexMap);
            }

            currentRangeStartIndex += itemCount;
            message = nextMessage;
        } while (message != null && !bulkWriteBatchCombiner.shouldStopSendingMoreBatches());

        return bulkWriteBatchCombiner.getResult();
    }

    @Override
    public void executeAsync(final InternalConnection connection, final SingleResultCallback<BulkWriteResult> callback) {
        executeBatchesAsync(connection, createRequestMessage(getMessageSettings(connection.getDescription())),
                            new BulkWriteBatchCombiner(connection.getDescription().getServerAddress(), ordered, writeConcern), 1, 0,
                            callback);
    }

    private void executeBatchesAsync(final InternalConnection connection, final BaseWriteCommandMessage message,
                                     final BulkWriteBatchCombiner bulkWriteBatchCombiner, final int batchNum,
                                     final int currentRangeStartIndex, final SingleResultCallback<BulkWriteResult> callback) {
        try {
            if (message != null && !bulkWriteBatchCombiner.shouldStopSendingMoreBatches()) {

                if (getLogger().isDebugEnabled()) {
                    getLogger().debug(format("Asynchronously sending batch %d", batchNum));
                }

                connection.sendAndReceiveAsync(message, new SingleResultCallback<ResponseBuffers>() {
                    @Override
                    public void onResult(final ResponseBuffers responseBuffers, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else {
                            if (getLogger().isDebugEnabled()) {
                                getLogger().debug(format("Asynchronously received response for batch %d", batchNum));
                            }

                            BaseWriteCommandMessage nextMessage = (BaseWriteCommandMessage) message.getEncodingMetadata().getNextMessage();

                            int itemCount = nextMessage != null
                                                    ? message.getItemCount() - nextMessage.getItemCount() : message.getItemCount();
                            IndexMap indexMap = IndexMap.create(currentRangeStartIndex, itemCount);

                            BsonDocument result = createResultDocument(message, responseBuffers);

                            if (WriteCommandResultHelper.hasError(result)) {
                                bulkWriteBatchCombiner.addErrorResult(getBulkWriteException(getType(), result,
                                        connection.getDescription().getServerAddress()),
                                        indexMap);
                            } else {
                                bulkWriteBatchCombiner.addResult(getBulkWriteResult(getType(), result), indexMap);
                            }

                            executeBatchesAsync(connection, nextMessage, bulkWriteBatchCombiner, batchNum + 1,
                                    currentRangeStartIndex + itemCount, callback);
                        }
                    }
                });
            } else {
                if (bulkWriteBatchCombiner.hasErrors()) {
                    callback.onResult(null, bulkWriteBatchCombiner.getError());
                } else {
                    callback.onResult(bulkWriteBatchCombiner.getResult(), null);
                }
            }
        } catch (Throwable t) {
            callback.onResult(null, t);
        }
    }

    protected abstract WriteRequest.Type getType();

    protected abstract BaseWriteCommandMessage createRequestMessage(MessageSettings messageSettings);

    private BsonDocument createResultDocument(final BaseWriteCommandMessage message, final ResponseBuffers responseBuffers) {
        try {
            return new ReplyMessage<BsonDocument>(responseBuffers, new BsonDocumentCodec(), message.getId()).getDocuments().get(0);
        } finally {
            responseBuffers.close();
        }
    }

    /**
     * Gets the namespace to execute the protocol in.
     *
     * @return the namespace
     */
    public MongoNamespace getNamespace() {
        return namespace;
    }

    /**
     * Gets the logger.
     *
     * @return the logger
     */
    protected abstract com.mongodb.diagnostics.logging.Logger getLogger();

    /**
     * Gets whether the writes must be executed in order.
     *
     * @return true if the writes must be executed in order
     */
    protected boolean isOrdered() {
        return ordered;
    }

}
