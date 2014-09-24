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

package com.mongodb.protocol;

import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.connection.ByteBufferBsonOutput;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ResponseBuffers;
import com.mongodb.operation.WriteRequest;
import com.mongodb.protocol.message.BaseWriteCommandMessage;
import com.mongodb.protocol.message.MessageSettings;
import com.mongodb.protocol.message.ReplyMessage;
import com.mongodb.protocol.message.RequestMessage;
import org.bson.BsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.mongodb.BulkWriteResult;

import static com.mongodb.protocol.ProtocolHelper.getCommandFailureException;
import static com.mongodb.protocol.ProtocolHelper.getMessageSettings;
import static com.mongodb.protocol.WriteCommandResultHelper.getBulkWriteException;
import static com.mongodb.protocol.WriteCommandResultHelper.getBulkWriteResult;
import static com.mongodb.protocol.WriteCommandResultHelper.hasError;
import static java.lang.String.format;

/**
 * A base class for implementations of the bulk write commands.
 *
 * @since 3.0
 */
public abstract class WriteCommandProtocol implements Protocol<BulkWriteResult> {
    private final MongoNamespace namespace;
    private final boolean ordered;
    private final WriteConcern writeConcern;

    /**
     * Construct an instance.
     *
     * @param namespace    the namespace
     * @param ordered      whether the inserts are ordered
     * @param writeConcern the write concern
     */
    public WriteCommandProtocol(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern) {
        this.namespace = namespace;
        this.ordered = ordered;
        this.writeConcern = writeConcern;
    }

    /**
     * Gets the write concern.
     *
     * @return the write concern
     */
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    @Override
    public BulkWriteResult execute(final Connection connection) {
        BaseWriteCommandMessage message = createRequestMessage(getMessageSettings(connection.getDescription()));
        BulkWriteBatchCombiner bulkWriteBatchCombiner = new BulkWriteBatchCombiner(connection.getServerAddress(), ordered, writeConcern);
        int batchNum = 0;
        int currentRangeStartIndex = 0;
        do {
            batchNum++;
            BaseWriteCommandMessage nextMessage = sendMessage(connection, message, batchNum);
            int itemCount = nextMessage != null ? message.getItemCount() - nextMessage.getItemCount() : message.getItemCount();
            IndexMap indexMap = IndexMap.create(currentRangeStartIndex, itemCount);
            BsonDocument result = receiveMessage(connection, message);

            if (nextMessage != null || batchNum > 1) {
                getLogger().debug(format("Received response for batch %d", batchNum));
            }

            if (hasError(result)) {
                bulkWriteBatchCombiner.addErrorResult(getBulkWriteException(getType(), result, connection.getServerAddress()), indexMap);
            } else {
                bulkWriteBatchCombiner.addResult(getBulkWriteResult(getType(), result), indexMap);
            }
            currentRangeStartIndex += itemCount;
            message = nextMessage;
        } while (message != null && !bulkWriteBatchCombiner.shouldStopSendingMoreBatches());

        return bulkWriteBatchCombiner.getResult();
    }

    @Override
    public MongoFuture<BulkWriteResult> executeAsync(final Connection connection) {
        SingleResultFuture<BulkWriteResult> future = new SingleResultFuture<BulkWriteResult>();
        executeBatchesAsync(connection, createRequestMessage(getMessageSettings(connection.getDescription())),
                            new BulkWriteBatchCombiner(connection.getServerAddress(), ordered, writeConcern), 0, 0, future);
        return future;
    }

    private void executeBatchesAsync(final Connection connection, final BaseWriteCommandMessage message,
                                     final BulkWriteBatchCombiner bulkWriteBatchCombiner, final int batchNum,
                                     final int currentRangeStartIndex, final SingleResultFuture<BulkWriteResult> future) {

        if (message != null && !bulkWriteBatchCombiner.shouldStopSendingMoreBatches()) {

            final ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
            final BaseWriteCommandMessage nextMessage = message.encode(bsonOutput);
            final int itemCount = nextMessage != null ? message.getItemCount() - nextMessage.getItemCount() : message.getItemCount();
            final IndexMap indexMap = IndexMap.create(currentRangeStartIndex, itemCount);
            final int nextBatchNum = batchNum + 1;
            final int nextRangeStartIndex = currentRangeStartIndex + itemCount;

            if (nextBatchNum > 1) {
                getLogger().debug(format("Asynchronously sending batch %d", batchNum));
            }

            sendMessageAsync(connection, message.getId(), bsonOutput).register(new SingleResultCallback<BsonDocument>() {
                @Override
                public void onResult(final BsonDocument result, final MongoException e) {
                    bsonOutput.close();
                    if (e != null) {
                        future.init(null, e);
                    } else {

                        if (nextBatchNum > 1) {
                            getLogger().debug(format("Asynchronously received response for batch %d", batchNum));
                        }

                        if (hasError(result)) {
                            bulkWriteBatchCombiner.addErrorResult(getBulkWriteException(getType(), result,
                                                                                        connection.getServerAddress()), indexMap);
                        } else {
                            bulkWriteBatchCombiner.addResult(getBulkWriteResult(getType(), result), indexMap);
                        }

                        executeBatchesAsync(connection, nextMessage, bulkWriteBatchCombiner, nextBatchNum, nextRangeStartIndex, future);
                    }
                }
            });
        } else {
            if (bulkWriteBatchCombiner.hasErrors()) {
                future.init(null, bulkWriteBatchCombiner.getError());
            } else {
                future.init(bulkWriteBatchCombiner.getResult(), null);
            }
        }
    }

    protected abstract WriteRequest.Type getType();

    protected abstract BaseWriteCommandMessage createRequestMessage(final MessageSettings messageSettings);

    private BaseWriteCommandMessage sendMessage(final Connection connection, final BaseWriteCommandMessage message, final int batchNum) {
        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
        try {
            BaseWriteCommandMessage nextMessage = message.encode(bsonOutput);
            if (nextMessage != null || batchNum > 1) {
                getLogger().debug(format("Sending batch %d", batchNum));
            }
            connection.sendMessage(bsonOutput.getByteBuffers(), message.getId());
            return nextMessage;
        } finally {
            bsonOutput.close();
        }
    }

    private BsonDocument receiveMessage(final Connection connection, final RequestMessage message) {
        ResponseBuffers responseBuffers = connection.receiveMessage(message.getId());
        try {
            ReplyMessage<BsonDocument> replyMessage = new ReplyMessage<BsonDocument>(responseBuffers, new BsonDocumentCodec(),
                                                                                     message.getId());
            BsonDocument result = replyMessage.getDocuments().get(0);
            if (!ProtocolHelper.isCommandOk(result)) {
                throw getCommandFailureException(result, connection.getServerAddress());
            }

            return result;

        } finally {
            responseBuffers.close();
        }
    }

    private MongoFuture<BsonDocument> sendMessageAsync(final Connection connection, final int messageId,
                                                       final ByteBufferBsonOutput buffer) {
        SingleResultFuture<BsonDocument> future = new SingleResultFuture<BsonDocument>();

        CommandResultCallback<BsonDocument> receiveCallback =
        new CommandResultCallback<BsonDocument>(new SingleResultFutureCallback<BsonDocument>(future), new BsonDocumentCodec(),
                                                messageId, connection.getServerAddress());
        connection.sendMessageAsync(buffer.getByteBuffers(), messageId,
                                    new SendMessageCallback<BsonDocument>(connection, buffer, messageId, future, receiveCallback));

        return future;
    }

    public MongoNamespace getNamespace() {
        return namespace;
    }

    protected abstract com.mongodb.diagnostics.logging.Logger getLogger();

    protected boolean isOrdered() {
        return ordered;
    }
}
