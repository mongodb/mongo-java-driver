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
import com.mongodb.WriteConcern;
import com.mongodb.connection.ByteBufferOutputBuffer;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ResponseBuffers;
import com.mongodb.connection.SingleResultCallback;
import com.mongodb.operation.SingleResultFuture;
import com.mongodb.operation.SingleResultFutureCallback;
import com.mongodb.operation.WriteRequest;
import com.mongodb.protocol.message.BaseWriteCommandMessage;
import com.mongodb.protocol.message.MessageSettings;
import com.mongodb.protocol.message.ReplyMessage;
import com.mongodb.protocol.message.RequestMessage;
import org.bson.BsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.mongodb.BulkWriteResult;
import org.mongodb.CommandResult;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;

import static com.mongodb.protocol.ProtocolHelper.getCommandFailureException;
import static com.mongodb.protocol.ProtocolHelper.getMessageSettings;
import static com.mongodb.protocol.WriteCommandResultHelper.getBulkWriteException;
import static com.mongodb.protocol.WriteCommandResultHelper.getBulkWriteResult;
import static com.mongodb.protocol.WriteCommandResultHelper.hasError;
import static java.lang.String.format;

public abstract class WriteCommandProtocol implements Protocol<BulkWriteResult> {
    private final MongoNamespace namespace;
    private final boolean ordered;
    private final WriteConcern writeConcern;

    public WriteCommandProtocol(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern) {
        this.namespace = namespace;
        this.ordered = ordered;
        this.writeConcern = writeConcern;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    public BulkWriteResult execute(final Connection connection) {
        BaseWriteCommandMessage message = createRequestMessage(getMessageSettings(connection.getServerDescription()));
        BulkWriteBatchCombiner bulkWriteBatchCombiner = new BulkWriteBatchCombiner(connection.getServerAddress(), ordered, writeConcern);
        int batchNum = 0;
        int currentRangeStartIndex = 0;
        do {
            batchNum++;
            BaseWriteCommandMessage nextMessage = sendMessage(connection, message, batchNum);
            int itemCount = nextMessage != null ? message.getItemCount() - nextMessage.getItemCount() : message.getItemCount();
            IndexMap indexMap = IndexMap.create(currentRangeStartIndex, itemCount);
            CommandResult commandResult = receiveMessage(connection, message);

            if (nextMessage != null || batchNum > 1) {
                getLogger().debug(format("Received response for batch %d", batchNum));
            }

            if (hasError(commandResult)) {
                bulkWriteBatchCombiner.addErrorResult(getBulkWriteException(getType(), commandResult), indexMap);
            } else {
                bulkWriteBatchCombiner.addResult(getBulkWriteResult(getType(), commandResult), indexMap);
            }
            currentRangeStartIndex += itemCount;
            message = nextMessage;
        } while (message != null && !bulkWriteBatchCombiner.shouldStopSendingMoreBatches());

        return bulkWriteBatchCombiner.getResult();
    }

    @Override
    public MongoFuture<BulkWriteResult> executeAsync(final Connection connection) {
        SingleResultFuture<BulkWriteResult> future = new SingleResultFuture<BulkWriteResult>();
        executeBatchesAsync(connection, createRequestMessage(getMessageSettings(connection.getServerDescription())),
                            new BulkWriteBatchCombiner(connection.getServerAddress(), ordered, writeConcern), 0, 0, future);
        return future;
    }

    private void executeBatchesAsync(final Connection connection, final BaseWriteCommandMessage message,
                                     final BulkWriteBatchCombiner bulkWriteBatchCombiner, final int batchNum,
                                     final int currentRangeStartIndex, final SingleResultFuture<BulkWriteResult> future) {

        if (message != null && !bulkWriteBatchCombiner.shouldStopSendingMoreBatches()) {

            final ByteBufferOutputBuffer buffer = new ByteBufferOutputBuffer(connection);
            final BaseWriteCommandMessage nextMessage = message.encode(buffer);
            final int itemCount = nextMessage != null ? message.getItemCount() - nextMessage.getItemCount() : message.getItemCount();
            final IndexMap indexMap = IndexMap.create(currentRangeStartIndex, itemCount);
            final int nextBatchNum = batchNum + 1;
            final int nextRangeStartIndex = currentRangeStartIndex + itemCount;

            if (nextBatchNum > 1) {
                getLogger().debug(format("Asynchronously sending batch %d", batchNum));
            }

            sendMessageAsync(connection, message.getId(), buffer).register(new SingleResultCallback<CommandResult>() {
                @Override
                public void onResult(final CommandResult result, final MongoException e) {
                    buffer.close();
                    if (e != null) {
                        future.init(null, e);
                    } else {

                        if (nextBatchNum > 1) {
                            getLogger().debug(format("Asynchronously received response for batch %d", batchNum));
                        }

                        if (hasError(result)) {
                            bulkWriteBatchCombiner.addErrorResult(getBulkWriteException(getType(), result), indexMap);
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
        ByteBufferOutputBuffer buffer = new ByteBufferOutputBuffer(connection);
        try {
            BaseWriteCommandMessage nextMessage = message.encode(buffer);
            if (nextMessage != null || batchNum > 1) {
                getLogger().debug(format("Sending batch %d", batchNum));
            }
            connection.sendMessage(buffer.getByteBuffers(), message.getId());
            return nextMessage;
        } finally {
            buffer.close();
        }
    }

    private CommandResult receiveMessage(final Connection connection, final RequestMessage message) {
        ResponseBuffers responseBuffers = connection.receiveMessage(message.getId());
        try {
            ReplyMessage<BsonDocument> replyMessage = new ReplyMessage<BsonDocument>(responseBuffers, new BsonDocumentCodec(),
                                                                                     message.getId());
            CommandResult commandResult = new CommandResult(connection.getServerAddress(), replyMessage.getDocuments().get(0)
            );
            if (!commandResult.isOk()) {
                throw getCommandFailureException(commandResult);
            }

            return commandResult;

        } finally {
            responseBuffers.close();
        }
    }

    private MongoFuture<CommandResult> sendMessageAsync(final Connection connection, final int messageId,
                                                        final ByteBufferOutputBuffer buffer) {
        SingleResultFuture<CommandResult> future = new SingleResultFuture<CommandResult>();

        CommandResultCallback receiveCallback = new CommandResultCallback(new SingleResultFutureCallback<CommandResult>(future),
                                                                          new BsonDocumentCodec(),
                                                                          messageId,
                                                                          connection.getServerAddress());
        connection.sendMessageAsync(buffer.getByteBuffers(), messageId,
                                    new SendMessageCallback<CommandResult>(connection, buffer, messageId, future, receiveCallback));

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
