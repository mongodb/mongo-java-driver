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

import static com.mongodb.connection.ByteBufBsonDocument.createOne;
import static com.mongodb.connection.ProtocolHelper.getCommandFailureException;
import static com.mongodb.connection.ProtocolHelper.getMessageSettings;
import static com.mongodb.connection.ProtocolHelper.sendCommandFailedEvent;
import static com.mongodb.connection.ProtocolHelper.sendCommandStartedEvent;
import static com.mongodb.connection.ProtocolHelper.sendCommandSucceededEvent;
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
    private CommandListener commandListener;

    /**
     * Construct an instance.
     *
     * @param namespace                 the namespace
     * @param ordered                   whether the inserts are ordered
     * @param writeConcern              the write concern
     * @param bypassDocumentValidation  the bypass documentation validation flag
     */
    public WriteCommandProtocol(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                final Boolean bypassDocumentValidation) {
        this.namespace = namespace;
        this.ordered = ordered;
        this.writeConcern = writeConcern;
        this.bypassDocumentValidation = bypassDocumentValidation;
    }

    @Override
    public void setCommandListener(final CommandListener commandListener) {
        this.commandListener = commandListener;
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
        long startTimeNanos = System.nanoTime();
        try {
            BulkWriteBatchCombiner bulkWriteBatchCombiner = new BulkWriteBatchCombiner(connection.getDescription().getServerAddress(),
                                                                                       ordered, writeConcern);
            int batchNum = 0;
            int currentRangeStartIndex = 0;
            do {
                batchNum++;
                startTimeNanos = System.nanoTime();
                BaseWriteCommandMessage nextMessage = sendMessage(connection, message, batchNum);
                int itemCount = nextMessage != null ? message.getItemCount() - nextMessage.getItemCount() : message.getItemCount();
                IndexMap indexMap = IndexMap.create(currentRangeStartIndex, itemCount);
                BsonDocument result = receiveMessage(connection, message);

                if (nextMessage != null || batchNum > 1) {
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug(format("Received response for batch %d", batchNum));
                    }
                }

                if (WriteCommandResultHelper.hasError(result)) {
                    MongoBulkWriteException bulkWriteException = getBulkWriteException(getType(), result,
                                                                                       connection.getDescription().getServerAddress());
                    bulkWriteBatchCombiner.addErrorResult(bulkWriteException, indexMap);
                } else {
                    bulkWriteBatchCombiner.addResult(getBulkWriteResult(getType(), result), indexMap);
                }

                sendSucceededEvent(connection, message, startTimeNanos, result);

                currentRangeStartIndex += itemCount;
                message = nextMessage;
            } while (message != null && !bulkWriteBatchCombiner.shouldStopSendingMoreBatches());

            return bulkWriteBatchCombiner.getResult();
        } catch (MongoBulkWriteException e) {
            throw e;
        } catch (RuntimeException e) {
            sendFailedEvent(connection, message, startTimeNanos, e);
            throw e;
        }
    }

    @Override
    public void executeAsync(final InternalConnection connection, final SingleResultCallback<BulkWriteResult> callback) {
        executeBatchesAsync(connection, createRequestMessage(getMessageSettings(connection.getDescription())),
                            new BulkWriteBatchCombiner(connection.getDescription().getServerAddress(), ordered, writeConcern), 0, 0,
                            callback);
    }

    private void executeBatchesAsync(final InternalConnection connection, final BaseWriteCommandMessage message,
                                     final BulkWriteBatchCombiner bulkWriteBatchCombiner, final int batchNum,
                                     final int currentRangeStartIndex, final SingleResultCallback<BulkWriteResult> callback) {
        final long startTimeNanos = System.nanoTime();
        boolean sentStartedEvent = false;
        try {
            if (message != null && !bulkWriteBatchCombiner.shouldStopSendingMoreBatches()) {
                final ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
                RequestMessage.EncodingMetadata metadata = message.encodeWithMetadata(bsonOutput);

                sendStartedEvent(connection, message, bsonOutput, metadata);
                sentStartedEvent = true;

                final BaseWriteCommandMessage nextMessage = (BaseWriteCommandMessage) metadata.getNextMessage();

                final int itemCount = nextMessage != null ? message.getItemCount() - nextMessage.getItemCount() : message.getItemCount();
                final IndexMap indexMap = IndexMap.create(currentRangeStartIndex, itemCount);
                final int nextBatchNum = batchNum + 1;
                final int nextRangeStartIndex = currentRangeStartIndex + itemCount;

                if (nextBatchNum > 1) {
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug(format("Asynchronously sending batch %d", batchNum));
                    }
                }

                sendMessageAsync(connection, bsonOutput, message, startTimeNanos, callback, new SingleResultCallback<BsonDocument>() {
                    @Override
                    public void onResult(final BsonDocument result, final Throwable t) {
                        bsonOutput.close();
                        if (t != null) {
                            sendFailedEvent(connection, message, startTimeNanos, t);
                            callback.onResult(null, t);
                        } else {
                            sendSucceededEvent(connection, message, startTimeNanos, result);

                            if (nextBatchNum > 1) {
                                if (getLogger().isDebugEnabled()) {
                                    getLogger().debug(format("Asynchronously received response for batch %d", batchNum));
                                }
                            }

                            if (WriteCommandResultHelper.hasError(result)) {
                                bulkWriteBatchCombiner.addErrorResult(getBulkWriteException(getType(), result,
                                        connection.getDescription().getServerAddress()),
                                        indexMap);
                            } else {
                                bulkWriteBatchCombiner.addResult(getBulkWriteResult(getType(), result), indexMap);
                            }

                            executeBatchesAsync(connection, nextMessage, bulkWriteBatchCombiner, nextBatchNum, nextRangeStartIndex,
                                    callback);
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
            if (sentStartedEvent) {
                sendFailedEvent(connection, message, startTimeNanos, t);
            }
            callback.onResult(null, t);
        }
    }

    protected abstract WriteRequest.Type getType();

    protected abstract BaseWriteCommandMessage createRequestMessage(final MessageSettings messageSettings);

    private BaseWriteCommandMessage sendMessage(final InternalConnection connection, final BaseWriteCommandMessage message,
                                                final int batchNum) {
        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
        try {
            RequestMessage.EncodingMetadata encodingMetadata = message.encodeWithMetadata(bsonOutput);
            BaseWriteCommandMessage nextMessage = (BaseWriteCommandMessage) encodingMetadata.getNextMessage();

            sendStartedEvent(connection, message, bsonOutput, encodingMetadata);

            if (nextMessage != null || batchNum > 1) {
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug(format("Sending batch %d", batchNum));
                }
            }
            connection.sendMessage(bsonOutput.getByteBuffers(), message.getId());
            return nextMessage;
        } finally {
            bsonOutput.close();
        }
    }

    private void sendStartedEvent(final InternalConnection connection, final BaseWriteCommandMessage message,
                                  final ByteBufferBsonOutput bsonOutput, final RequestMessage.EncodingMetadata encodingMetadata) {
        if (commandListener != null) {
            sendCommandStartedEvent(message, namespace.getDatabaseName(), message.getCommandName(),
                                    createOne(bsonOutput, encodingMetadata.getFirstDocumentPosition()),
                                    connection.getDescription(), commandListener);
        }
    }

    private void sendSucceededEvent(final InternalConnection connection, final BaseWriteCommandMessage message, final long startTimeNanos,
                                    final BsonDocument result) {
        if (commandListener != null) {
            sendCommandSucceededEvent(message, message.getCommandName(), result, connection.getDescription(),
                    startTimeNanos, commandListener);
        }
    }

    private void sendFailedEvent(final InternalConnection connection, final BaseWriteCommandMessage message, final long startTimeNanos,
                                 final Throwable t) {
        if (commandListener != null) {
            sendCommandFailedEvent(message, message.getCommandName(), connection.getDescription(), startTimeNanos, t, commandListener);
        }
    }

    private BsonDocument receiveMessage(final InternalConnection connection, final RequestMessage message) {
        ResponseBuffers responseBuffers = connection.receiveMessage(message.getId());
        try {
            ReplyMessage<BsonDocument> replyMessage = new ReplyMessage<BsonDocument>(responseBuffers, new BsonDocumentCodec(),
                                                                                     message.getId());
            BsonDocument result = replyMessage.getDocuments().get(0);
            if (!ProtocolHelper.isCommandOk(result)) {
                throw getCommandFailureException(result, connection.getDescription().getServerAddress());
            }

            return result;

        } finally {
            responseBuffers.close();
        }
    }

    private void sendMessageAsync(final InternalConnection connection, final ByteBufferBsonOutput buffer,
                                  final BaseWriteCommandMessage message, final long startTimeNanos,
                                  final SingleResultCallback<BulkWriteResult> clientCallback,
                                  final SingleResultCallback<BsonDocument> callback) {
        SingleResultCallback<ResponseBuffers> receiveCallback = new CommandResultCallback<BsonDocument>(callback,
                                                                                                        new BsonDocumentCodec(),
                                                                                                        message.getId(),
                                                                                                        connection.getDescription()
                                                                                                                  .getServerAddress());
        connection.sendMessageAsync(buffer.getByteBuffers(), message.getId(),
                                    new SendMessageCallback<BulkWriteResult>(connection, buffer, message, message.getCommandName(),
                                            startTimeNanos, commandListener, clientCallback, receiveCallback));
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
