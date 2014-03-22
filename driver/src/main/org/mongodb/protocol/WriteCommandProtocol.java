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

package org.mongodb.protocol;

import org.mongodb.BulkWriteResult;
import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.codecs.EncoderRegistry;
import org.mongodb.codecs.PrimitiveCodecs;
import org.mongodb.codecs.validators.QueryFieldNameValidator;
import org.mongodb.connection.Connection;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerDescription;
import org.mongodb.operation.WriteRequest;
import org.mongodb.protocol.message.BaseWriteCommandMessage;
import org.mongodb.protocol.message.ReplyMessage;
import org.mongodb.protocol.message.RequestMessage;

import java.util.List;

import static java.lang.String.format;
import static org.mongodb.protocol.ProtocolHelper.getCommandFailureException;
import static org.mongodb.protocol.WriteCommandResultHelper.getBulkWriteException;
import static org.mongodb.protocol.WriteCommandResultHelper.getBulkWriteResult;
import static org.mongodb.protocol.WriteCommandResultHelper.hasError;

public abstract class WriteCommandProtocol implements Protocol<BulkWriteResult> {
    private final MongoNamespace namespace;
    private final boolean ordered;
    private final WriteConcern writeConcern;
    private final ServerDescription serverDescription;
    private final Connection connection;
    private final boolean closeConnection;

    public WriteCommandProtocol(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                final ServerDescription serverDescription, final Connection connection, final boolean closeConnection) {
        this.namespace = namespace;
        this.ordered = ordered;
        this.writeConcern = writeConcern;
        this.serverDescription = serverDescription;
        this.connection = connection;
        this.closeConnection = closeConnection;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    public BulkWriteResult execute() {
        try {
            BaseWriteCommandMessage message = createRequestMessage();
            BulkWriteBatchCombiner bulkWriteBatchCombiner = new BulkWriteBatchCombiner(serverDescription.getAddress(), ordered,
                                                                                       writeConcern);
            int batchNum = 0;
            int currentRangeStartIndex = 0;
            do {
                batchNum++;
                BaseWriteCommandMessage nextMessage = sendMessage(message, batchNum);
                int itemCount = nextMessage != null ? message.getItemCount() - nextMessage.getItemCount() : message.getItemCount();
                IndexMap indexMap = IndexMap.create(currentRangeStartIndex, itemCount);
                CommandResult commandResult = receiveMessage(message);

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
        } finally {
            if (closeConnection) {
                connection.close();
            }
        }
    }

    @Override
    public MongoFuture<BulkWriteResult> executeAsync() {
        throw new UnsupportedOperationException();  // TODO!!!!!!!!!!!!!!!!
    }

    protected abstract WriteRequest.Type getType();

    protected abstract BaseWriteCommandMessage createRequestMessage();

    private BaseWriteCommandMessage sendMessage(final BaseWriteCommandMessage message, final int batchNum) {
        PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(connection);
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

    private CommandResult receiveMessage(final RequestMessage message) {
        ResponseBuffers responseBuffers = connection.receiveMessage(message.getId());
        try {
            ReplyMessage<Document> replyMessage = new ReplyMessage<Document>(responseBuffers, new DocumentCodec(), message.getId());
            CommandResult commandResult = new CommandResult(connection.getServerAddress(), replyMessage.getDocuments().get(0),
                                                            replyMessage.getElapsedNanoseconds());
            if (!commandResult.isOk()) {
                throw getCommandFailureException(commandResult);
            }

            return commandResult;

        } finally {
            responseBuffers.close();
        }
    }

    public MongoNamespace getNamespace() {
        return namespace;
    }

    public ServerDescription getServerDescription() {
        return serverDescription;
    }

    public Connection getConnection() {
        return connection;
    }

    protected abstract List<WriteRequest> getRequests();

    protected abstract org.mongodb.diagnostics.logging.Logger getLogger();

    protected boolean isOrdered() {
        return ordered;
    }

    protected static class CommandCodec<T> extends DocumentCodec {
        public CommandCodec(final Encoder<T> encoder) {
            super(PrimitiveCodecs.createDefault(), new QueryFieldNameValidator(), createEncoderRegistry(encoder));
        }

        private static <T> EncoderRegistry createEncoderRegistry(final Encoder<T> encoder) {
            EncoderRegistry encoderRegistry = new EncoderRegistry();
            encoderRegistry.register(encoder.getEncoderClass(), encoder);
            return encoderRegistry;
        }
    }
}
