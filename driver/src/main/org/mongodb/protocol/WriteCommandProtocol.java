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

import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoException;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.WriteResult;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.codecs.EncoderRegistry;
import org.mongodb.codecs.PrimitiveCodecs;
import org.mongodb.codecs.validators.QueryFieldNameValidator;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerDescription;
import org.mongodb.protocol.message.BaseWriteCommandMessage;
import org.mongodb.protocol.message.ReplyMessage;
import org.mongodb.protocol.message.RequestMessage;

import java.util.logging.Logger;

import static java.lang.String.format;

public abstract class WriteCommandProtocol implements Protocol<WriteResult> {
    private final MongoNamespace namespace;
    private final BufferProvider bufferProvider;
    private final WriteConcern writeConcern;
    private final ServerDescription serverDescription;
    private final Connection connection;
    private final boolean closeConnection;

    public WriteCommandProtocol(final MongoNamespace namespace, final WriteConcern writeConcern, final BufferProvider bufferProvider,
                                final ServerDescription serverDescription, final Connection connection, final boolean closeConnection) {
        this.namespace = namespace;
        this.bufferProvider = bufferProvider;
        this.writeConcern = writeConcern;
        this.serverDescription = serverDescription;
        this.connection = connection;
        this.closeConnection = closeConnection;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    public WriteResult execute() {
        try {
            return sendAndReceiveAllMessages();
        } finally {
            if (closeConnection) {
                connection.close();
            }
        }
    }

    @Override
    public MongoFuture<WriteResult> executeAsync() {
        throw new UnsupportedOperationException();  // TODO!!!!!!!!!!!!!!!!
    }

    protected abstract BaseWriteCommandMessage createRequestMessage();

    private WriteResult sendAndReceiveAllMessages() {
        BaseWriteCommandMessage message = createRequestMessage();
        WriteResult writeResult = null;
        MongoException lastException = null;
        int batchNum = 0;
        do {
            batchNum++;
            BaseWriteCommandMessage nextMessage = sendMessage(message, batchNum);
            try {
                writeResult = receiveMessage(message);
                if (nextMessage != null || batchNum > 1) {
                    getLogger().fine(format("Received response for batch %d", batchNum));
                }
            } catch (MongoException e) {
                lastException = e;
                if (!writeConcern.getContinueOnError()) {
                    if (writeConcern.isAcknowledged()) {
                        throw e;
                    } else {
                        break;
                    }
                }
            }
            message = nextMessage;
        } while (message != null);

        if (writeConcern.isAcknowledged() && lastException != null) {
            throw lastException;
        }

        return writeConcern.isAcknowledged() ? writeResult : null;
    }

    private BaseWriteCommandMessage sendMessage(final BaseWriteCommandMessage message, final int batchNum) {
        PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferProvider);
        try {
            BaseWriteCommandMessage nextMessage = message.encode(buffer);
            if (nextMessage != null || batchNum > 1) {
                getLogger().fine(format("Sending batch %d", batchNum));
            }
            connection.sendMessage(buffer.getByteBuffers(), message.getId());
            return nextMessage;
        } finally {
            buffer.close();
        }
    }

    private WriteResult receiveMessage(final RequestMessage message) {
        ResponseBuffers responseBuffers = connection.receiveMessage(message.getId());
        try {
            ReplyMessage<Document> replyMessage = new ReplyMessage<Document>(responseBuffers, new DocumentCodec(), message.getId());
            WriteResult result = new WriteResult(new CommandResult(connection.getServerAddress(),
                                                                   replyMessage.getDocuments().get(0),
                                                                   replyMessage.getElapsedNanoseconds()), writeConcern);
            throwIfWriteException(result);
            return result;
        } finally {
            responseBuffers.close();
        }
    }

    private void throwIfWriteException(final WriteResult result) {
        MongoException exception = ProtocolHelper.getWriteException(result);
        if (exception != null) {
            throw exception;
        }
    }

    public MongoNamespace getNamespace() {
        return namespace;
    }

    public BufferProvider getBufferProvider() {
        return bufferProvider;
    }

    public ServerDescription getServerDescription() {
        return serverDescription;
    }

    public Connection getConnection() {
        return connection;
    }

    protected abstract Logger getLogger();

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
