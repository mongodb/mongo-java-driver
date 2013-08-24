/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.operation.protocol;

import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoException;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.codecs.EncoderRegistry;
import org.mongodb.codecs.PrimitiveCodecs;
import org.mongodb.codecs.validators.QueryFieldNameValidator;
import org.mongodb.command.MongoCommandFailureException;
import org.mongodb.command.MongoDuplicateKeyException;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerDescription;

import java.util.Arrays;
import java.util.List;

import static org.mongodb.operation.OperationHelpers.createCommandResult;
import static org.mongodb.operation.OperationHelpers.getResponseSettings;

public abstract class WriteCommandProtocol implements Protocol<CommandResult> {
    private static final List<Integer> DUPLICATE_KEY_ERROR_CODES = Arrays.asList(11000, 11001, 12582);

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

    public CommandResult execute() {
        try {
            return sendAndReceiveAllMessages();
        } finally {
            if (closeConnection) {
                connection.close();
            }
        }
    }

    protected abstract RequestMessage createRequestMessage();

    private CommandResult sendAndReceiveAllMessages() {
        RequestMessage message = createRequestMessage();
        CommandResult commandResult;
        MongoException lastException = null;
        do {
            RequestMessage nextMessage = sendMessage(message);
            commandResult = receiveMessage(message);
            try {
               commandResult = parseWriteCommandResult(commandResult);
            } catch (MongoException e) {
                lastException = e;
                if (!writeConcern.getContinueOnErrorForInsert()) {
                    if (writeConcern.isAcknowledged()) {
                        throw e;
                    }
                    else {
                        break;
                    }
                }
            }
            message = nextMessage;
        } while (message != null);

        if (writeConcern.isAcknowledged() && lastException != null) {
            throw lastException;
        }

        return writeConcern.isAcknowledged() ? commandResult : null;
    }

    private RequestMessage sendMessage(final RequestMessage message) {
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferProvider);
        try {
            final RequestMessage nextMessage = message.encode(buffer);
            connection.sendMessage(buffer.getByteBuffers());
            return nextMessage;
        } finally {
            buffer.close();
        }
    }

    private CommandResult receiveMessage(final RequestMessage message) {
        final ResponseBuffers responseBuffers = connection.receiveMessage(
                getResponseSettings(serverDescription, message.getId()));
        try {
            ReplyMessage<Document> replyMessage = new ReplyMessage<Document>(responseBuffers, new DocumentCodec(), message.getId());
            return createCommandResult(replyMessage, connection);
        } finally {
            responseBuffers.close();
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

    private static CommandResult parseWriteCommandResult(final CommandResult commandResult) {
        MongoCommandFailureException exception = getWriteCommandException(commandResult);
        if (exception != null) {
            throw exception;
        }
        return mungeCommandResult(commandResult, getResults(commandResult).get(0));
    }

    private static CommandResult mungeCommandResult(final CommandResult commandResult, final Document aResult) {
        return new CommandResult(commandResult.getAddress(), aResult, commandResult.getElapsedNanoseconds());
    }

    private static MongoCommandFailureException getWriteCommandException(final CommandResult commandResult) {
        List<Document> results = getResults(commandResult);
        for (Document cur : results) {
            final Integer code = (Integer) cur.get("code");
            if (DUPLICATE_KEY_ERROR_CODES.contains(code)) {
                return new MongoDuplicateKeyException(mungeCommandResult(commandResult, cur));
            }
        }
        return null;
    }


    @SuppressWarnings("unchecked")
    private static List<Document> getResults(final CommandResult commandResult) {
        return (List<Document>) commandResult.getResponse().get("results", List.class);
    }
}
