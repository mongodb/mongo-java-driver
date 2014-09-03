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

import com.mongodb.MongoNamespace;
import com.mongodb.ServerAddress;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.connection.ByteBufferOutputBuffer;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ResponseBuffers;
import com.mongodb.diagnostics.Loggers;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.operation.QueryFlag;
import com.mongodb.protocol.message.CommandMessage;
import com.mongodb.protocol.message.ReplyMessage;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.FieldNameValidator;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;

import java.util.EnumSet;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.protocol.ProtocolHelper.encodeMessageToBuffer;
import static com.mongodb.protocol.ProtocolHelper.getCommandFailureException;
import static com.mongodb.protocol.ProtocolHelper.getMessageSettings;
import static java.lang.String.format;

/**
 * A protocol for executing a command against a MongoDB server.
 *
 * @param <T> the type returned from execution
 * @since 3.0
 */
public class CommandProtocol<T> implements Protocol<T> {

    public static final Logger LOGGER = Loggers.getLogger("protocol.command");

    private final MongoNamespace namespace;
    private final BsonDocument command;
    private final Decoder<T> commandResultDecoder;
    private final EnumSet<QueryFlag> queryFlags;
    private final FieldNameValidator fieldNameValidator;

    public CommandProtocol(final String database, final BsonDocument command, final EnumSet<QueryFlag> queryFlags,
                           final FieldNameValidator fieldNameValidator, final Decoder<T> commandResultDecoder) {
        notNull("database", database);
        this.namespace = new MongoNamespace(database, MongoNamespace.COMMAND_COLLECTION_NAME);
        this.command = notNull("command", command);
        this.commandResultDecoder = notNull("commandResultDecoder", commandResultDecoder);
        this.queryFlags = notNull("queryFlags", queryFlags);
        this.fieldNameValidator = notNull("fieldNameValidator", fieldNameValidator);
    }

    public T execute(final Connection connection) {
        LOGGER.debug(format("Sending command {%s : %s} to database %s on connection [%s] to server %s",
                            command.keySet().iterator().next(), command.values().iterator().next(),
                            namespace.getDatabaseName(), connection.getId(), connection.getServerAddress()));
        T retval = receiveMessage(connection, sendMessage(connection).getId());
        LOGGER.debug("Command execution completed");
        return retval;
    }

    private CommandMessage sendMessage(final Connection connection) {
        ByteBufferOutputBuffer buffer = new ByteBufferOutputBuffer(connection);
        try {
            CommandMessage message = new CommandMessage(namespace.getFullName(), command, queryFlags, fieldNameValidator,
                                                        getMessageSettings(connection.getServerDescription()));
            message.encode(buffer);
            connection.sendMessage(buffer.getByteBuffers(), message.getId());
            return message;
        } finally {
            buffer.close();
        }
    }

    private T receiveMessage(final Connection connection, final int messageId) {
        ResponseBuffers responseBuffers = connection.receiveMessage(messageId);
        try {
            ReplyMessage<BsonDocument> replyMessage = new ReplyMessage<BsonDocument>(responseBuffers, new BsonDocumentCodec(), messageId);
            return createCommandResult(replyMessage, connection.getServerAddress());
        } finally {
            responseBuffers.close();
        }
    }

    public MongoFuture<T> executeAsync(final Connection connection) {
        LOGGER.debug(format("Asynchronously sending command {%s : %s} to database %s on connection [%s] to server %s",
                            command.keySet().iterator().next(), command.values().iterator().next(),
                            namespace.getDatabaseName(), connection.getId(), connection.getServerAddress()));
        SingleResultFuture<T> retVal = new SingleResultFuture<T>();

        ByteBufferOutputBuffer buffer = new ByteBufferOutputBuffer(connection);
        CommandMessage message = new CommandMessage(namespace.getFullName(), command, queryFlags, fieldNameValidator,
                                                    getMessageSettings(connection.getServerDescription()));
        encodeMessageToBuffer(message, buffer);

        CommandResultCallback<T> receiveCallback = new CommandResultCallback<T>(new SingleResultFutureCallback<T>(retVal),
                                                                                commandResultDecoder,
                                                                                message.getId(),
                                                                                connection.getServerAddress());
        connection.sendMessageAsync(buffer.getByteBuffers(),
                                    message.getId(),
                                    new SendMessageCallback<T>(connection, buffer, message.getId(), retVal, receiveCallback));
        return retVal;
    }

    private T createCommandResult(final ReplyMessage<BsonDocument> replyMessage, final ServerAddress serverAddress) {
        BsonDocument response = replyMessage.getDocuments().get(0);
        if (!ProtocolHelper.isCommandOk(response)) {
            throw getCommandFailureException(response, serverAddress);
        }

        return commandResultDecoder.decode(new BsonDocumentReader(response), DecoderContext.builder().build());
    }
}
