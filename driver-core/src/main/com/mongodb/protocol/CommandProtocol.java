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
import com.mongodb.connection.ByteBufferBsonOutput;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ResponseBuffers;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.protocol.message.CommandMessage;
import com.mongodb.protocol.message.ReplyMessage;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.FieldNameValidator;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.protocol.ProtocolHelper.encodeMessage;
import static com.mongodb.protocol.ProtocolHelper.getCommandFailureException;
import static com.mongodb.protocol.ProtocolHelper.getMessageSettings;
import static java.lang.String.format;

/**
 * A protocol for executing a command against a MongoDB server using the OP_QUERY wire protocol message.
 *
 * @param <T> the type returned from execution
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
 * @since 3.0
 */
public class CommandProtocol<T> implements Protocol<T> {

    public static final Logger LOGGER = Loggers.getLogger("protocol.command");

    private final MongoNamespace namespace;
    private final BsonDocument command;
    private final Decoder<T> commandResultDecoder;
    private final boolean slaveOk;
    private final FieldNameValidator fieldNameValidator;

    /**
     * Construct an instance.
     *
     * @param database             the database
     * @param command              the command
     * @param slaveOk              if querying of non-primary replica set members is allowed
     * @param fieldNameValidator   the field name validator to apply tot the command
     * @param commandResultDecoder the decoder to use to decode the command result
     */
    public CommandProtocol(final String database, final BsonDocument command, final boolean slaveOk,
                           final FieldNameValidator fieldNameValidator, final Decoder<T> commandResultDecoder) {
        notNull("database", database);
        this.namespace = new MongoNamespace(database, MongoNamespace.COMMAND_COLLECTION_NAME);
        this.command = notNull("command", command);
        this.commandResultDecoder = notNull("commandResultDecoder", commandResultDecoder);
        this.slaveOk = slaveOk;
        this.fieldNameValidator = notNull("fieldNameValidator", fieldNameValidator);
    }

    @Override
    public T execute(final Connection connection) {
        LOGGER.debug(format("Sending command {%s : %s} to database %s on connection [%s] to server %s",
                            command.keySet().iterator().next(), command.values().iterator().next(),
                            namespace.getDatabaseName(), connection.getId(), connection.getServerAddress()));
        T retval = receiveMessage(connection, sendMessage(connection).getId());
        LOGGER.debug("Command execution completed");
        return retval;
    }

    private CommandMessage sendMessage(final Connection connection) {
        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
        try {
            CommandMessage message = new CommandMessage(namespace.getFullName(), command, slaveOk, fieldNameValidator,
                                                        getMessageSettings(connection.getDescription()));
            message.encode(bsonOutput);
            connection.sendMessage(bsonOutput.getByteBuffers(), message.getId());
            return message;
        } finally {
            bsonOutput.close();
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

    @Override
    public MongoFuture<T> executeAsync(final Connection connection) {
        LOGGER.debug(format("Asynchronously sending command {%s : %s} to database %s on connection [%s] to server %s",
                            command.keySet().iterator().next(), command.values().iterator().next(),
                            namespace.getDatabaseName(), connection.getId(), connection.getServerAddress()));
        SingleResultFuture<T> retVal = new SingleResultFuture<T>();

        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
        CommandMessage message = new CommandMessage(namespace.getFullName(), command, slaveOk, fieldNameValidator,
                                                    getMessageSettings(connection.getDescription()));
        encodeMessage(message, bsonOutput);

        CommandResultCallback<T> receiveCallback = new CommandResultCallback<T>(new SingleResultFutureCallback<T>(retVal),
                                                                                commandResultDecoder,
                                                                                message.getId(),
                                                                                connection.getServerAddress());
        connection.sendMessageAsync(bsonOutput.getByteBuffers(),
                                    message.getId(),
                                    new SendMessageCallback<T>(connection, bsonOutput, message.getId(), retVal, receiveCallback));
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
