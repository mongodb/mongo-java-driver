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
import com.mongodb.ServerAddress;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.FieldNameValidator;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;

import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;

/**
 * A protocol for executing a command against a MongoDB server using the OP_QUERY wire protocol message.
 *
 * @param <T> the type returned from execution
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
 */
class CommandProtocol<T> implements Protocol<T> {

    public static final Logger LOGGER = Loggers.getLogger("protocol.command");

    private final MongoNamespace namespace;
    private final BsonDocument command;
    private final Decoder<T> commandResultDecoder;
    private final FieldNameValidator fieldNameValidator;
    private boolean slaveOk;

    /**
     * Construct an instance.
     *  @param database             the database
     * @param command              the command
     * @param fieldNameValidator   the field name validator to apply tot the command
     * @param commandResultDecoder the decoder to use to decode the command result
     */
    public CommandProtocol(final String database, final BsonDocument command, final FieldNameValidator fieldNameValidator,
                           final Decoder<T> commandResultDecoder) {
        notNull("database", database);
        this.namespace = new MongoNamespace(database, MongoNamespace.COMMAND_COLLECTION_NAME);
        this.command = notNull("command", command);
        this.commandResultDecoder = notNull("commandResultDecoder", commandResultDecoder);
        this.fieldNameValidator = notNull("fieldNameValidator", fieldNameValidator);
    }

    public boolean isSlaveOk() {
        return slaveOk;
    }

    public CommandProtocol<T> slaveOk(final boolean slaveOk) {
        this.slaveOk = slaveOk;
        return this;
    }


    @Override
    public T execute(final InternalConnection connection) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Sending command {%s : %s} to database %s on connection [%s] to server %s",
                                command.keySet().iterator().next(), command.values().iterator().next(),
                                namespace.getDatabaseName(), connection.getDescription().getConnectionId(),
                                connection.getDescription().getServerAddress()));
        }
        T retval = receiveMessage(connection, sendMessage(connection).getId());
        LOGGER.debug("Command execution completed");
        return retval;
    }

    @Override
    public void executeAsync(final InternalConnection connection, final SingleResultCallback<T> callback) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Asynchronously sending command {%s : %s} to database %s on connection [%s] to server %s",
                                    command.keySet().iterator().next(), command.values().iterator().next(),
                                    namespace.getDatabaseName(), connection.getDescription().getConnectionId(),
                                    connection.getDescription().getServerAddress()));
            }
            ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
            CommandMessage message = new CommandMessage(namespace.getFullName(), command, slaveOk, fieldNameValidator,
                                                        ProtocolHelper.getMessageSettings(connection.getDescription()));
            ProtocolHelper.encodeMessage(message, bsonOutput);

            SingleResultCallback<ResponseBuffers> receiveCallback = new CommandResultCallback<T>(callback, commandResultDecoder,
                                                                                                 message.getId(),
                                                                                                 connection.getDescription()
                                                                                                           .getServerAddress());
            connection.sendMessageAsync(bsonOutput.getByteBuffers(), message.getId(),
                                        new SendMessageCallback<T>(connection, bsonOutput, message.getId(), callback, receiveCallback));
        } catch (Throwable t) {
            callback.onResult(null, t);
        }
    }

    private CommandMessage sendMessage(final InternalConnection connection) {
        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
        try {
            CommandMessage message = new CommandMessage(namespace.getFullName(), command, slaveOk, fieldNameValidator,
                                                        ProtocolHelper.getMessageSettings(connection.getDescription()));
            message.encode(bsonOutput);
            connection.sendMessage(bsonOutput.getByteBuffers(), message.getId());
            return message;
        } finally {
            bsonOutput.close();
        }
    }

    private T receiveMessage(final InternalConnection connection, final int messageId) {
        ResponseBuffers responseBuffers = connection.receiveMessage(messageId);
        try {
            ReplyMessage<BsonDocument> replyMessage = new ReplyMessage<BsonDocument>(responseBuffers, new BsonDocumentCodec(), messageId);
            return createCommandResult(replyMessage, connection.getDescription().getServerAddress());
        } finally {
            responseBuffers.close();
        }
    }

    private T createCommandResult(final ReplyMessage<BsonDocument> replyMessage, final ServerAddress serverAddress) {
        BsonDocument response = replyMessage.getDocuments().get(0);
        if (!ProtocolHelper.isCommandOk(response)) {
            throw ProtocolHelper.getCommandFailureException(response, serverAddress);
        }

        return commandResultDecoder.decode(new BsonDocumentReader(response), DecoderContext.builder().build());
    }

}
