/*
 * Copyright 2008-2016 MongoDB, Inc.
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
import com.mongodb.async.SingleResultCallback;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.CommandListener;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;

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

    CommandProtocol(final String database, final BsonDocument command, final FieldNameValidator fieldNameValidator,
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
                                getCommandName(), command.values().iterator().next(),
                                namespace.getDatabaseName(), connection.getDescription().getConnectionId(),
                                connection.getDescription().getServerAddress()));
        }
        ResponseBuffers responseBuffers = null;
        try {
            SimpleCommandMessage commandMessage = new SimpleCommandMessage(namespace.getFullName(), command, slaveOk, fieldNameValidator,
                                                                      ProtocolHelper.getMessageSettings(connection.getDescription()));
            responseBuffers = connection.sendAndReceive(commandMessage);
            T retval = getResponseDocument(responseBuffers, commandMessage, commandResultDecoder);
            LOGGER.debug("Command execution completed");
            return retval;
        } finally {
            if (responseBuffers != null) {
                responseBuffers.close();
            }
        }
    }

    @Override
    public void executeAsync(final InternalConnection connection, final SingleResultCallback<T> callback) {
        final SimpleCommandMessage message = new SimpleCommandMessage(namespace.getFullName(), command, slaveOk, fieldNameValidator,
                ProtocolHelper.getMessageSettings(connection.getDescription()));
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Asynchronously sending command {%s : %s} to database %s on connection [%s] to server %s",
                                    getCommandName(), command.values().iterator().next(),
                                    namespace.getDatabaseName(), connection.getDescription().getConnectionId(),
                                    connection.getDescription().getServerAddress()));
            }
            connection.sendAndReceiveAsync(message, new SingleResultCallback<ResponseBuffers>() {
                @Override
                public void onResult(final ResponseBuffers responseBuffers, final Throwable t) {
                    if (t != null) {
                        callback.onResult(null, t);
                    } else {
                        try {
                            T responseDocument = getResponseDocument(responseBuffers, message, commandResultDecoder);
                            callback.onResult(responseDocument, null);
                        } catch (Exception e) {
                            callback.onResult(null, e);
                        }
                    }
                }
            });
        } catch (Throwable t) {
            callback.onResult(null, t);
        }
    }

    @Override
    public void setCommandListener(final CommandListener commandListener) {
    }

    private String getCommandName() {
        return command.keySet().iterator().next();
    }

    private static <D> D getResponseDocument(final ResponseBuffers responseBuffers, final SimpleCommandMessage commandMessage,
                                             final Decoder<D> decoder) {
        responseBuffers.reset();
        ReplyMessage<D> replyMessage = new ReplyMessage<D>(responseBuffers, decoder, commandMessage.getId());

        return replyMessage.getDocuments().get(0);
    }
}
