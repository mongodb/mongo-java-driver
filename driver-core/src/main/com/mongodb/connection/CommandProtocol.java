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

import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.CommandListener;
import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;
import org.bson.codecs.RawBsonDocumentCodec;
import org.bson.io.ByteBufferBsonInput;

import java.util.HashSet;
import java.util.Set;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ByteBufBsonDocument.createOne;
import static com.mongodb.connection.ProtocolHelper.getCommandFailureException;
import static com.mongodb.connection.ProtocolHelper.sendCommandFailedEvent;
import static com.mongodb.connection.ProtocolHelper.sendCommandStartedEvent;
import static com.mongodb.connection.ProtocolHelper.sendCommandSucceededEvent;
import static java.lang.String.format;
import static java.util.Arrays.asList;

/**
 * A protocol for executing a command against a MongoDB server using the OP_QUERY wire protocol message.
 *
 * @param <T> the type returned from execution
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
 */
class CommandProtocol<T> implements Protocol<T> {

    public static final Logger LOGGER = Loggers.getLogger("protocol.command");

    private static final Set<String> SECURITY_SENSITIVE_COMMANDS = new HashSet<String>(asList("authenticate",
                                                                                              "saslStart",
                                                                                              "saslContinue",
                                                                                              "getnonce",
                                                                                              "createUser",
                                                                                              "updateUser",
                                                                                              "copydbgetnonce",
                                                                                              "copydbsaslstart",
                                                                                              "copydb"));
    private final MongoNamespace namespace;
    private final BsonDocument command;
    private final Decoder<T> commandResultDecoder;
    private final FieldNameValidator fieldNameValidator;
    private boolean slaveOk;
    private CommandListener commandListener;
    private volatile String commandName;

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
        long startTimeNanos = System.nanoTime();
        CommandMessage commandMessage = new CommandMessage(namespace.getFullName(), command, slaveOk, fieldNameValidator,
                ProtocolHelper.getMessageSettings(connection.getDescription()));
        ResponseBuffers responseBuffers = null;
        try {
            sendMessage(commandMessage, connection);
            responseBuffers = connection.receiveMessage(commandMessage.getId());
            if (!ProtocolHelper.isCommandOk(new BsonBinaryReader(new ByteBufferBsonInput(responseBuffers.getBodyByteBuffer())))) {
                throw getCommandFailureException(getResponseDocument(responseBuffers, commandMessage, new BsonDocumentCodec()),
                        connection.getDescription().getServerAddress());
            }

            T retval = getResponseDocument(responseBuffers, commandMessage, commandResultDecoder);

            if (commandListener != null) {
                sendSucceededEvent(connection.getDescription(), startTimeNanos, commandMessage,
                        getResponseDocument(responseBuffers, commandMessage, new RawBsonDocumentCodec()));
            }
            LOGGER.debug("Command execution completed");
            return retval;
        } catch (RuntimeException e) {
            sendFailedEvent(connection.getDescription(), startTimeNanos, commandMessage, e);
            throw e;
        } finally {
            if (responseBuffers != null) {
                responseBuffers.close();
            }
        }
    }

    private static <D> D getResponseDocument(final ResponseBuffers responseBuffers, final CommandMessage commandMessage,
                                             final Decoder<D> decoder) {
        responseBuffers.reset();
        ReplyMessage<D> replyMessage = new ReplyMessage<D>(responseBuffers, decoder, commandMessage.getId());

        return replyMessage.getDocuments().get(0);
    }

    @Override
    public void executeAsync(final InternalConnection connection, final SingleResultCallback<T> callback) {
        long startTimeNanos = System.nanoTime();
        CommandMessage message = new CommandMessage(namespace.getFullName(), command, slaveOk, fieldNameValidator,
                ProtocolHelper.getMessageSettings(connection.getDescription()));
        boolean sentStartedEvent = false;
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Asynchronously sending command {%s : %s} to database %s on connection [%s] to server %s",
                                    getCommandName(), command.values().iterator().next(),
                                    namespace.getDatabaseName(), connection.getDescription().getConnectionId(),
                                    connection.getDescription().getServerAddress()));
            }
            ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
            int documentPosition = ProtocolHelper.encodeMessageWithMetadata(message, bsonOutput).getFirstDocumentPosition();
            sendStartedEvent(connection, bsonOutput, message, documentPosition);
            sentStartedEvent = true;
            SingleResultCallback<ResponseBuffers> receiveCallback = new CommandResultCallback(callback, message,
                    connection.getDescription(), startTimeNanos);
            connection.sendMessageAsync(bsonOutput.getByteBuffers(), message.getId(),
                                        new SendMessageCallback<T>(connection, bsonOutput, message, getCommandName(), startTimeNanos,
                                                commandListener, callback, receiveCallback));
        } catch (Throwable t) {
            if (sentStartedEvent) {
                sendFailedEvent(connection.getDescription(), startTimeNanos, message, t);
            }
            callback.onResult(null, t);
        }
    }

    @Override
    public void setCommandListener(final CommandListener commandListener) {
        this.commandListener = commandListener;
    }

    private String getCommandName() {
        return commandName != null ? commandName : command.keySet().iterator().next();
    }

    private void sendMessage(final CommandMessage message, final InternalConnection connection) {
        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
        try {
            int documentPosition = message.encodeWithMetadata(bsonOutput).getFirstDocumentPosition();
            sendStartedEvent(connection, bsonOutput, message, documentPosition);

            connection.sendMessage(bsonOutput.getByteBuffers(), message.getId());
        } finally {
            bsonOutput.close();
        }
    }

    private void sendStartedEvent(final InternalConnection connection, final ByteBufferBsonOutput bsonOutput, final CommandMessage message,
                                  final int documentPosition) {
        if (commandListener != null) {
            ByteBufBsonDocument byteBufBsonDocument = createOne(bsonOutput, documentPosition);
            BsonDocument commandDocument;
            if (byteBufBsonDocument.containsKey("$query")) {
                commandDocument = byteBufBsonDocument.getDocument("$query");
                commandName = commandDocument.keySet().iterator().next();
            } else {
                commandDocument = byteBufBsonDocument;
                commandName = byteBufBsonDocument.getFirstKey();
            }
            BsonDocument commandDocumentForEvent = (SECURITY_SENSITIVE_COMMANDS.contains(commandName))
                                                   ? new BsonDocument() : commandDocument;
            sendCommandStartedEvent(message, namespace.getDatabaseName(), commandName,
                                    commandDocumentForEvent, connection.getDescription(), commandListener);
        }
    }

    private void sendSucceededEvent(final ConnectionDescription connectionDescription, final long startTimeNanos,
                                    final CommandMessage commandMessage, final BsonDocument response) {
        if (commandListener != null) {
            BsonDocument responseDocumentForEvent = (SECURITY_SENSITIVE_COMMANDS.contains(getCommandName()))
                    ? new BsonDocument() : response;
            sendCommandSucceededEvent(commandMessage, getCommandName(), responseDocumentForEvent, connectionDescription,
                    startTimeNanos, commandListener);
        }
    }

    private void sendFailedEvent(final ConnectionDescription connectionDescription, final long startTimeNanos,
                                 final CommandMessage commandMessage, final Throwable t) {
        if (commandListener != null) {
            Throwable commandEventException = t;
            if (t instanceof MongoCommandException && (SECURITY_SENSITIVE_COMMANDS.contains(getCommandName()))) {
                commandEventException = new MongoCommandException(new BsonDocument(), connectionDescription.getServerAddress());
            }
            sendCommandFailedEvent(commandMessage, getCommandName(), connectionDescription, startTimeNanos, commandEventException,
                    commandListener);
        }
    }

    class CommandResultCallback extends ResponseCallback {
        private final SingleResultCallback<T> callback;
        private final CommandMessage message;
        private final ConnectionDescription connectionDescription;
        private final long startTimeNanos;

        CommandResultCallback(final SingleResultCallback<T> callback, final CommandMessage message,
                              final ConnectionDescription connectionDescription, final long startTimeNanos) {
            super(message.getId(), connectionDescription.getServerAddress());
            this.callback = callback;
            this.message = message;
            this.connectionDescription = connectionDescription;
            this.startTimeNanos = startTimeNanos;
        }

        @Override
        protected void callCallback(final ResponseBuffers responseBuffers, final Throwable throwableFromCallback) {
            try {
                if (throwableFromCallback != null) {
                    throw throwableFromCallback;
                }

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Command execution completed");
                }

                if (!ProtocolHelper.isCommandOk(new BsonBinaryReader(new ByteBufferBsonInput(responseBuffers.getBodyByteBuffer())))) {
                    throw getCommandFailureException(getResponseDocument(responseBuffers, message, new BsonDocumentCodec()),
                            connectionDescription.getServerAddress());
                }

                if (commandListener != null) {
                    sendSucceededEvent(connectionDescription, startTimeNanos, message,
                            getResponseDocument(responseBuffers, message, new RawBsonDocumentCodec()));
                }
                callback.onResult(getResponseDocument(responseBuffers, message, commandResultDecoder), null);

            } catch (Throwable t) {
                sendFailedEvent(connectionDescription, startTimeNanos, message, t);
                callback.onResult(null, t);
            } finally {
                if (responseBuffers != null) {
                    responseBuffers.close();
                }
            }


        }
    }
}
