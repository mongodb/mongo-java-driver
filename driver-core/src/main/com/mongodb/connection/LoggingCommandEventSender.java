/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.connection;

import com.mongodb.MongoCommandException;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.event.CommandListener;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonReader;
import org.bson.codecs.RawBsonDocumentCodec;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;

import java.io.StringWriter;
import java.util.Set;

import static com.mongodb.connection.ProtocolHelper.sendCommandFailedEvent;
import static com.mongodb.connection.ProtocolHelper.sendCommandStartedEvent;
import static com.mongodb.connection.ProtocolHelper.sendCommandSucceededEvent;
import static java.lang.String.format;

class LoggingCommandEventSender implements CommandEventSender {
    private static final int MAX_COMMAND_DOCUMENT_LENGTH_TO_LOG = 1000;

    private final Set<String> securitySensitiveCommands;
    private final ConnectionDescription description;
    private final CommandListener commandListener;
    private final Logger logger;
    private final long startTimeNanos;
    private final CommandMessage message;
    private final String commandName;
    private volatile BsonDocument commandDocument;

    LoggingCommandEventSender(final Set<String> securitySensitiveCommands, final ConnectionDescription description,
                              final CommandListener commandListener, final CommandMessage message,
                              final ByteBufferBsonOutput bsonOutput, final Logger logger) {
        this.securitySensitiveCommands = securitySensitiveCommands;
        this.description = description;
        this.commandListener = commandListener;
        this.logger = logger;
        this.startTimeNanos = System.nanoTime();
        this.message = message;
        this.commandDocument = message.getCommandDocument(bsonOutput);
        this.commandName = commandDocument.getFirstKey();
    }

    @Override
    public void sendStartedEvent() {
        if (loggingRequired()) {
            logger.debug(
                    format("Sending command '%s' with request id %d to database %s on connection [%s] to server %s",
                            getTruncatedJsonCommand(), message.getId(),
                            message.getNamespace().getDatabaseName(), description.getConnectionId(), description.getServerAddress()));
        }

        if (eventRequired()) {
            BsonDocument commandDocumentForEvent = (securitySensitiveCommands.contains(commandName))
                    ? new BsonDocument() : commandDocument;

            sendCommandStartedEvent(message, message.getNamespace().getDatabaseName(),
                    commandName, commandDocumentForEvent, description, commandListener);
        }
        // the buffer underlying the command document may be released after the started event, so set to null to ensure it's not used
        // when sending the failed or succeeded event
        commandDocument = null;
    }

    private String getTruncatedJsonCommand() {
        StringWriter writer = new StringWriter();

        BsonReader bsonReader = commandDocument.asBsonReader();
        try {
            JsonWriter jsonWriter = new JsonWriter(writer,
                    JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).maxLength(MAX_COMMAND_DOCUMENT_LENGTH_TO_LOG).build());

            jsonWriter.pipe(bsonReader);

            if (jsonWriter.isTruncated()) {
                writer.append(" ...");
            }

            return writer.toString();
        } finally {
            bsonReader.close();
        }
    }

    @Override
    public void sendFailedEvent(final Throwable t) {
        Throwable commandEventException = t;
        if (t instanceof MongoCommandException && (securitySensitiveCommands.contains(commandName))) {
            commandEventException = new MongoCommandException(new BsonDocument(), description.getServerAddress());
        }
        long elapsedTimeNanos = System.nanoTime() - startTimeNanos;

        if (loggingRequired()) {
            logger.debug(
                    format("Execution of command with request id %d failed to complete successfully in %s ms on connection [%s] "
                                    + "to server %s",
                            message.getId(), getElapsedTimeFormattedInMilliseconds(elapsedTimeNanos), description.getConnectionId(),
                            description.getServerAddress()),
                    commandEventException);
        }

        if (eventRequired()) {
            sendCommandFailedEvent(message, commandName, description, elapsedTimeNanos, commandEventException, commandListener);
        }
    }

    @Override
    public void sendSucceededEvent(final ResponseBuffers responseBuffers) {
        long elapsedTimeNanos = System.nanoTime() - startTimeNanos;

        if (loggingRequired()) {
            logger.debug(
                    format("Execution of command with request id %d completed successfully in %s ms on connection [%s] to server %s",
                            message.getId(), getElapsedTimeFormattedInMilliseconds(elapsedTimeNanos), description.getConnectionId(),
                            description.getServerAddress()));
        }

        if (eventRequired()) {
            BsonDocument responseDocumentForEvent = (securitySensitiveCommands.contains(commandName))
                    ? new BsonDocument()
                    : responseBuffers.getResponseDocument(message.getId(), new RawBsonDocumentCodec());
            sendCommandSucceededEvent(message, commandName, responseDocumentForEvent, description,
                    elapsedTimeNanos, commandListener);
        }
    }

    @Override
    public void sendSucceededEventForOneWayCommand() {
        long elapsedTimeNanos = System.nanoTime() - startTimeNanos;

        if (loggingRequired()) {
            logger.debug(
                    format("Execution of one-way command with request id %d completed successfully in %s ms on connection [%s] "
                                    + "to server %s",
                            message.getId(), getElapsedTimeFormattedInMilliseconds(elapsedTimeNanos), description.getConnectionId(),
                            description.getServerAddress()));
        }

        if (eventRequired()) {
            BsonDocument responseDocumentForEvent = new BsonDocument("ok", new BsonInt32(1));
            sendCommandSucceededEvent(message, commandName, responseDocumentForEvent, description,
                    elapsedTimeNanos, commandListener);
        }
    }

    private boolean loggingRequired() {
        return logger.isDebugEnabled();
    }

    private boolean eventRequired() {
        return commandListener != null;
    }

    private String getElapsedTimeFormattedInMilliseconds(final long elapsedTimeNanos) {
        return DecimalFormatHelper.format("#0.00", elapsedTimeNanos / 1000000.0);
    }

}
