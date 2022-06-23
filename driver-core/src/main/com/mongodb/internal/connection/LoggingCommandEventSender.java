/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.internal.connection;

import com.mongodb.MongoCommandException;
import com.mongodb.RequestContext;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.event.CommandListener;
import com.mongodb.internal.logging.StructuredLogger;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonReader;
import org.bson.codecs.RawBsonDocumentCodec;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;

import java.io.StringWriter;
import java.util.Set;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.internal.connection.ProtocolHelper.sendCommandFailedEvent;
import static com.mongodb.internal.connection.ProtocolHelper.sendCommandStartedEvent;
import static com.mongodb.internal.connection.ProtocolHelper.sendCommandSucceededEvent;

class LoggingCommandEventSender implements CommandEventSender {
    private static final int MAX_COMMAND_DOCUMENT_LENGTH_TO_LOG = 1000;
    private static final double NANOS_PER_MILLI = 1_000_000.0d;

    private final ConnectionDescription description;
    @Nullable private final CommandListener commandListener;
    private final RequestContext requestContext;
    private final StructuredLogger logger;
    private final long startTimeNanos;
    private final CommandMessage message;
    private final String commandName;
    private volatile BsonDocument commandDocument;
    private final boolean redactionRequired;

    LoggingCommandEventSender(final Set<String> securitySensitiveCommands, final Set<String> securitySensitiveHelloCommands,
            final ConnectionDescription description,
            @Nullable final CommandListener commandListener, final RequestContext requestContext, final CommandMessage message,
            final ByteBufferBsonOutput bsonOutput, final StructuredLogger logger) {
        this.description = description;
        this.commandListener = commandListener;
        this.requestContext = requestContext;
        this.logger = logger;
        this.startTimeNanos = System.nanoTime();
        this.message = message;
        this.commandDocument = message.getCommandDocument(bsonOutput);
        this.commandName = commandDocument.getFirstKey();
        this.redactionRequired = securitySensitiveCommands.contains(commandName)
                || (securitySensitiveHelloCommands.contains(commandName) && commandDocument.containsKey("speculativeAuthenticate"));
    }

    @Override
    public void sendStartedEvent() {
        if (loggingRequired()) {
            String commandString = redactionRequired ? "{}" : getTruncatedJsonCommand(commandDocument);
            if (description.getServiceId() == null) {
                logger.debug("Command started", getClusterId(),
                        "Command \"%s\" started on database %s using a connection with driver-generated ID %d and server-generated ID %d "
                                + "to %s:%s. The request ID is %s. Command: %s",
                        "commandName", commandName,
                        "databaseName", message.getNamespace().getDatabaseName(),
                        "driverConnectionId", description.getConnectionId().getLocalValue(),
                        "serverConnectionId", description.getConnectionId().getServerValue(),
                        "serverHost", description.getServerAddress().getHost(),
                        "serverPort", description.getServerAddress().getPort(),
                        "requestId", message.getId(),
                        "command", commandString);
            } else {
                logger.debug("Command started", getClusterId(),
                        "Command \"%s\" started on database %s using a connection with driver-generated ID %d and server-generated ID %d "
                                + "to %s:%s with service ID %s. The request ID is %s. Command: %s",
                        "commandName", commandName,
                        "databaseName", message.getNamespace().getDatabaseName(),
                        "driverConnectionId", description.getConnectionId().getLocalValue(),
                        "serverConnectionId", description.getConnectionId().getServerValue(),
                        "serverHost", description.getServerAddress().getHost(),
                        "serverPort", description.getServerAddress().getPort(),
                        "serviceId", description.getServiceId(),
                        "requestId", message.getId(),
                        "command", commandString);
            }
        }

        if (eventRequired()) {
            BsonDocument commandDocumentForEvent = redactionRequired
                    ? new BsonDocument() : commandDocument;

            sendCommandStartedEvent(message, message.getNamespace().getDatabaseName(),
                    commandName, commandDocumentForEvent, description, assertNotNull(commandListener), requestContext);
        }
        // the buffer underlying the command document may be released after the started event, so set to null to ensure it's not used
        // when sending the failed or succeeded event
        commandDocument = null;
    }

    @Override
    public void sendFailedEvent(final Throwable t) {
        Throwable commandEventException = t;
        if (t instanceof MongoCommandException && redactionRequired) {
            MongoCommandException originalCommandException = (MongoCommandException) t;
            commandEventException = new MongoCommandException(new BsonDocument(), originalCommandException.getServerAddress());
            commandEventException.setStackTrace(t.getStackTrace());
        }
        long elapsedTimeNanos = System.nanoTime() - startTimeNanos;

        if (loggingRequired()) {
            if (description.getServiceId() == null) {
                logger.debug("Command failed", getClusterId(), commandEventException,
                        "Command \"%s\" failed in %.2f ms using a connection with driver-generated ID %d and server-generated ID "
                                + "%d to %s:%s. The request ID is %d.",
                        "commandName", commandName,
                        "durationMS", elapsedTimeNanos / NANOS_PER_MILLI,
                        "driverConnectionId", description.getConnectionId().getLocalValue(),
                        "serverConnectionId", description.getConnectionId().getServerValue(),
                        "serverHost", description.getServerAddress().getHost(),
                        "serverPort", description.getServerAddress().getPort(),
                        "requestId", message.getId());
            } else {
                logger.debug("Command failed", getClusterId(), commandEventException,
                        "Command \"%s\" failed in %.2f ms using a connection with driver-generated ID %d and server-generated ID "
                                + "%d to %s:%s with service ID %s. The request ID is %d.",
                        "commandName", commandName,
                        "durationMS", elapsedTimeNanos / NANOS_PER_MILLI,
                        "driverConnectionId", description.getConnectionId().getLocalValue(),
                        "serverConnectionId", description.getConnectionId().getServerValue(),
                        "serverHost", description.getServerAddress().getHost(),
                        "serverPort", description.getServerAddress().getPort(),
                        "serviceId", description.getServiceId(),
                        "requestId", message.getId());
            }
        }

        if (eventRequired()) {
            sendCommandFailedEvent(message, commandName, description, elapsedTimeNanos, commandEventException, commandListener,
                    requestContext);
        }
    }

    @Override
    public void sendSucceededEvent(final ResponseBuffers responseBuffers) {
        sendSucceededEvent(responseBuffers.getResponseDocument(message.getId(), new RawBsonDocumentCodec()));
    }

    @Override
    public void sendSucceededEventForOneWayCommand() {
        sendSucceededEvent(new BsonDocument("ok", new BsonInt32(1)));
    }

    private void sendSucceededEvent(final BsonDocument reply) {
        long elapsedTimeNanos = System.nanoTime() - startTimeNanos;

        if (loggingRequired()) {
            BsonDocument responseDocumentForEvent = redactionRequired ? new BsonDocument() : reply;
            String replyString = redactionRequired ? "{}" : getTruncatedJsonCommand(responseDocumentForEvent);
            if (description.getServiceId() == null) {
                logger.debug("Command succeeded", getClusterId(),
                        "Command \"%s\" succeeded in %.2f ms using a connection with driver-generated ID %d and server-generated ID %d to "
                                + "%s:%s. The request ID is %d. Command reply: %s",
                        "commandName", commandName,
                        "durationMS", elapsedTimeNanos / NANOS_PER_MILLI,
                        "driverConnectionId", description.getConnectionId().getLocalValue(),
                        "serverConnectionId", description.getConnectionId().getServerValue(),
                        "serverHost", description.getServerAddress().getHost(),
                        "serverPort", description.getServerAddress().getPort(),
                        "requestId", message.getId(),
                        "reply", replyString);
            } else {
                logger.debug("Command succeeded", getClusterId(),
                        "Command \"%s\" succeeded in %.2f ms using a connection with driver-generated ID %d and server-generated ID %d to "
                                + " %s:%s with service ID %s. The request ID is %d. Command reply: %s",
                        "commandName", commandName,
                        "durationMS", elapsedTimeNanos / NANOS_PER_MILLI,
                        "driverConnectionId", description.getConnectionId().getLocalValue(),
                        "serverConnectionId", description.getConnectionId().getServerValue(),
                        "serverHost", description.getServerAddress().getHost(),
                        "serverPort", description.getServerAddress().getPort(),
                        "serviceId", description.getServiceId(),
                        "requestId", message.getId(),
                        "reply", replyString);
            }
        }

        if (eventRequired()) {
            BsonDocument responseDocumentForEvent = redactionRequired ? new BsonDocument() : reply;
            sendCommandSucceededEvent(message, commandName, responseDocumentForEvent, description,
                    elapsedTimeNanos, commandListener, requestContext);
        }
    }

    private boolean loggingRequired() {
        return logger.isDebugEnabled(getClusterId());
    }


    private ClusterId getClusterId() {
        return description.getConnectionId().getServerId().getClusterId();
    }

    private boolean eventRequired() {
        return commandListener != null;
    }

    private static String getTruncatedJsonCommand(final BsonDocument commandDocument) {
        StringWriter writer = new StringWriter();

        try (BsonReader bsonReader = commandDocument.asBsonReader()) {
            JsonWriter jsonWriter = new JsonWriter(writer,
                    JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).maxLength(MAX_COMMAND_DOCUMENT_LENGTH_TO_LOG).build());

            jsonWriter.pipe(bsonReader);

            if (jsonWriter.isTruncated()) {
                writer.append(" ...");
            }

            return writer.toString();
        }
    }
}
