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

import com.mongodb.LoggerSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.RequestContext;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.event.CommandListener;
import com.mongodb.internal.logging.LogMessage;
import com.mongodb.internal.logging.LogMessage.Entry;
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
import java.util.ArrayDeque;

import java.util.Deque;
import java.util.Set;
import java.util.function.Consumer;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.internal.connection.ProtocolHelper.sendCommandFailedEvent;
import static com.mongodb.internal.connection.ProtocolHelper.sendCommandStartedEvent;
import static com.mongodb.internal.connection.ProtocolHelper.sendCommandSucceededEvent;
import static com.mongodb.internal.logging.LogMessage.Component.COMMAND;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.COMMAND_CONTENT;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.COMMAND_NAME;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.DATABASE_NAME;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.DRIVER_CONNECTION_ID;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.DURATION_MS;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.OPERATION_ID;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.REPLY;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.REQUEST_ID;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.SERVER_CONNECTION_ID;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.SERVER_HOST;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.SERVER_PORT;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.SERVICE_ID;
import static com.mongodb.internal.logging.LogMessage.Level.DEBUG;

class LoggingCommandEventSender implements CommandEventSender {
    private static final double NANOS_PER_MILLI = 1_000_000.0d;
    private final ConnectionDescription description;
    @Nullable
    private final CommandListener commandListener;
    private final RequestContext requestContext;
    private final OperationContext operationContext;
    private final StructuredLogger logger;
    private final LoggerSettings loggerSettings;
    private final long startTimeNanos;
    private final CommandMessage message;
    private final String commandName;
    private volatile BsonDocument commandDocument;
    private final boolean redactionRequired;

    LoggingCommandEventSender(final Set<String> securitySensitiveCommands, final Set<String> securitySensitiveHelloCommands,
            final ConnectionDescription description,
            @Nullable final CommandListener commandListener, final RequestContext requestContext, final OperationContext operationContext,
            final CommandMessage message, final ByteBufferBsonOutput bsonOutput, final StructuredLogger logger,
            final LoggerSettings loggerSettings) {
        this.description = description;
        this.commandListener = commandListener;
        this.requestContext = requestContext;
        this.operationContext = operationContext;
        this.logger = logger;
        this.loggerSettings = loggerSettings;
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
            String messagePrefix = "Command \"{}\" started on database {}";
            String command = redactionRequired ? "{}" : getTruncatedJsonCommand(commandDocument);

            logEvent(messagePrefix, "Command started", entries -> {
                entries.addFirst(new Entry(DATABASE_NAME.getValue(), message.getNamespace().getDatabaseName()));
                entries.addFirst(new Entry(COMMAND_NAME.getValue(), commandName));
                entries.add(new Entry(COMMAND_CONTENT.getValue(), command));
            });
        }

        if (eventRequired()) {
            BsonDocument commandDocumentForEvent = redactionRequired
                    ? new BsonDocument() : commandDocument;

            sendCommandStartedEvent(message, message.getNamespace().getDatabaseName(), commandName, commandDocumentForEvent, description,
                    assertNotNull(commandListener), requestContext, operationContext);
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
            String messagePrefix = "Command \"{}\" failed in {} ms";

            logEvent(messagePrefix, "Command failed", commandEventException, entries -> {
                entries.addFirst(new Entry(DURATION_MS.getValue(), elapsedTimeNanos / NANOS_PER_MILLI));
                entries.addFirst(new Entry(COMMAND_NAME.getValue(), commandName));
                entries.addLast(new Entry(COMMAND_CONTENT.getValue(), null));
            });
        }

        if (eventRequired()) {
            sendCommandFailedEvent(message, commandName, description, elapsedTimeNanos, commandEventException, commandListener,
                    requestContext, operationContext);
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
            String format = "Command \"{}\" succeeded in {} ms using a connection with driver-generated ID {}"
                    + "[ and server-generated ID {}] to {}:{}[ with service ID {}]. The request ID is {}"
                    + " and the operation ID is {}. Command reply: {}";

            BsonDocument responseDocumentForEvent = redactionRequired ? new BsonDocument() : reply;
            String replyString = redactionRequired ? "{}" : getTruncatedJsonCommand(responseDocumentForEvent);

            logMessage("Command succeeded", null, entries -> {
                entries.addFirst(new Entry(DURATION_MS.getValue(), elapsedTimeNanos / NANOS_PER_MILLI));
                entries.addFirst(new Entry(COMMAND_NAME.getValue(), commandName));
                entries.addLast(new Entry(REPLY.getValue(), replyString));
            }, format);
        }

        if (eventRequired()) {
            BsonDocument responseDocumentForEvent = redactionRequired ? new BsonDocument() : reply;
            sendCommandSucceededEvent(message, commandName, responseDocumentForEvent, description,
                    elapsedTimeNanos, commandListener, requestContext, operationContext);
        }
    }

    private boolean loggingRequired() {
        return logger.isRequired(DEBUG, getClusterId());
    }


    private ClusterId getClusterId() {
        return description.getConnectionId().getServerId().getClusterId();
    }

    private boolean eventRequired() {
        return commandListener != null;
    }

    private void logEvent(final String messagePrefix,
            final String messageId,
            final Consumer<Deque<Entry>> mutator) {
        logEvent(messagePrefix, messageId, null, mutator);
    }

    private void logEvent(final String messagePrefix,
            final String messageId,
            @Nullable final Throwable exception, final Consumer<Deque<Entry>> entriesMutator) {
        String format = messagePrefix + " using a connection with driver-generated ID {}"
                + "[ and server-generated ID {}] to {}:{}[ with service ID {}]. The request ID is {}"
                + " and the operation ID is {}.[ Command: {}]";
        logMessage(messageId, exception, entriesMutator, format);
    }

    private void logMessage(final String messageId, final @Nullable Throwable exception, final Consumer<Deque<Entry>> entriesMutator,
            final String format) {
        Deque<Entry> entries = new ArrayDeque<>();
        entries.add(new Entry(DRIVER_CONNECTION_ID.getValue(), description.getConnectionId().getLocalValue()));
        entries.add(new Entry(SERVER_CONNECTION_ID.getValue(), description.getConnectionId().getServerValue()));
        entries.add(new Entry(SERVER_HOST.getValue(), description.getServerAddress().getHost()));
        entries.add(new Entry(SERVER_PORT.getValue(), description.getServerAddress().getPort()));
        entries.add(new Entry(SERVICE_ID.getValue(), description.getServiceId()));
        entries.add(new Entry(REQUEST_ID.getValue(), message.getId()));
        entries.add(new Entry(OPERATION_ID.getValue(), operationContext.getId()));
        entriesMutator.accept(entries);
        logger.log(new LogMessage(COMMAND, DEBUG, messageId, getClusterId(), exception, entries, format));
    }

    private String getTruncatedJsonCommand(final BsonDocument commandDocument) {
        StringWriter writer = new StringWriter();

        try (BsonReader bsonReader = commandDocument.asBsonReader()) {
            JsonWriter jsonWriter = new JsonWriter(writer,
                    JsonWriterSettings.builder().outputMode(JsonMode.RELAXED)
                            .maxLength(loggerSettings.getMaxDocumentLength())
                            .build());

            jsonWriter.pipe(bsonReader);

            if (jsonWriter.isTruncated()) {
                writer.append(" ...");
            }

            return writer.toString();
        }
    }
}
