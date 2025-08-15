/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.client.unified;

import com.mongodb.MongoNamespace;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterOpeningEvent;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.internal.logging.LogMessage;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.json.JsonWriterSettings;

import java.util.List;
import java.util.stream.Collectors;

abstract class ContextElement {
    public static ContextElement ofTest(final BsonDocument definition) {
        return new TestContextContextElement(definition);
    }

    static ContextElement ofStartedOperation(final BsonDocument operation, final int index) {
        return new StartedOperationContextElement(operation, index);
    }

    static ContextElement ofCompletedOperation(final BsonDocument operation, final OperationResult result, final int index) {
        return new CompletedOperationContextElement(operation, result, index);
    }

    static ContextElement ofValueMatcher(final BsonValue expected, @Nullable final BsonValue actual, final String key,
            final int arrayPosition) {
        return new ValueMatchingContextElement(expected, actual, key, arrayPosition);
    }

    static ContextElement ofError(final BsonDocument expectedError, final Exception e) {
        return new ErrorMatchingContextElement(expectedError, e);
    }

    static ContextElement ofOutcome(final MongoNamespace namespace, final List<BsonDocument> expectedOutcome,
                                    final List<BsonDocument> actualOutcome) {
        return new OutcomeMatchingContextElement(namespace, expectedOutcome, actualOutcome);
    }

    static ContextElement ofCommandEvents(final String client, final BsonArray expectedEvents, final List<CommandEvent> actualEvents) {
        return new CommandEventsMatchingContextElement(client, expectedEvents, actualEvents);
    }

    static ContextElement ofCommandEvent(final BsonDocument expected, final CommandEvent actual, final int eventPosition) {
        return new CommandEventMatchingContextElement(expected, actual, eventPosition);
    }

    public static ContextElement ofConnectionPoolEvents(final String client, final BsonArray expectedEvents,
                                                        final List<Object> actualEvents) {
        return new ConnectionPoolEventsMatchingContextElement(client, expectedEvents, actualEvents);
    }

    public static ContextElement ofConnectionPoolEvent(final BsonDocument expected, final Object actual, final int eventPosition) {
        return new ConnectionPoolEventMatchingContextElement(expected, actual, eventPosition);
    }

    public static ContextElement ofWaitForPrimaryChange() {
        return new ContextElement() {
            @Override
            public String toString() {
                return "Wait For Primary Change Context\n";
            }
        };
    }

    public static ContextElement ofWaitForThread(final String threadId) {
        return new ContextElement() {
            @Override
            public String toString() {
                return "Wait For Thread Context:\n"
                        + "   Thread id: " + threadId + "\n";
            }
        };
    }

    public static ContextElement ofTopologyType(final String topologyType) {
        return new ContextElement() {
            @Override
            public String toString() {
                return "Topology Type Context:\n"
                        + "   Topology Type: " + topologyType + "\n";
            }
        };
    }

    public static ContextElement ofWaitForConnectionPoolEvents(final String client, final BsonDocument event, final int count) {
        return new EventCountContext("Wait For Connection Pool Events", client, event, count);
    }

    public static ContextElement ofConnectionPoolEventCount(final String client, final BsonDocument event, final int count) {
        return new EventCountContext("Connection Pool Event Count", client, event, count);
    }

    public static ContextElement ofWaitForServerDescriptionChangedEvents(final String client, final BsonDocument event, final int count) {
        return new EventCountContext("Wait For Server Description Changed Events", client, event, count);
    }

    public static ContextElement ofServerDescriptionChangedEventCount(final String client, final BsonDocument event, final int count) {
        return new EventCountContext("Server Description Changed Event Count", client, event, count);
    }

    public static ContextElement ofWaitForClusterDescriptionChangedEvents(final String client, final BsonDocument event, final int count) {
        return new EventCountContext("Wait For Cluster Description Changed Events", client, event, count);
    }

    public static ContextElement ofClusterDescriptionChangedEventCount(final String client, final BsonDocument event, final int count) {
        return new EventCountContext("Cluster Description Changed Event Count", client, event, count);
    }

    public static ContextElement ofWaitForClusterClosedEvent(final String client) {
        return new ContextElement() {
            @Override
            public String toString() {
                return "Event MatchingContext\n"
                        + "   client: " + client + "\n"
                        + "   expected event: ClusterClosedEvent\n";
            }
        };
    }

    public static ContextElement ofTopologyEvents(final String client, final BsonArray expectedEvents,
            final List<?> actualEvents) {
        return new ContextElement() {
            @Override
            public String toString() {
                return "Events MatchingContext: \n"
                        + "   client: '" + client + "'\n"
                        + "   Expected events:\n"
                        + new BsonDocument("events", expectedEvents).toJson(JsonWriterSettings.builder().indent(true).build()) + "\n"
                        + "   Actual events:\n"
                        + new BsonDocument("events",
                        new BsonArray(actualEvents.stream().map(ContextElement::topologyEventToDocument).collect(Collectors.toList())))
                        .toJson(JsonWriterSettings.builder().indent(true).build())
                        + "\n";
            }
        };
    }

    public static ContextElement ofTopologyEvent(final BsonDocument expected, final Object actual, final int eventPosition) {
        return new ContextElement() {
            @Override
            public String toString() {
                return "Event Matching Context\n"
                        + "   event position: " + eventPosition + "\n"
                        + "   expected event: " + expected + "\n"
                        + "   actual event:   " + topologyEventToDocument(actual) + "\n";
            }
        };
    }

    public static ContextElement ofWaitForServerMonitorEvents(final String client, final BsonDocument event, final int count) {
        return new EventCountContext("Wait For Server Monitor Events", client, event, count);
    }

    public static ContextElement ofServerMonitorEventCount(final String client, final BsonDocument event, final int count) {
        return new EventCountContext("Server Monitor Event Count", client, event, count);
    }

    public static ContextElement ofServerMonitorEvents(final String client, final BsonArray expectedEvents, final List<?> actualEvents) {
        return new ContextElement() {
            @Override
            public String toString() {
                return "Events MatchingContext: \n"
                        + "   client: '" + client + "'\n"
                        + "   Expected events:\n"
                        + new BsonDocument("events", expectedEvents).toJson(JsonWriterSettings.builder().indent(true).build()) + "\n"
                        + "   Actual events:\n"
                        + new BsonDocument("events",
                                new BsonArray(actualEvents.stream().map(ContextElement::serverMonitorEventToDocument).collect(Collectors.toList())))
                                        .toJson(JsonWriterSettings.builder().indent(true).build())
                        + "\n";
            }
        };
    }

    public static ContextElement ofServerMonitorEvent(final BsonDocument expected, final Object actual, final int eventPosition) {
        return new ContextElement() {
            @Override
            public String toString() {
                return "Event Matching Context\n"
                        + "   event position: " + eventPosition + "\n"
                        + "   expected event: " + expected + "\n"
                        + "   actual event:   " + serverMonitorEventToDocument(actual) + "\n";
            }
        };
    }

    private static class EventCountContext extends ContextElement {

        private final String name;
        private final String client;
        private final BsonDocument event;
        private final int count;

        EventCountContext(final String name, final String client, final BsonDocument event, final int count) {
            this.name = name;
            this.client = client;
            this.event = event;
            this.count = count;
        }

        @Override
        public String toString() {
            return name + " Context: " + "\n"
                    + "   Client: " + client + "\n"
                    + "   Event:\n"
                    + event.toJson(JsonWriterSettings.builder().indent(true).build()) + "\n"
                    + "   Count: " + count + "\n";
        }
    }
    public static ContextElement ofLogMessages(final String client, final BsonArray expectedMessages,
            final List<LogMessage> actualMessages) {
        return new LogMessageMatchingContextElement(client, expectedMessages, actualMessages);
    }


    private static class TestContextContextElement extends ContextElement {
        private final BsonDocument definition;

        TestContextContextElement(final BsonDocument definition) {
            this.definition = definition;
        }

        public String toString() {
            return "Test Context: " + "\n"
                    + definition.toJson(JsonWriterSettings.builder().indent(true).build());
        }
    }

    private static class StartedOperationContextElement extends ContextElement {
        private final BsonDocument operation;
        private final int index;

        StartedOperationContextElement(final BsonDocument operation, final int index) {
            this.operation = operation;
            this.index = index;
        }

        public String toString() {
            return "Started Operation Context: " + "\n"
                    + "   Operation:\n"
                    + operation.toJson(JsonWriterSettings.builder().indent(true).build()) + "\n"
                    + "   Operation index: " + index + "\n";
        }
    }

    private static class CompletedOperationContextElement extends ContextElement {
        private final BsonDocument operation;
        private final OperationResult result;
        private final int index;

        CompletedOperationContextElement(final BsonDocument operation, final OperationResult result, final int index) {
            this.operation = operation;
            this.result = result;
            this.index = index;
        }

        public String toString() {
            return "Completed Operation Context: " + "\n"
                    + "   Operation:\n"
                    + operation.toJson(JsonWriterSettings.builder().indent(true).build()) + "\n"
                    + "   Actual result:\n"
                    + result + "\n"
                    + "   Operation index: " + index + "\n";
        }
    }

    private static class ValueMatchingContextElement extends ContextElement {
        private final BsonValue expected;
        private final BsonValue actual;
        private final String key;
        private final int arrayPosition;

        ValueMatchingContextElement(final BsonValue expected, @Nullable final BsonValue actual, final String key, final int arrayPosition) {
            this.expected = expected;
            this.actual = actual;
            this.key = key;
            this.arrayPosition = arrayPosition;
        }

        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("Value Matching Context:\n");
            if (key != null) {
                builder.append("   Key: ").append(key).append("\n");
            }
            if (arrayPosition != -1) {
                builder.append("   Array position: ").append(arrayPosition).append("\n");
            }
            builder.append("   Expected value:\n      ");
            builder.append(expected).append("\n");
            builder.append("   Actual value:\n      ");
            builder.append(actual).append("\n");
            return builder.toString();
        }
    }

    private static class ErrorMatchingContextElement extends ContextElement {
        private final BsonDocument expectedError;
        private final Exception actalError;

        ErrorMatchingContextElement(final BsonDocument expectedError, final Exception actualError) {
            this.expectedError = expectedError;
            this.actalError = actualError;
        }

        public String toString() {
            return "Error Matching Context:\n"
                    + "   Expected error:\n"
                    + expectedError.toJson(JsonWriterSettings.builder().indent(true).build()) + "\n"
                    + "   Actual error:\n"
                    + actalError.toString() + "\n";
        }
    }

    private static class OutcomeMatchingContextElement extends ContextElement {
        private final MongoNamespace namespace;
        private final List<BsonDocument> expectedOutcome;
        private final List<BsonDocument> actualOutcome;

        OutcomeMatchingContextElement(final MongoNamespace namespace, final List<BsonDocument> expectedOutcome,
                                      final List<BsonDocument> actualOutcome) {
            this.namespace = namespace;
            this.expectedOutcome = expectedOutcome;
            this.actualOutcome = actualOutcome;
        }

        public String toString() {
            return "Outcome Matching Context:\n"
                    + "   Namespace: " + namespace + "\n"
                    + "   Expected outcome:\n      "
                    + expectedOutcome + "\n"
                    + "   Actual outcome:\n      "
                    + actualOutcome + "\n";
        }
    }

    private static class CommandEventsMatchingContextElement extends ContextElement {
        private final String client;
        private final BsonArray expectedEvents;
        private final List<CommandEvent> actualEvents;

        CommandEventsMatchingContextElement(final String client, final BsonArray expectedEvents, final List<CommandEvent> actualEvents) {
            this.client = client;
            this.expectedEvents = expectedEvents;
            this.actualEvents = actualEvents;
        }

        @Override
        public String toString() {
            return "Events MatchingContext: \n"
                    + "   client: '" + client + "\n"
                    + "   Expected events:\n"
                    + new BsonDocument("events", expectedEvents).toJson(JsonWriterSettings.builder().indent(true).build()) + "\n"
                    + "   Actual events:\n"
                    + new BsonDocument("events", new BsonArray(actualEvents.stream()
                    .map(ContextElement::commandEventToDocument).collect(Collectors.toList())))
                    .toJson(JsonWriterSettings.builder().indent(true).build())
                    + "\n";
        }
    }

    private static class CommandEventMatchingContextElement extends ContextElement {
        private final BsonDocument expectedEvent;
        private final CommandEvent actualEvent;
        private final int eventPosition;

        CommandEventMatchingContextElement(final BsonDocument expectedEvent, final CommandEvent actualEvent, final int eventPosition) {
            this.expectedEvent = expectedEvent;
            this.actualEvent = actualEvent;
            this.eventPosition = eventPosition;
        }

        @Override
        public String toString() {
            return "Event Matching Context\n"
                    + "   event position: " + eventPosition + "\n"
                    + "   expected event: " + expectedEvent + "\n"
                    + "   actual event:   " + commandEventToDocument(actualEvent) + "\n";
        }
    }

    private static BsonDocument commandEventToDocument(final CommandEvent event) {
        if (event instanceof CommandStartedEvent) {
            CommandStartedEvent commandStartedEvent = (CommandStartedEvent) event;
            return new BsonDocument("commandStartedEvent",
                    new BsonDocument("command", commandStartedEvent.getCommand())
                            .append("databaseName", new BsonString(commandStartedEvent.getDatabaseName())));
        }
        if (event instanceof CommandSucceededEvent) {
            CommandSucceededEvent commandSucceededEvent = (CommandSucceededEvent) event;
            return new BsonDocument("commandSucceededEvent",
                    new BsonDocument("reply", commandSucceededEvent.getResponse())
                            .append("commandName", new BsonString(commandSucceededEvent.getCommandName())));
        } else if (event instanceof CommandFailedEvent) {
            CommandFailedEvent commandFailedEvent = (CommandFailedEvent) event;
            return new BsonDocument("commandFailedEvent",
                    new BsonDocument("commandName", new BsonString(commandFailedEvent.getCommandName())));
        } else {
            throw new UnsupportedOperationException("Unsupported command event: " + event.getClass().getName());
        }
    }

    private static class ConnectionPoolEventsMatchingContextElement extends ContextElement {
        private final String client;
        private final BsonArray expectedEvents;
        private final List<Object> actualEvents;

        ConnectionPoolEventsMatchingContextElement(final String client, final BsonArray expectedEvents, final List<Object> actualEvents) {
            this.client = client;
            this.expectedEvents = expectedEvents;
            this.actualEvents = actualEvents;
        }

        @Override
        public String toString() {
            return "Events MatchingContext: \n"
                    + "   client: '" + client + "\n"
                    + "   Expected events:\n"
                    + new BsonDocument("events", expectedEvents).toJson(JsonWriterSettings.builder().indent(true).build()) + "\n"
                    + "   Actual events:\n"
                    + new BsonDocument("events", new BsonArray(actualEvents.stream()
                    .map(ContextElement::connectionPoolEventToDocument).collect(Collectors.toList())))
                    .toJson(JsonWriterSettings.builder().indent(true).build())
                    + "\n";
        }
    }
    private static class ConnectionPoolEventMatchingContextElement extends ContextElement {
        private final BsonDocument expectedEvent;
        private final Object actualEvent;
        private final int eventPosition;

        ConnectionPoolEventMatchingContextElement(final BsonDocument expectedEvent, final Object actualEvent, final int eventPosition) {
            this.expectedEvent = expectedEvent;
            this.actualEvent = actualEvent;
            this.eventPosition = eventPosition;
        }

        @Override
        public String toString() {
            return "Event Matching Context\n"
                    + "   event position: " + eventPosition + "\n"
                    + "   expected event: " + expectedEvent + "\n"
                    + "   actual event:   " + connectionPoolEventToDocument(actualEvent) + "\n";
        }
    }


    private static class LogMessageMatchingContextElement extends ContextElement {
        private final String client;
        private final BsonArray expectedMessages;
        private final List<LogMessage> actualMessages;

        LogMessageMatchingContextElement(final String client, final BsonArray expectedMessages,
                final List<LogMessage> actualMessages) {
            super();
            this.client = client;
            this.expectedMessages = expectedMessages;
            this.actualMessages = actualMessages;
        }

        @Override
        public String toString() {
            return "Log Message Matching Context\n"
                    + "   client='" + client + '\'' + "\n"
                    + "   expectedMessages="
                    + new BsonDocument("messages", expectedMessages).toJson(JsonWriterSettings.builder().indent(true).build()) + "\n"
                    + "   actualMessages="
                    + new BsonDocument("messages", new BsonArray(actualMessages.stream()
                    .map(LogMatcher::logMessageAsDocument).collect(Collectors.toList())))
                    .toJson(JsonWriterSettings.builder().indent(true).build()) + "\n";
        }
    }

    private static BsonDocument connectionPoolEventToDocument(final Object event) {
        return new BsonDocument(event.getClass().getSimpleName(), new BsonDocument());
    }

    private static BsonDocument serverMonitorEventToDocument(final Object event) {
        return new BsonDocument(EventMatcher.getEventType(event.getClass()),
                new BsonDocument("awaited", BsonBoolean.valueOf(EventMatcher.getAwaitedFromServerMonitorEvent(event))));
    }

    static BsonDocument topologyEventToDocument(final Object event) {
        if (event != null && !(event instanceof ClusterOpeningEvent || event instanceof ClusterDescriptionChangedEvent || event instanceof ClusterClosedEvent)) {
            throw new UnsupportedOperationException("Unsupported topology event: " + event.getClass().getName());
        }
        BsonDocument eventDocument = new BsonDocument();
        if (event instanceof ClusterDescriptionChangedEvent) {
            ClusterDescriptionChangedEvent changedEvent = (ClusterDescriptionChangedEvent) event;
            eventDocument.put("previousDescription",
                    new BsonDocument("type", new BsonString(clusterDescriptionToString(changedEvent.getPreviousDescription()))));
            eventDocument.put("newDescription",
                    new BsonDocument("type", new BsonString(clusterDescriptionToString(changedEvent.getNewDescription()))));
        }
        return new BsonDocument(EventMatcher.getEventType(event.getClass()), eventDocument);
    }

    static String clusterDescriptionToString(final ClusterDescription clusterDescription) {
        switch (clusterDescription.getType()) {
            case STANDALONE:
                return "Single";
            case REPLICA_SET:
                return clusterDescription.getServerDescriptions().stream()
                        .anyMatch(ServerDescription::isPrimary) ? "ReplicaSetWithPrimary" : "ReplicaSetNoPrimary";
            case SHARDED:
                return "Sharded";
            case LOAD_BALANCED:
                return "LoadBalancer";
            case UNKNOWN:
                return "Unknown";
            default:
                throw new UnsupportedOperationException("Unexpected value: " + clusterDescription.getShortDescription());
        }
    }
}
