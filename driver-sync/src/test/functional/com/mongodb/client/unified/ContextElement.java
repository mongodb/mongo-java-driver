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
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import org.bson.BsonArray;
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

    static ContextElement ofOperation(final BsonDocument operation, final OperationResult result) {
        return new OperationContextElement(operation, result);
    }

    static ContextElement ofValueMatcher(final BsonValue expected, final BsonValue actual, final String key, final int arrayPosition) {
        return new ValueMatchingContextElement(expected, actual, key, arrayPosition);
    }

    static ContextElement ofError(final BsonDocument expectedError, final Exception e) {
        return new ErrorMatchingContextElement(expectedError, e);
    }

    static ContextElement ofOutcome(final MongoNamespace namespace, final List<BsonDocument> expectedOutcome,
                                    final List<BsonDocument> actualOutcome) {
        return new OutcomeMatchingContextElement(namespace, expectedOutcome, actualOutcome);
    }

    static ContextElement ofEvents(final String client, final BsonArray expectedEvents, final List<CommandEvent> actualEvents) {
        return new EventsMatchingContextElement(client, expectedEvents, actualEvents);
    }

    static ContextElement ofEvent(final BsonDocument expected, final CommandEvent actual, final int eventPosition) {
        return new EventMatchingContextElement(expected, actual, eventPosition);
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

    private static class OperationContextElement extends ContextElement {
        private final BsonDocument operation;
        private final OperationResult result;

        OperationContextElement(final BsonDocument operation, final OperationResult result) {
            this.operation = operation;
            this.result = result;
        }

        public String toString() {
            return "Operation Result Context: " + "\n"
                    + "   Operation:\n"
                    + operation.toJson(JsonWriterSettings.builder().indent(true).build()) + "\n"
                    + "   Actual result:\n"
                    + result + "\n";
        }
    }

    private static class ValueMatchingContextElement extends ContextElement {
        private final BsonValue expected;
        private final BsonValue actual;
        private final String key;
        private final int arrayPosition;

        ValueMatchingContextElement(final BsonValue expected, final BsonValue actual, final String key, final int arrayPosition) {
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
            builder.append(expected.toString()).append("\n");
            builder.append("   Actual value:\n      ");
            builder.append(actual.toString()).append("\n");
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

    private static class EventsMatchingContextElement extends ContextElement {
        private final String client;
        private final BsonArray expectedEvents;
        private final List<CommandEvent> actualEvents;

        EventsMatchingContextElement(final String client, final BsonArray expectedEvents, final List<CommandEvent> actualEvents) {
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
                    .map(ContextElement::eventToDocument).collect(Collectors.toList())))
                    .toJson(JsonWriterSettings.builder().indent(true).build())
                    + "\n";
        }
    }

    private static class EventMatchingContextElement extends ContextElement {
        private final BsonDocument expectedEvent;
        private final CommandEvent actualEvent;
        private final int eventPosition;

        EventMatchingContextElement(final BsonDocument expectedEvent, final CommandEvent actualEvent, final int eventPosition) {
            this.expectedEvent = expectedEvent;
            this.actualEvent = actualEvent;
            this.eventPosition = eventPosition;
        }

        @Override
        public String toString() {
            return "Event Matching Context\n"
                    + "   event position: " + eventPosition + "\n"
                    + "   expected event: " + expectedEvent + "\n"
                    + "   actual event:   " + eventToDocument(actualEvent) + "\n";
        }
    }

    private static BsonDocument eventToDocument(final CommandEvent event) {
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
}
