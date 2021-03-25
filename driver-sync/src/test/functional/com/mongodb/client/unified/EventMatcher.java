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

import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.event.ConnectionCheckOutFailedEvent;
import com.mongodb.event.ConnectionCheckOutStartedEvent;
import com.mongodb.event.ConnectionCheckedInEvent;
import com.mongodb.event.ConnectionCheckedOutEvent;
import com.mongodb.event.ConnectionClosedEvent;
import com.mongodb.event.ConnectionCreatedEvent;
import com.mongodb.event.ConnectionPoolClearedEvent;
import com.mongodb.event.ConnectionPoolClosedEvent;
import com.mongodb.event.ConnectionPoolCreatedEvent;
import com.mongodb.event.ConnectionReadyEvent;
import org.bson.BsonArray;
import org.bson.BsonDocument;

import java.util.List;

import static org.junit.Assert.assertEquals;

final class EventMatcher {
    private final ValueMatcher valueMatcher;
    private final AssertionContext context;

    EventMatcher(final ValueMatcher valueMatcher, final AssertionContext context) {
        this.valueMatcher = valueMatcher;
        this.context = context;
    }

    public void assertCommandEventsEquality(final String client, final BsonArray expectedEventDocuments, final List<CommandEvent> events) {
        context.push(ContextElement.ofCommandEvents(client, expectedEventDocuments, events));
        assertEquals(context.getMessage("Number of events must be the same"), expectedEventDocuments.size(), events.size());

        for (int i = 0; i < events.size(); i++) {
            CommandEvent actual = events.get(i);
            BsonDocument expectedEventDocument = expectedEventDocuments.get(i).asDocument();
            String eventType = expectedEventDocument.getFirstKey();
            context.push(ContextElement.ofCommandEvent(expectedEventDocument, actual, i));
            BsonDocument expected = expectedEventDocument.getDocument(eventType);

            if (expected.containsKey("commandName")) {
                assertEquals(context.getMessage("Command names must be equal"),
                        expected.getString("commandName").getValue(), actual.getCommandName());
            }

            if (actual.getClass().equals(CommandStartedEvent.class)) {
                assertEquals(context.getMessage("Expected CommandStartedEvent"), eventType, "commandStartedEvent");
                CommandStartedEvent actualCommandStartedEvent = (CommandStartedEvent) actual;

                if (expected.containsKey("databaseName")) {
                    assertEquals(context.getMessage("Expected database names to match"),
                            expected.getString("databaseName").getValue(), actualCommandStartedEvent.getDatabaseName());
                }
                if (expected.containsKey("command")) {
                    valueMatcher.assertValuesMatch(expected.getDocument("command"), actualCommandStartedEvent.getCommand());
                }
            } else if (actual.getClass().equals(CommandSucceededEvent.class)) {
                assertEquals(context.getMessage("Expected CommandSucceededEvent"), eventType, "commandSucceededEvent");
                CommandSucceededEvent actualCommandSucceededEvent = (CommandSucceededEvent) actual;

                if (expected.containsKey("reply")) {
                    valueMatcher.assertValuesMatch(expected.getDocument("reply"), actualCommandSucceededEvent.getResponse());
                }
            } else if (actual.getClass().equals(CommandFailedEvent.class)) {
                assertEquals(context.getMessage("Expected CommandFailedEvent"), eventType, "commandFailedEvent");
            } else {
                throw new UnsupportedOperationException("Unsupported event type: " + actual.getClass());
            }
            context.pop();
        }
        context.pop();
    }

    public void assertConnectionPoolEventsEquality(final String client, final BsonArray expectedEventDocuments, final List<Object> events) {
        context.push(ContextElement.ofConnectionPoolEvents(client, expectedEventDocuments, events));
        assertEquals(context.getMessage("Number of events must be the same"), expectedEventDocuments.size(), events.size());

        for (int i = 0; i < events.size(); i++) {
            Object actual = events.get(i);
            BsonDocument expectedEventDocument = expectedEventDocuments.get(i).asDocument();
            String eventType = expectedEventDocument.getFirstKey();
            context.push(ContextElement.ofConnectionPoolEvent(expectedEventDocument, actual, i));
            BsonDocument expected = expectedEventDocument.getDocument(eventType);

            if (actual.getClass().equals(ConnectionPoolCreatedEvent.class)) {
                assertEquals(context.getMessage("Expected ConnectionPoolOpenedEvent"), eventType, "poolCreatedEvent");
            } else if (actual.getClass().equals(ConnectionPoolClearedEvent.class)) {
                assertEquals(context.getMessage("Expected ConnectionPoolClearedEvent"), eventType, "poolClearedEvent");
            } else if (actual.getClass().equals(ConnectionPoolClosedEvent.class)) {
                assertEquals(context.getMessage("Expected ConnectionPoolClosedEvent"), eventType, "poolClosedEvent");
            } else if (actual.getClass().equals(ConnectionCheckOutStartedEvent.class)) {
                assertEquals(context.getMessage("Expected ConnectionCheckOutStartedEvent"), eventType, "connectionCheckOutStartedEvent");
            } else if (actual.getClass().equals(ConnectionCheckedOutEvent.class)) {
                assertEquals(context.getMessage("Expected ConnectionCheckedOutEvent"), eventType, "connectionCheckedOutEvent");
            } else if (actual.getClass().equals(ConnectionCheckOutFailedEvent.class)) {
                assertEquals(context.getMessage("Expected ConnectionCheckOutFailedEvent"), eventType, "connectionCheckOutFailedEvent");
                ConnectionCheckOutFailedEvent actualEvent = (ConnectionCheckOutFailedEvent) actual;
                if (expected.containsKey("reason")) {
                    assertEquals(context.getMessage(""), expected.getString("reason").getValue(),
                            getReasonString(actualEvent.getReason()));
                }
            } else if (actual.getClass().equals(ConnectionCheckedInEvent.class)) {
                assertEquals(context.getMessage("Expected ConnectionCheckedInEvent"), eventType, "connectionCheckedInEvent");
            } else if (actual.getClass().equals(ConnectionCreatedEvent.class)) {
                assertEquals(context.getMessage("Expected ConnectionCreatedEvent"), eventType, "connectionCreatedEvent");
            } else if (actual.getClass().equals(ConnectionReadyEvent.class)) {
                assertEquals(context.getMessage("Expected ConnectionReadyEvent"), eventType, "connectionReadyEvent");
            } else if (actual.getClass().equals(ConnectionClosedEvent.class)) {
                assertEquals(context.getMessage("Expected ConnectionClosedEvent"), eventType, "connectionClosedEvent");
                ConnectionClosedEvent actualEvent = (ConnectionClosedEvent) actual;
                if (expected.containsKey("reason")) {
                    assertEquals(context.getMessage(""), expected.getString("reason").getValue(),
                            getReasonString(actualEvent.getReason()));
                }
            } else {
                throw new UnsupportedOperationException("Unsupported event type: " + actual.getClass());
            }
            context.pop();
        }
        context.pop();
    }

    public static String getReasonString(final ConnectionCheckOutFailedEvent.Reason reason) {
        switch (reason) {
            case POOL_CLOSED:
                return "poolClosed";
            case TIMEOUT:
                return "timeout";
            case CONNECTION_ERROR:
                return "connectionError";
            case UNKNOWN:
                return "unknown";
            default:
                throw new UnsupportedOperationException("Unsupported reason: " + reason);
        }
    }

    public static String getReasonString(final ConnectionClosedEvent.Reason reason) {
        switch (reason) {
            case STALE:
                return "stale";
            case IDLE:
                return "idle";
            case ERROR:
                return "error";
            case POOL_CLOSED:
                return "poolClosed";
            default:
                throw new UnsupportedOperationException("Unsupported reason: " + reason);
        }
    }
}
