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

import com.mongodb.assertions.Assertions;
import com.mongodb.connection.ServerType;
import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterOpeningEvent;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.event.ConnectionCheckOutFailedEvent;
import com.mongodb.event.ConnectionClosedEvent;
import com.mongodb.event.ConnectionCreatedEvent;
import com.mongodb.event.ConnectionPoolClearedEvent;
import com.mongodb.event.ConnectionPoolReadyEvent;
import com.mongodb.event.ConnectionReadyEvent;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.event.ServerHeartbeatFailedEvent;
import com.mongodb.event.ServerHeartbeatStartedEvent;
import com.mongodb.event.ServerHeartbeatSucceededEvent;
import com.mongodb.event.TestServerMonitorListener;
import com.mongodb.internal.connection.TestClusterListener;
import com.mongodb.internal.connection.TestConnectionPoolListener;
import com.mongodb.internal.connection.TestServerListener;
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.mongodb.client.unified.ContextElement.clusterDescriptionToString;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

final class EventMatcher {
    private final ValueMatcher valueMatcher;
    private final AssertionContext context;

    EventMatcher(final ValueMatcher valueMatcher, final AssertionContext context) {
        this.valueMatcher = valueMatcher;
        this.context = context;
    }

    public void assertCommandEventsEquality(final String client, final boolean ignoreExtraEvents, final BsonArray expectedEventDocuments,
                                            final List<CommandEvent> events) {
        context.push(ContextElement.ofCommandEvents(client, expectedEventDocuments, events));
        if (ignoreExtraEvents) {
            assertTrue(context.getMessage("Number of events must be greater than or equal to the expected number of events"),
                    events.size() >= expectedEventDocuments.size());
        } else {
            assertEquals(context.getMessage("Number of events must be the same"), expectedEventDocuments.size(), events.size());
        }

        for (int i = 0; i < expectedEventDocuments.size(); i++) {
            CommandEvent actual = events.get(i);
            BsonDocument expectedEventDocument = expectedEventDocuments.get(i).asDocument();
            String eventType = expectedEventDocument.getFirstKey();
            context.push(ContextElement.ofCommandEvent(expectedEventDocument, actual, i));
            BsonDocument expected = expectedEventDocument.getDocument(eventType);

            if (expected.containsKey("commandName")) {
                assertEquals(context.getMessage("Command names must be equal"),
                        expected.getString("commandName").getValue(), actual.getCommandName());
            }

            if (expected.containsKey("databaseName")) {
                assertEquals(context.getMessage("Expected database names to match"),
                        expected.getString("databaseName").getValue(), actual.getDatabaseName());
            }

            if (expected.containsKey("hasServiceId")) {
                boolean hasServiceId = expected.getBoolean("hasServiceId").getValue();
                ObjectId serviceId = actual.getConnectionDescription().getServiceId();
                if (hasServiceId) {
                    assertNotNull(context.getMessage("Expected serviceId"), serviceId);
                } else {
                    assertNull(context.getMessage("Expected no serviceId"), serviceId);
                }
            }

            if (expected.containsKey("hasServerConnectionId")) {
                boolean hasServerConnectionId = expected.getBoolean("hasServerConnectionId").getValue();
                Long serverConnectionId = actual.getConnectionDescription().getConnectionId().getServerValue();
                if (hasServerConnectionId) {
                    assertNotNull(context.getMessage("Expected serverConnectionId"), serverConnectionId);
                } else {
                    assertNull(context.getMessage("Expected no serverConnectionId"), serverConnectionId);
                }
            }

            if (actual.getClass().equals(CommandStartedEvent.class)) {
                assertEquals(context.getMessage("Expected CommandStartedEvent"), eventType, "commandStartedEvent");
                CommandStartedEvent actualCommandStartedEvent = (CommandStartedEvent) actual;

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

    public void assertConnectionPoolEventsEquality(final String client, final boolean ignoreExtraEvents, final BsonArray expectedEventDocuments,
                                                   final List<Object> events) {
        context.push(ContextElement.ofConnectionPoolEvents(client, expectedEventDocuments, events));
        if (ignoreExtraEvents) {
            assertTrue(context.getMessage("Number of events must be greater than or equal to the expected number of events"),
                    events.size() >= expectedEventDocuments.size());
        } else {
            assertEquals(context.getMessage("Number of events must be the same"), expectedEventDocuments.size(), events.size());
        }

        for (int i = 0; i < expectedEventDocuments.size(); i++) {
            Object actual = events.get(i);
            BsonDocument expectedEventDocument = expectedEventDocuments.get(i).asDocument();
            String eventType = expectedEventDocument.getFirstKey();
            context.push(ContextElement.ofConnectionPoolEvent(expectedEventDocument, actual, i));

            assertEquals(context.getMessage("Expected event type to match"), eventType, getEventType(actual.getClass()));

            if (actual.getClass().equals(ConnectionPoolClearedEvent.class)) {
                BsonDocument expected = expectedEventDocument.getDocument(eventType);
                ConnectionPoolClearedEvent connectionPoolClearedEvent = (ConnectionPoolClearedEvent) actual;
                if (expected.containsKey("hasServiceId")) {
                    boolean hasServiceId = expected.getBoolean("hasServiceId").getValue();
                    ObjectId serviceId = connectionPoolClearedEvent.getServiceId();
                    if (hasServiceId) {
                        assertNotNull(context.getMessage("Expected serviceId"), serviceId);
                    } else {
                        assertNull(context.getMessage("Expected no serviceId"), serviceId);
                    }
                }
            } else if (actual.getClass().equals(ConnectionCheckOutFailedEvent.class)) {
                BsonDocument expected = expectedEventDocument.getDocument(eventType);
                ConnectionCheckOutFailedEvent actualEvent = (ConnectionCheckOutFailedEvent) actual;
                if (expected.containsKey("reason")) {
                    assertEquals(context.getMessage("Expected reason to match"), expected.getString("reason").getValue(),
                            getReasonString(actualEvent.getReason()));
                }
            } else if (actual.getClass().equals(ConnectionClosedEvent.class)) {
                BsonDocument expected = expectedEventDocument.getDocument(eventType);
                ConnectionClosedEvent actualEvent = (ConnectionClosedEvent) actual;
                if (expected.containsKey("reason")) {
                    assertEquals(context.getMessage("Expected reason to match"), expected.getString("reason").getValue(),
                            getReasonString(actualEvent.getReason()));
                }
            }
            context.pop();
        }
        context.pop();
    }

    public void waitForConnectionPoolEvents(final String client, final BsonDocument event, final int count,
            final TestConnectionPoolListener connectionPoolListener) {
        context.push(ContextElement.ofWaitForConnectionPoolEvents(client, event, count));
        Class<?> eventClass;
        switch (event.getFirstKey()) {
            case "poolClearedEvent":
                eventClass = ConnectionPoolClearedEvent.class;
                break;
            case "poolReadyEvent":
                eventClass = ConnectionPoolReadyEvent.class;
                break;
            case "connectionCreatedEvent":
                eventClass = ConnectionCreatedEvent.class;
                break;
            case "connectionReadyEvent":
                eventClass = ConnectionReadyEvent.class;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported event: " + event.getFirstKey());
        }
        if (!event.getDocument(event.getFirstKey()).isEmpty()) {
            throw new UnsupportedOperationException("Wait for connection pool events does not support event properties");
        }
        try {
            connectionPoolListener.waitForEvent(eventClass, count, 10, TimeUnit.SECONDS);
            context.pop();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            fail(context.getMessage("Timed out waiting for connection pool events"));
        }
    }

    public void assertConnectionPoolEventCount(final String client, final BsonDocument event, final int count, final List<Object> events) {
        context.push(ContextElement.ofConnectionPoolEventCount(client, event, count));
        Class<?> eventClass;
        switch (event.getFirstKey()) {
            case "poolClearedEvent":
                eventClass = ConnectionPoolClearedEvent.class;
                break;
            case "poolReadyEvent":
                eventClass = ConnectionPoolReadyEvent.class;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported event: " + event.getFirstKey());
        }
        if (!event.getDocument(event.getFirstKey()).isEmpty()) {
            throw new UnsupportedOperationException("Wait for connection pool events does not support event properties");
        }
        long matchCount = events.stream().filter(cur -> cur.getClass().equals(eventClass)).count();
        assertEquals(context.getMessage("Expected connection pool event counts to match"), count, matchCount);
        context.pop();
    }


    public void waitForServerDescriptionChangedEvents(final String client, final BsonDocument expectedEvent, final int count,
            final TestServerListener serverListener) {
        context.push(ContextElement.ofWaitForServerDescriptionChangedEvents(client, expectedEvent, count));
        BsonDocument expectedEventContents = getEventContents(expectedEvent);
        try {
            serverListener.waitForServerDescriptionChangedEvents(
                    event -> serverDescriptionChangedEventMatches(expectedEventContents, event), count, Duration.ofSeconds(10));
            context.pop();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            fail(context.getMessage("Timed out waiting for server description changed events"));
        }
    }

    public void assertServerDescriptionChangeEventCount(final String client, final BsonDocument expectedEvent, final int count,
            final List<ServerDescriptionChangedEvent> events) {
        BsonDocument expectedEventContents = getEventContents(expectedEvent);
        context.push(ContextElement.ofServerDescriptionChangedEventCount(client, expectedEvent, count));
        long matchCount = events.stream().filter(event -> serverDescriptionChangedEventMatches(expectedEventContents, event)).count();
        assertEquals(context.getMessage("Expected server description changed event counts to match"), count, matchCount);
        context.pop();
    }

    public void waitForClusterDescriptionChangedEvents(final String client, final BsonDocument expectedEvent, final int count,
            final TestClusterListener clusterListener) {
        context.push(ContextElement.ofWaitForClusterDescriptionChangedEvents(client, expectedEvent, count));
        BsonDocument expectedEventContents = getEventContents(expectedEvent);
        try {
            clusterListener.waitForClusterDescriptionChangedEvents(
                    event -> clusterDescriptionChangedEventMatches(expectedEventContents, event, context), count, Duration.ofSeconds(10));
            context.pop();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            fail(context.getMessage("Timed out waiting for cluster description changed events"));
        }
    }

    public void waitForClusterClosedEvent(final String client, final TestClusterListener clusterListener) {
        context.push(ContextElement.ofWaitForClusterClosedEvent(client));
        try {
            clusterListener.waitForClusterClosedEvent(Duration.ofSeconds(10));
            context.pop();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            fail(context.getMessage("Timed out waiting for cluster description changed events"));
        }
    }

    public void assertClusterDescriptionChangeEventCount(final String client, final BsonDocument expectedEvent, final int count,
            final List<ClusterDescriptionChangedEvent> events) {
        BsonDocument expectedEventContents = getEventContents(expectedEvent);
        context.push(ContextElement.ofClusterDescriptionChangedEventCount(client, expectedEvent, count));
        long matchCount =
                events.stream().filter(event -> clusterDescriptionChangedEventMatches(expectedEventContents, event, context)).count();
        assertEquals(context.getMessage("Expected cluster description changed event counts to match"), count, matchCount);
        context.pop();
    }

    public void assertTopologyEventsEquality(
            final String client,
            final boolean ignoreExtraEvents,
            final BsonArray expectedEventDocuments,
            final List<?> events) {
        context.push(ContextElement.ofTopologyEvents(client, expectedEventDocuments, events));
        if (ignoreExtraEvents) {
            assertTrue(context.getMessage("Number of events must be greater than or equal to the expected number of events"),
                    events.size() >= expectedEventDocuments.size());
        } else {
            assertEquals(context.getMessage("Number of events must be the same"), expectedEventDocuments.size(), events.size());
        }
        for (int i = 0; i < expectedEventDocuments.size(); i++) {
            Object actualEvent = events.get(i);
            BsonDocument expectedEventDocument = expectedEventDocuments.get(i).asDocument();
            String expectedEventType = expectedEventDocument.getFirstKey();
            context.push(ContextElement.ofTopologyEvent(expectedEventDocument, actualEvent, i));
            assertEquals(context.getMessage("Expected event type to match"), expectedEventType, getEventType(actualEvent.getClass()));
            assertTopologyEventEquality(expectedEventType, expectedEventDocument, actualEvent, context);
            context.pop();
        }
        context.pop();
    }

    public <T> void waitForServerMonitorEvents(final String client, final Class<T> expectedEventType, final BsonDocument expectedEvent,
            final int count, final TestServerMonitorListener serverMonitorListener) {
        context.push(ContextElement.ofWaitForServerMonitorEvents(client, expectedEvent, count));
        BsonDocument expectedEventContents = getEventContents(expectedEvent);
        try {
            serverMonitorListener.waitForEvents(expectedEventType,
                    event -> serverMonitorEventMatches(expectedEventContents, event, null), count, Duration.ofSeconds(10));
            context.pop();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            fail(context.getMessage("Timed out waiting for server monitor events"));
        }
    }

    public <T> void assertServerMonitorEventCount(final String client, final Class<T> expectedEventType, final BsonDocument expectedEvent,
            final int count, final TestServerMonitorListener serverMonitorListener) {
        BsonDocument expectedEventContents = getEventContents(expectedEvent);
        context.push(ContextElement.ofServerMonitorEventCount(client, expectedEvent, count));
        long matchCount = serverMonitorListener.countEvents(expectedEventType, event ->
                serverMonitorEventMatches(expectedEventContents, event, null));
        assertEquals(context.getMessage("Expected server monitor event counts to match"), count, matchCount);
        context.pop();
    }

    public void assertServerMonitorEventsEquality(
            final String client,
            final boolean ignoreExtraEvents,
            final BsonArray expectedEventDocuments,
            final List<?> events) {
        context.push(ContextElement.ofServerMonitorEvents(client, expectedEventDocuments, events));
        if (ignoreExtraEvents) {
            assertTrue(context.getMessage("Number of events must be greater than or equal to the expected number of events"),
                    events.size() >= expectedEventDocuments.size());
        } else {
            assertEquals(context.getMessage("Number of events must be the same"), expectedEventDocuments.size(), events.size());
        }
        for (int i = 0; i < expectedEventDocuments.size(); i++) {
            Object actualEvent = events.get(i);
            BsonDocument expectedEventDocument = expectedEventDocuments.get(i).asDocument();
            String expectedEventType = expectedEventDocument.getFirstKey();
            context.push(ContextElement.ofServerMonitorEvent(expectedEventDocument, actualEvent, i));
            assertEquals(context.getMessage("Expected event type to match"), expectedEventType, getEventType(actualEvent.getClass()));
            BsonDocument expectedEventContents = expectedEventDocument.getDocument(expectedEventType);
            serverMonitorEventMatches(expectedEventContents, actualEvent, context);
            context.pop();
        }
        context.pop();
    }

    @NonNull
    private BsonDocument getEventContents(final BsonDocument expectedEvent) {
        HashSet<String> supportedEventTypes = new HashSet<>(asList(
                "serverDescriptionChangedEvent", "topologyDescriptionChangedEvent",
                "serverHeartbeatStartedEvent", "serverHeartbeatSucceededEvent", "serverHeartbeatFailedEvent"));
        String expectedEventType = expectedEvent.getFirstKey();
        if (!supportedEventTypes.contains(expectedEventType)) {
            throw new UnsupportedOperationException("Unsupported event type " + expectedEventType);
        }
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        BsonDocument expectedEventContents = expectedEvent.values().stream().findFirst().get().asDocument();
        if (expectedEventContents.isEmpty()) {
            return expectedEventContents;
        }

        HashSet<String> emptyEventTypes = new HashSet<>(singleton("topologyDescriptionChangedEvent"));
        if (emptyEventTypes.contains(expectedEventType)) {
            throw new UnsupportedOperationException("Contents of " + expectedEventType + " must be empty");
        }
        if (expectedEventContents.size() != 1 || !expectedEventContents.getFirstKey().equals("newDescription")
                || expectedEventContents.getDocument("newDescription").size() != 1) {
            throw new UnsupportedOperationException("Unsupported event contents " + expectedEvent);
        }
        return expectedEventContents;
    }

    private static boolean serverDescriptionChangedEventMatches(final BsonDocument expectedEventContents,
            final ServerDescriptionChangedEvent event) {
        if (expectedEventContents.isEmpty()) {
            return true;
        }
        String newType = expectedEventContents.getDocument("newDescription").getString("type").getValue();
        switch (newType) {
            case "Unknown":
                return event.getNewDescription().getType() == ServerType.UNKNOWN;
            case "LoadBalancer": {
                return event.getNewDescription().getType() == ServerType.LOAD_BALANCER;
            }
            default:
                throw new UnsupportedOperationException();
        }
    }

    private static boolean clusterDescriptionChangedEventMatches(final BsonDocument expectedEventContents,
            final ClusterDescriptionChangedEvent event, @Nullable final AssertionContext context) {
        if (!expectedEventContents.isEmpty()) {
            throw new UnsupportedOperationException(
                    "Contents of " + ClusterDescriptionChangedEvent.class.getSimpleName() + " must be empty");
        }
        return true;
    }

    /**
     * @param context Not {@code null} iff mismatch must result in an error, that is, this method works as an assertion.
     */
    private static <T> void assertTopologyEventEquality(
            final String expectedEventType,
            final BsonDocument expectedEventDocument,
            final T actualEvent,
            final AssertionContext context) {

        switch (expectedEventType) {
            case "topologyOpeningEvent":
                assertTrue(context.getMessage("Expected ClusterOpeningEvent"), actualEvent instanceof ClusterOpeningEvent);
                break;
            case "topologyClosedEvent":
                assertTrue(context.getMessage("Expected ClusterClosedEvent"), actualEvent instanceof ClusterClosedEvent);
                break;
            case "topologyDescriptionChangedEvent":
                assertTrue(context.getMessage("Expected ClusterDescriptionChangedEvent"), actualEvent instanceof ClusterDescriptionChangedEvent);
                ClusterDescriptionChangedEvent event = (ClusterDescriptionChangedEvent) actualEvent;
                BsonDocument topologyChangeDocument = expectedEventDocument.getDocument(expectedEventType, new BsonDocument());

                if (!topologyChangeDocument.isEmpty()) {
                    if (topologyChangeDocument.containsKey("previousDescription")) {
                        String previousDescription = topologyChangeDocument.getDocument("previousDescription").getString("type").getValue();
                        assertEquals(context.getMessage("Expected ClusterDescriptionChangedEvent with previousDescription: " + previousDescription),
                                previousDescription, clusterDescriptionToString(event.getPreviousDescription()));
                    }
                    if (topologyChangeDocument.containsKey("newDescription")) {
                        String newDescription = topologyChangeDocument.getDocument("newDescription").getString("type").getValue();
                        assertEquals(context.getMessage("Expected ClusterDescriptionChangedEvent with newDescription: " + newDescription),
                                newDescription, clusterDescriptionToString(event.getNewDescription()));
                    }
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported topology event:" + expectedEventType);
        }
    }

    /**
     * @param context Not {@code null} iff mismatch must result in an error, that is, this method works as an assertion.
     */
    private static <T> boolean serverMonitorEventMatches(
            final BsonDocument expectedEventContents,
            final T event,
            @Nullable final AssertionContext context) {
        if (expectedEventContents.size() > 1) {
            throw new UnsupportedOperationException("Matching for the following event is not implemented " + expectedEventContents.toJson());
        }
        if (expectedEventContents.containsKey("awaited")) {
            boolean expectedAwaited = expectedEventContents.getBoolean("awaited").getValue();
            boolean actualAwaited = getAwaitedFromServerMonitorEvent(event);
            boolean awaitedMatches = expectedAwaited == actualAwaited;
            if (context != null) {
                assertTrue(context.getMessage("Expected `awaited` to match"), awaitedMatches);
            }
            return awaitedMatches;
        }
        return true;
    }

    static boolean getAwaitedFromServerMonitorEvent(final Object event) {
        if (event instanceof ServerHeartbeatStartedEvent) {
            return ((ServerHeartbeatStartedEvent) event).isAwaited();
        } else if (event instanceof ServerHeartbeatSucceededEvent) {
            return ((ServerHeartbeatSucceededEvent) event).isAwaited();
        } else if (event instanceof ServerHeartbeatFailedEvent) {
            return ((ServerHeartbeatFailedEvent) event).isAwaited();
        } else {
            throw Assertions.fail(event.toString());
        }
    }

    static String getEventType(final Class<?> eventClass) {
        String eventClassName = eventClass.getSimpleName();
        if (eventClassName.startsWith("Cluster")) {
            return eventClassName.replace("Cluster", "topology");
        } else if (eventClassName.startsWith("ConnectionPool")) {
            return eventClassName.replace("ConnectionPool", "pool");
        } else if (eventClassName.startsWith("Connection")) {
            return eventClassName.replace("Connection", "connection");
        } else if (eventClassName.startsWith("ServerHeartbeat")) {
            StringBuilder eventTypeBuilder = new StringBuilder(eventClassName);
            eventTypeBuilder.setCharAt(0, Character.toLowerCase(eventTypeBuilder.charAt(0)));
            return eventTypeBuilder.toString();
        } else {
            throw new UnsupportedOperationException(eventClassName);
        }
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
