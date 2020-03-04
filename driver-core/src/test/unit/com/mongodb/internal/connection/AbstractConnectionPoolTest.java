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

import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerId;
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
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import util.JsonPoweredTestHelper;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;

// Implementation of
// https://github.com/mongodb/specifications/blob/master/source/connection-monitoring-and-pooling/connection-monitoring-and-pooling.rst
// specification tests
@SuppressWarnings("deprecation")
@RunWith(Parameterized.class)
public abstract class AbstractConnectionPoolTest {
    private final String fileName;
    private final String description;
    private final BsonDocument definition;
    private final ServerAddress serverAddress = new ServerAddress("host1");
    private ConnectionPoolSettings settings;
    private final Map<String, ExecutorService> executorServiceMap = new HashMap<String, ExecutorService>();
    private final Map<String, Future<Exception>> futureMap = new HashMap<String, Future<Exception>>();

    private TestConnectionPoolListener listener;
    private ServerId serverId;

    private final Map<String, InternalConnection> connectionMap = new HashMap<String, InternalConnection>();
    private DefaultConnectionPool pool;

    public AbstractConnectionPoolTest(final String fileName, final String description, final BsonDocument definition) {
        this.fileName = fileName;
        this.description = description;
        this.definition = definition;
    }

    @Before
    public void setUp() {
        ConnectionPoolSettings.Builder settingsBuilder = ConnectionPoolSettings.builder()
                .maintenanceFrequency(1, TimeUnit.MILLISECONDS);
        BsonDocument poolOptions = definition.getDocument("poolOptions", new BsonDocument());

        if (poolOptions.containsKey("maxPoolSize")) {
            settingsBuilder.maxSize(poolOptions.getNumber("maxPoolSize").intValue());
        }
        if (poolOptions.containsKey("minPoolSize")) {
            settingsBuilder.minSize(poolOptions.getNumber("minPoolSize").intValue());
        }
        if (poolOptions.containsKey("maxIdleTimeMS")) {
            settingsBuilder.maxConnectionIdleTime(poolOptions.getNumber("maxIdleTimeMS").intValue(), TimeUnit.MILLISECONDS);
        }
        if (poolOptions.containsKey("waitQueueTimeoutMS")) {
            settingsBuilder.maxWaitTime(poolOptions.getNumber("waitQueueTimeoutMS").intValue(), TimeUnit.MILLISECONDS);
        }

        listener = new TestConnectionPoolListener();
        settingsBuilder.addConnectionPoolListener(listener);
        settings = settingsBuilder.build();

        serverId = new ServerId(new ClusterId(), serverAddress);
        pool = new DefaultConnectionPool(serverId, new TestInternalConnectionFactory(), settings);
        pool.start();
    }

    @After
    public void tearDown() {
        for (ExecutorService cur : executorServiceMap.values()) {
            cur.shutdownNow();
        }
        if (pool != null) {
            pool.close();
        }
    }

    @Test
    public void shouldPassAllOutcomes() throws Exception {
        try {
            for (BsonValue cur : definition.getArray("operations")) {
                final BsonDocument operation = cur.asDocument();
                String name = operation.getString("name").getValue();

                if (name.equals("start")) {
                    String target = operation.getString("target", new BsonString("")).getValue();
                    executorServiceMap.put(target, Executors.newSingleThreadExecutor());
                } else if (name.equals("wait")) {
                    Thread.sleep(operation.getNumber("ms").intValue());
                } else if (name.equals("waitForThread")) {
                    String target = operation.getString("target", new BsonString("")).getValue();
                    Exception exceptionFromFuture = futureMap.remove(target).get(5, TimeUnit.SECONDS);
                    if (exceptionFromFuture != null) {
                        throw exceptionFromFuture;
                    }
                } else if (name.equals("waitForEvent")) {
                    Class<?> eventClass = getEventClass(operation.getString("event").getValue());
                    assumeNotNull(eventClass);
                    listener.waitForEvent(eventClass, operation.getNumber("count").intValue(), 5, TimeUnit.SECONDS);
                } else if (name.equals("clear")) {
                    pool.invalidate();
                } else if (name.equals("close")) {
                    pool.close();
                } else if (name.equals("checkOut") || name.equals("checkIn")) {
                    Callable<Exception> callable = createCallable(operation);
                    if (operation.containsKey("thread")) {
                        String threadTarget = operation.getString("thread").getValue();
                        ExecutorService executorService = executorServiceMap.get(threadTarget);
                        futureMap.put(threadTarget, executorService.submit(callable));
                    } else {
                        callable.call();
                    }
                } else {
                    throw new UnsupportedOperationException("No support for " + name);
                }
            }
        } catch (Exception e) {
            if (!definition.containsKey("error")) {
                throw e;
            }
            BsonDocument errorDocument = definition.getDocument("error");
            String exceptionType = errorDocument.getString("type").getValue();
            if (exceptionType.equals("PoolClosedError")) {
                assertEquals(IllegalStateException.class, e.getClass());
            } else if (exceptionType.equals("WaitQueueTimeoutError")) {
                if (e.getClass() != MongoTimeoutException.class) {
                    throw e;
                }
            } else {
                throw e;
            }
        }

        if (definition.containsKey("events")) {
            Iterator<Object> actualEventsIterator = getNonIgnoredActualEvents().iterator();
            BsonArray expectedEvents = definition.getArray("events");
            for (BsonValue cur : expectedEvents) {
                BsonDocument expectedEvent = cur.asDocument();
                String type = expectedEvent.getString("type").getValue();
                if (type.equals("ConnectionPoolCreated")) {
                    ConnectionPoolCreatedEvent actualEvent = getNextEvent(actualEventsIterator, ConnectionPoolCreatedEvent.class);
                    assertEquals(serverAddress, actualEvent.getServerId().getAddress());
                    assertEquals(settings, actualEvent.getSettings());
                } else if (type.equals("ConnectionPoolCleared")) {
                    ConnectionPoolClearedEvent actualEvent = getNextEvent(actualEventsIterator, ConnectionPoolClearedEvent.class);
                    assertEquals(serverAddress, actualEvent.getServerId().getAddress());
                } else if (type.equals("ConnectionPoolClosed")) {
                    ConnectionPoolClosedEvent actualEvent = getNextEvent(actualEventsIterator, ConnectionPoolClosedEvent.class);
                    assertEquals(serverAddress, actualEvent.getServerId().getAddress());
                } else if (type.equals("ConnectionCreated")) {
                    ConnectionCreatedEvent actualEvent = getNextEvent(actualEventsIterator, ConnectionCreatedEvent.class);
                    assertConnectionIdMatch(expectedEvent, actualEvent.getConnectionId());
                } else if (type.equals("ConnectionReady")) {
                    ConnectionReadyEvent actualEvent = getNextEvent(actualEventsIterator, ConnectionReadyEvent.class);
                    assertEquals(serverAddress, actualEvent.getConnectionId().getServerId().getAddress());
                } else if (type.equals("ConnectionClosed")) {
                    ConnectionClosedEvent actualEvent = getNextEvent(actualEventsIterator, ConnectionClosedEvent.class);
                    assertConnectionIdMatch(expectedEvent, actualEvent.getConnectionId());
                    assertReasonMatch(expectedEvent, actualEvent);
                } else if (type.equals("ConnectionCheckOutStarted")) {
                    ConnectionCheckOutStartedEvent actualEvent = getNextEvent(actualEventsIterator, ConnectionCheckOutStartedEvent.class);
                    assertEquals(serverAddress, actualEvent.getServerId().getAddress());
                } else if (type.equals("ConnectionCheckOutFailed")) {
                    ConnectionCheckOutFailedEvent actualEvent = getNextEvent(actualEventsIterator, ConnectionCheckOutFailedEvent.class);
                    assertEquals(serverAddress, actualEvent.getServerId().getAddress());
                } else if (type.equals("ConnectionCheckedOut")) {
                    ConnectionCheckedOutEvent actualEvent = getNextEvent(actualEventsIterator, ConnectionCheckedOutEvent.class);
                    assertConnectionIdMatch(expectedEvent, actualEvent.getConnectionId());
                } else if (type.equals("ConnectionCheckedIn")) {
                    ConnectionCheckedInEvent actualEvent = getNextEvent(actualEventsIterator, ConnectionCheckedInEvent.class);
                    assertConnectionIdMatch(expectedEvent, actualEvent.getConnectionId());
                } else {
                    throw new UnsupportedOperationException("Unsupported event type " + type);
                }
            }
        }
    }

    private void assertReasonMatch(final BsonDocument expectedEvent, final ConnectionClosedEvent connectionClosedEvent) {
        if (!expectedEvent.containsKey("reason")) {
            return;
        }

        String expectedReason = expectedEvent.getString("reason").getValue();
        switch (connectionClosedEvent.getReason()) {
            case STALE:
                assertEquals(expectedReason, "stale");
                break;
            case IDLE:
                assertEquals(expectedReason, "idle");
                break;
            case ERROR:
                assertEquals(expectedReason, "error");
                break;
            case POOL_CLOSED:
                assertEquals(expectedReason, "poolClosed");
                break;
            default:
                fail("Unexpected reason to close connection " + connectionClosedEvent.getReason());
        }
    }

    private void assertConnectionIdMatch(final BsonDocument expectedEvent, final ConnectionId actualConnectionId) {
        int expectedConnectionId = expectedEvent.getNumber("connectionId").intValue();
        if (expectedConnectionId != 42) {
            assertEquals("Connection id does not match", expectedConnectionId, actualConnectionId.getLocalValue());
        }
    }

    private List<Object> getNonIgnoredActualEvents() {
        List<Object> nonIgnoredActualEvents = new ArrayList<Object>();
        Set<Class<?>> ignoredEventClasses = getIgnoredEventClasses();
        for (Object cur : listener.getEvents()) {
            if (!ignoredEventClasses.contains(cur.getClass())) {
                nonIgnoredActualEvents.add(cur);
            }
        }
        return nonIgnoredActualEvents;
    }

    private Set<Class<?>> getIgnoredEventClasses() {
        Set<Class<?>> ignoredEventClasses = new HashSet<Class<?>>();
        ignoredEventClasses.add(com.mongodb.event.ConnectionPoolOpenedEvent.class);
        ignoredEventClasses.add(com.mongodb.event.ConnectionAddedEvent.class);
        ignoredEventClasses.add(com.mongodb.event.ConnectionRemovedEvent.class);
        for (BsonValue cur : definition.getArray("ignore", new BsonArray())) {
            String type = cur.asString().getValue();
            Class<?> eventClass = getEventClass(type);
            if (eventClass != null) {
                ignoredEventClasses.add(eventClass);
            }
        }
        return ignoredEventClasses;
    }

    private Class<?> getEventClass(final String type) {
        if (type.equals("ConnectionPoolCreated")) {
             return ConnectionPoolCreatedEvent.class;
        } else if (type.equals("ConnectionPoolClosed")) {
            return ConnectionPoolClosedEvent.class;
        } else if (type.equals("ConnectionCreated")) {
            return ConnectionCreatedEvent.class;
        } else if (type.equals("ConnectionCheckedOut")) {
            return ConnectionCheckedOutEvent.class;
        } else if (type.equals("ConnectionCheckedIn")) {
            return ConnectionCheckedInEvent.class;
        } else if (type.equals("ConnectionClosed")) {
            return ConnectionClosedEvent.class;
        } else if (type.equals("ConnectionPoolCleared")) {
            return ConnectionPoolClearedEvent.class;
        } else if (type.equals("ConnectionReady")) {
            return ConnectionReadyEvent.class;
        } else if (type.equals("ConnectionCheckOutStarted")) {
            return ConnectionCheckOutStartedEvent.class;
        } else if (type.equals("ConnectionCheckOutFailed")) {
            return ConnectionCheckOutFailedEvent.class;
        } else {
            throw new UnsupportedOperationException("Unsupported event type " + type);
        }
    }


    private <Event> Event getNextEvent(final Iterator<Object> eventsIterator, final Class<Event> expectedType) {
        if (!eventsIterator.hasNext()) {
           fail("Expected event of type " + expectedType + " but there are no more events");
        }
        Object next = eventsIterator.next();
        assertEquals(expectedType, next.getClass());
        return expectedType.cast(next);
    }

    protected abstract Callable<Exception> createCallable(BsonDocument operation);

    protected Map<String, InternalConnection> getConnectionMap() {
        return connectionMap;
    }

    protected DefaultConnectionPool getPool() {
        return pool;
    }

    @Parameterized.Parameters(name = "{0}: {1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/connection-monitoring-and-pooling")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            data.add(new Object[]{file.getName(), testDocument.getString("description").getValue(), testDocument});
        }
        return data;
    }

}
