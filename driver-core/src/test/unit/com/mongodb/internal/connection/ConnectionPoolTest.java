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
import com.mongodb.event.ConnectionAddedEvent;
import com.mongodb.event.ConnectionCheckedInEvent;
import com.mongodb.event.ConnectionCheckedOutEvent;
import com.mongodb.event.ConnectionPoolClosedEvent;
import com.mongodb.event.ConnectionPoolOpenedEvent;
import com.mongodb.event.ConnectionPoolWaitQueueEnteredEvent;
import com.mongodb.event.ConnectionPoolWaitQueueExitedEvent;
import com.mongodb.event.ConnectionRemovedEvent;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.After;
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
@RunWith(Parameterized.class)
public class ConnectionPoolTest {
    private final String fileName;
    private final String description;
    private final BsonDocument definition;
    private final TestConnectionPoolListener listener;
    private final ServerAddress serverAddress = new ServerAddress("host1");
    private final DefaultConnectionPool pool;
    private ConnectionPoolSettings settings;
    private final Map<String, ExecutorService> executorServiceMap = new HashMap<String, ExecutorService>();
    private final Map<String, Future<Exception>> futureMap = new HashMap<String, Future<Exception>>();
    private final Map<String, InternalConnection> connectionMap = new HashMap<String, InternalConnection>();

    public ConnectionPoolTest(final String fileName, final String description, final BsonDocument definition) {
        this.fileName = fileName;
        this.description = description;
        this.definition = definition;

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

        pool = new DefaultConnectionPool(new ServerId(new ClusterId(), serverAddress), new TestInternalConnectionFactory(),
                settings);
        pool.start();
    }

    @After
    public void tearDown() {
        for (ExecutorService cur : executorServiceMap.values()) {
            cur.shutdownNow();
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
                    // TODO: In 4.0 we will support all event classes.  Until then, skipping tests where we need to wait on an
                    // unsupported one
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
                    ConnectionPoolOpenedEvent actualEvent = getNextEvent(actualEventsIterator, ConnectionPoolOpenedEvent.class);
                    assertEquals(serverAddress, actualEvent.getServerId().getAddress());
                    assertEquals(settings, actualEvent.getSettings());
                } else if (type.equals("ConnectionPoolClosed")) {
                    ConnectionPoolClosedEvent actualEvent = getNextEvent(actualEventsIterator, ConnectionPoolClosedEvent.class);
                    assertEquals(serverAddress, actualEvent.getServerId().getAddress());
                } else if (type.equals("ConnectionPoolCleared")) {
                    // TODO in 4.0, when this event will be implemented
                } else if (type.equals("ConnectionReady")) {
                    // TODO in 4.0, when this event will be implemented
                } else if (type.equals("ConnectionCheckOutStarted")) {
                    // TODO in 4.0, when this event will be implemented
                } else if (type.equals("ConnectionCheckOutFailed")) {
                    // TODO in 4.0, when this event will be implemented
                } else if (type.equals("ConnectionCreated")) {
                    ConnectionAddedEvent actualEvent = getNextEvent(actualEventsIterator, ConnectionAddedEvent.class);
                    assertConnectionIdMatch(expectedEvent, actualEvent.getConnectionId());
                } else if (type.equals("ConnectionCheckedOut")) {
                    ConnectionCheckedOutEvent actualEvent = getNextEvent(actualEventsIterator, ConnectionCheckedOutEvent.class);
                    assertConnectionIdMatch(expectedEvent, actualEvent.getConnectionId());
                } else if (type.equals("ConnectionCheckedIn")) {
                    ConnectionCheckedInEvent actualEvent = getNextEvent(actualEventsIterator, ConnectionCheckedInEvent.class);
                    assertConnectionIdMatch(expectedEvent, actualEvent.getConnectionId());
                } else if (type.equals("ConnectionClosed")) {
                    ConnectionRemovedEvent actualEvent = getNextEvent(actualEventsIterator, ConnectionRemovedEvent.class);
                    assertConnectionIdMatch(expectedEvent, actualEvent.getConnectionId());
                    assertReasonMatch(expectedEvent, actualEvent);
                } else {
                    throw new UnsupportedOperationException("Unsupported event type " + type);
                }
            }
        }
    }

    private void assertReasonMatch(final BsonDocument expectedEvent, final ConnectionRemovedEvent connectionRemovedEvent) {
        if (!expectedEvent.containsKey("reason")) {
            return;
        }

        String expectedReason = expectedEvent.getString("reason").getValue();
        switch (connectionRemovedEvent.getReason()) {
            case STALE:
                assertEquals(expectedReason, "stale");
                break;
            case MAX_IDLE_TIME_EXCEEDED:
                assertEquals(expectedReason, "idle");
                break;
            case ERROR:
                assertEquals(expectedReason, "error");
                break;
            case POOL_CLOSED:
                assertEquals(expectedReason, "poolClosed");
                break;
            default:
                fail("Unexpected reason to close connection " + connectionRemovedEvent.getReason());
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
        ignoredEventClasses.add(ConnectionPoolWaitQueueEnteredEvent.class);
        ignoredEventClasses.add(ConnectionPoolWaitQueueExitedEvent.class);
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
             return ConnectionPoolOpenedEvent.class;
        } else if (type.equals("ConnectionPoolClosed")) {
            return ConnectionPoolClosedEvent.class;
        } else if (type.equals("ConnectionCreated")) {
            return ConnectionAddedEvent.class;
        } else if (type.equals("ConnectionCheckedOut")) {
            return ConnectionCheckedOutEvent.class;
        } else if (type.equals("ConnectionCheckedIn")) {
            return ConnectionCheckedInEvent.class;
        } else if (type.equals("ConnectionClosed")) {
            return ConnectionRemovedEvent.class;
        } else if (type.equals("ConnectionPoolCleared")) {
            return null;
        } else if (type.equals("ConnectionReady")) {
            return null;
        } else if (type.equals("ConnectionCheckOutStarted")) {
            return null;
        } else if (type.equals("ConnectionCheckOutFailed")) {
            return null;
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

    private Callable<Exception> createCallable(final BsonDocument operation) {
        String name = operation.getString("name").getValue();
        if (name.equals("checkOut")) {
            return new Callable<Exception>() {
                @Override
                public Exception call() {
                    try {
                        InternalConnection connection = pool.get();
                        if (operation.containsKey("label")) {
                            connectionMap.put(operation.getString("label").getValue(), connection);
                        }
                        return null;
                    } catch (Exception e) {
                        return e;
                    }
                }
            };
        } else if (name.equals("checkIn")) {
            return new Callable<Exception>() {
                @Override
                public Exception call() {
                    try {
                        InternalConnection connection = connectionMap.get(operation.getString("connection").getValue());
                        connection.close();
                        return null;
                    } catch (Exception e) {
                        return e;
                    }
                }
            };
        } else {
            throw new UnsupportedOperationException("Operation " + name + " not supported");
        }
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
