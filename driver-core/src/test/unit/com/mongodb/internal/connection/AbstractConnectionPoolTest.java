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

import com.mongodb.ClusterFixture;
import com.mongodb.JsonTestServerVersionChecker;
import com.mongodb.MongoDriverInformation;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.StreamFactory;
import com.mongodb.event.ConnectionCheckOutFailedEvent;
import com.mongodb.event.ConnectionCheckOutStartedEvent;
import com.mongodb.event.ConnectionCheckedInEvent;
import com.mongodb.event.ConnectionCheckedOutEvent;
import com.mongodb.event.ConnectionClosedEvent;
import com.mongodb.event.ConnectionCreatedEvent;
import com.mongodb.event.ConnectionPoolClearedEvent;
import com.mongodb.event.ConnectionPoolClosedEvent;
import com.mongodb.event.ConnectionPoolCreatedEvent;
import com.mongodb.event.ConnectionPoolReadyEvent;
import com.mongodb.event.ConnectionReadyEvent;
import com.mongodb.event.ServerListener;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.inject.SameObjectProvider;
import com.mongodb.internal.operation.CommandReadOperation;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.types.ObjectId;
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
import java.util.Collections;
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
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.assertions.Assertions.assertFalse;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;
import static org.mockito.Mockito.mock;

// Implementation of
// https://github.com/mongodb/specifications/blob/master/source/connection-monitoring-and-pooling/connection-monitoring-and-pooling.rst
// specification tests
@SuppressWarnings("deprecation")
@RunWith(Parameterized.class)
public abstract class AbstractConnectionPoolTest {
    private static final int ANY_INT = 42;
    private static final String ANY_STRING = "42";
    private static final Set<String> PRESTART_POOL_ASYNC_WORK_MANAGER_FILE_NAMES = Collections.singleton("wait-queue-timeout.json");

    private final String fileName;
    private final String description;
    private final BsonDocument definition;
    private final boolean skipTest;
    private ConnectionPoolSettings settings;
    private final Map<String, ExecutorService> executorServiceMap = new HashMap<String, ExecutorService>();
    private final Map<String, Future<Exception>> futureMap = new HashMap<String, Future<Exception>>();

    private TestConnectionPoolListener listener;
    @Nullable
    private BsonDocument configureFailPointCommand;

    private final Map<String, InternalConnection> connectionMap = new HashMap<String, InternalConnection>();
    private ConnectionPool pool;

    public AbstractConnectionPoolTest(
            final String fileName, final String description, final BsonDocument definition, final boolean skipTest) {
        this.fileName = fileName;
        this.description = description;
        this.definition = definition;
        this.skipTest = skipTest;
    }

    @Before
    public void setUp() {
        assumeFalse(skipTest);
        ConnectionPoolSettings.Builder settingsBuilder = ConnectionPoolSettings.builder();
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
        if (poolOptions.containsKey("backgroundThreadIntervalMS")) {
            long intervalMillis = poolOptions.getNumber("backgroundThreadIntervalMS").longValue();
            assertFalse(intervalMillis == 0);
            if (intervalMillis < 0) {
                settingsBuilder.maintenanceInitialDelay(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            } else {
                /* Using frequency/period instead of an interval as required by the specification is incorrect, for example,
                 * because it opens up a possibility to run the background thread non-stop if runs are as long as or longer than the period.
                 * Nevertheless, I am reusing what we already have in the driver instead of clogging up the implementation. */
                settingsBuilder.maintenanceFrequency(
                        poolOptions.getNumber("backgroundThreadIntervalMS").longValue(), TimeUnit.MILLISECONDS);
            }
        }
        if (poolOptions.containsKey("maxConnecting")) {
            settingsBuilder.maxConnecting(poolOptions.getInt32("maxConnecting").intValue());
        }

        listener = new TestConnectionPoolListener();
        settingsBuilder.addConnectionPoolListener(listener);
        settings = settingsBuilder.build();
        InternalConnectionPoolSettings internalSettings = InternalConnectionPoolSettings.builder()
                .prestartAsyncWorkManager(PRESTART_POOL_ASYNC_WORK_MANAGER_FILE_NAMES.contains(fileName))
                .build();
        Style style = Style.of(definition.getString("style").getValue());
        switch (style) {
            case UNIT: {
                ServerId serverId = new ServerId(new ClusterId(), new ServerAddress("host1"));
                pool = new DefaultConnectionPool(serverId, new TestInternalConnectionFactory(), settings, internalSettings,
                        SameObjectProvider.initialized(mock(SdamServerDescriptionManager.class)));
                break;
            }
            case INTEGRATION: {
                ServerId serverId = new ServerId(new ClusterId(), ClusterFixture.getPrimary());
                ClusterConnectionMode connectionMode = ClusterConnectionMode.MULTIPLE;
                SameObjectProvider<SdamServerDescriptionManager> sdamProvider = SameObjectProvider.uninitialized();
                pool = new ConnectionIdAdjustingConnectionPool(new DefaultConnectionPool(serverId,
                        new InternalStreamConnectionFactory(
                                connectionMode,
                                createStreamFactory(SocketSettings.builder().build(), ClusterFixture.getSslSettings()),
                                ClusterFixture.getCredentialWithCache(),
                                poolOptions.getString("appName", new BsonString(fileName + ": " + description)).getValue(),
                                MongoDriverInformation.builder().build(),
                                Collections.emptyList(),
                                new TestCommandListener(),
                                ClusterFixture.getServerApi()),
                        settings, internalSettings, sdamProvider));
                sdamProvider.initialize(new DefaultSdamServerDescriptionManager(serverId, mock(ServerDescriptionChangedListener.class),
                        mock(ServerListener.class), mock(ServerMonitor.class), pool, connectionMode));
                setFailPoint();
                break;
            }
            default: {
                throw new AssertionError(format("Style %s is not implemented", style));
            }
        }
        if (internalSettings.isPrestartAsyncWorkManager()) {
            waitForPoolAsyncWorkManagerStart();
        }
    }

    @After
    @SuppressWarnings("try")
    public void tearDown() {
        try (ConnectionPool unused = pool) {
            disableFailPoint();
        } finally {
            for (ExecutorService cur : executorServiceMap.values()) {
                cur.shutdownNow();
            }
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
                    executorServiceMap.put(target, Executors.newSingleThreadExecutor(r -> {
                        Thread result = Executors.defaultThreadFactory().newThread(r);
                        result.setName(target);
                        return result;
                    }));
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
                    long timeoutMillis = operation.getNumber("timeout", new BsonInt64(TimeUnit.SECONDS.toMillis(5)))
                            .longValue();
                    listener.waitForEvent(eventClass, operation.getNumber("count").intValue(), timeoutMillis, TimeUnit.MILLISECONDS);
                } else if (name.equals("clear")) {
                    pool.invalidate(null);
                } else if (name.equals("ready")) {
                    pool.ready();
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
                    assertAddressMatch(expectedEvent, actualEvent.getServerId().getAddress());
                    assertEquals(settings, actualEvent.getSettings());
                } else if (type.equals("ConnectionPoolCleared")) {
                    ConnectionPoolClearedEvent actualEvent = getNextEvent(actualEventsIterator, ConnectionPoolClearedEvent.class);
                    assertAddressMatch(expectedEvent, actualEvent.getServerId().getAddress());
                } else if (type.equals("ConnectionPoolReady")) {
                    ConnectionPoolReadyEvent actualEvent = getNextEvent(actualEventsIterator, ConnectionPoolReadyEvent.class);
                    assertAddressMatch(expectedEvent, actualEvent.getServerId().getAddress());
                } else if (type.equals("ConnectionPoolClosed")) {
                    ConnectionPoolClosedEvent actualEvent = getNextEvent(actualEventsIterator, ConnectionPoolClosedEvent.class);
                    assertAddressMatch(expectedEvent, actualEvent.getServerId().getAddress());
                } else if (type.equals("ConnectionCreated")) {
                    ConnectionCreatedEvent actualEvent = getNextEvent(actualEventsIterator, ConnectionCreatedEvent.class);
                    assertConnectionIdMatch(expectedEvent, actualEvent.getConnectionId());
                } else if (type.equals("ConnectionReady")) {
                    ConnectionReadyEvent actualEvent = getNextEvent(actualEventsIterator, ConnectionReadyEvent.class);
                    assertAddressMatch(expectedEvent, actualEvent.getConnectionId().getServerId().getAddress());
                } else if (type.equals("ConnectionClosed")) {
                    ConnectionClosedEvent actualEvent = getNextEvent(actualEventsIterator, ConnectionClosedEvent.class);
                    assertConnectionIdMatch(expectedEvent, actualEvent.getConnectionId());
                    assertReasonMatch(expectedEvent, actualEvent);
                } else if (type.equals("ConnectionCheckOutStarted")) {
                    ConnectionCheckOutStartedEvent actualEvent = getNextEvent(actualEventsIterator, ConnectionCheckOutStartedEvent.class);
                    assertAddressMatch(expectedEvent, actualEvent.getServerId().getAddress());
                } else if (type.equals("ConnectionCheckOutFailed")) {
                    ConnectionCheckOutFailedEvent actualEvent = getNextEvent(actualEventsIterator, ConnectionCheckOutFailedEvent.class);
                    assertAddressMatch(expectedEvent, actualEvent.getServerId().getAddress());
                    assertReasonMatch(expectedEvent, actualEvent);
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

    private void assertReasonMatch(final BsonDocument expectedEvent, final ConnectionCheckOutFailedEvent connectionCheckOutFailedEvent) {
        if (!expectedEvent.containsKey("reason")) {
            return;
        }

        String expectedReason = expectedEvent.getString("reason").getValue();
        switch (connectionCheckOutFailedEvent.getReason()) {
            case TIMEOUT:
                assertEquals(expectedReason, "timeout");
                break;
            case CONNECTION_ERROR:
                assertEquals(expectedReason, "connectionError");
                break;
            case POOL_CLOSED:
                assertEquals(expectedReason, "poolClosed");
                break;
            default:
                fail("Unexpected reason to fail connection check out " + connectionCheckOutFailedEvent.getReason());
        }
    }

    private static void assertAddressMatch(final BsonDocument expectedEvent, final ServerAddress actualAddress) {
        String addressKey = "address";
        if (expectedEvent.isString(addressKey)) {
            String expectedAddress = expectedEvent.getString(addressKey).getValue();
            if (!expectedAddress.equals(ANY_STRING)) {
                assertEquals(format("Address does not match (expected event is %s)", expectedEvent.toString()),
                        new ServerAddress(expectedAddress), actualAddress);
            }
        } else if (expectedEvent.containsKey(addressKey)) {
            assertEquals("Unsupported value", ANY_INT, expectedEvent.getInt32(addressKey).intValue());
        }
    }

    private void assertConnectionIdMatch(final BsonDocument expectedEvent, final ConnectionId actualConnectionId) {
        int actualConnectionIdLocalValue = actualConnectionId.getLocalValue();
        int adjustedConnectionIdLocalValue = adjustedConnectionIdLocalValue(actualConnectionIdLocalValue);
        String connectionIdKey = "connectionId";
        if (expectedEvent.containsKey(connectionIdKey)) {
            int expectedConnectionId = expectedEvent.getInt32(connectionIdKey).intValue();
            if (expectedConnectionId != ANY_INT) {
                assertEquals(format(
                        "Connection id does not match (expected event is %s; actual local value before adjustment is %s)",
                        expectedEvent.toString(), actualConnectionIdLocalValue),
                        expectedConnectionId, adjustedConnectionIdLocalValue);
            }
        }
    }

    private int adjustedConnectionIdLocalValue(final int connectionIdLocalValue) {
        if (pool instanceof ConnectionIdAdjustingConnectionPool) {
            return ((ConnectionIdAdjustingConnectionPool) pool).adjustedConnectionIdLocalValue(connectionIdLocalValue);
        } else {
            return connectionIdLocalValue;
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
        } else if (type.equals("ConnectionPoolReady")) {
            return ConnectionPoolReadyEvent.class;
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

    private static void executeAdminCommand(final BsonDocument command) {
        new CommandReadOperation<>("admin", command, new BsonDocumentCodec()).execute(ClusterFixture.getBinding());
    }

    private void setFailPoint() {
        final String failPointKey = "failPoint";
        if (definition.containsKey(failPointKey)) {
            configureFailPointCommand = definition.getDocument(failPointKey);
            executeAdminCommand(configureFailPointCommand);
        }
    }

    private void disableFailPoint() {
        if (configureFailPointCommand != null) {
            executeAdminCommand(configureFailPointCommand.append("mode", new BsonString("off")));
        }
    }

    protected abstract Callable<Exception> createCallable(BsonDocument operation);

    protected abstract StreamFactory createStreamFactory(SocketSettings socketSettings, SslSettings sslSettings);

    protected Map<String, InternalConnection> getConnectionMap() {
        return connectionMap;
    }

    protected ConnectionPool getPool() {
        return pool;
    }

    @Parameterized.Parameters(name = "{0}: {1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/connection-monitoring-and-pooling")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            data.add(new Object[]{
                    file.getName(),
                    testDocument.getString("description").getValue(),
                    testDocument,
                    JsonTestServerVersionChecker.skipTest(testDocument, BsonDocument.parse("{}"))
            });
        }
        return data;
    }

    public static void waitForPoolAsyncWorkManagerStart() {
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MongoInterruptedException(null, e);
        }
    }

    private enum Style {
        UNIT,
        INTEGRATION;

        public static Style of(final String name) {
            return valueOf(name.toUpperCase());
        }
    }

    private static final class ConnectionIdAdjustingConnectionPool implements ConnectionPool {
        private static final int UNINITIALIZED = Integer.MAX_VALUE;

        private final DefaultConnectionPool pool;
        private final AtomicInteger connectionIdLocalValueAdjustment;

        private ConnectionIdAdjustingConnectionPool(final DefaultConnectionPool pool) {
            this.pool = pool;
            connectionIdLocalValueAdjustment = new AtomicInteger(UNINITIALIZED);
        }

        private void updateConnectionIdLocalValueAdjustment(final InternalConnection conn) {
            connectionIdLocalValueAdjustment.accumulateAndGet(conn.getDescription().getConnectionId().getLocalValue() - 1, Math::min);
        }

        int adjustedConnectionIdLocalValue(final int connectionIdLocalValue) {
            return connectionIdLocalValue - connectionIdLocalValueAdjustment.get();
        }

        @Override
        public InternalConnection get() {
            InternalConnection result = pool.get();
            updateConnectionIdLocalValueAdjustment(result);
            return result;
        }

        @Override
        public InternalConnection get(final long timeout, final TimeUnit timeUnit) {
            InternalConnection result = pool.get(timeout, timeUnit);
            updateConnectionIdLocalValueAdjustment(result);
            return result;
        }

        @Override
        public void getAsync(final SingleResultCallback<InternalConnection> callback) {
            pool.getAsync((result, problem) -> {
                try {
                    if (result != null) {
                        updateConnectionIdLocalValueAdjustment(result);
                    }
                } finally {
                    callback.onResult(result, problem);
                }
            });
        }

        @Override
        public void invalidate(@Nullable final Throwable cause) {
            pool.invalidate(cause);
        }

        @Override
        public void invalidate(final ObjectId serviceId, final int generation) {
            pool.invalidate(serviceId, generation);
        }

        @Override
        public void ready() {
            pool.ready();
        }

        @Override
        public void close() {
            pool.close();
        }

        @Override
        public int getGeneration() {
            return pool.getGeneration();
        }
    }
}
