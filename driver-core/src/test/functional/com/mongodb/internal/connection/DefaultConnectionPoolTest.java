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

import com.mongodb.MongoConnectionPoolClearedException;
import com.mongodb.MongoServerUnavailableException;
import com.mongodb.ServerAddress;
import com.mongodb.client.syncadapter.SupplyingCallback;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerId;
import com.mongodb.event.ConnectionCreatedEvent;
import com.mongodb.internal.Timeout;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.inject.EmptyProvider;
import com.mongodb.internal.inject.OptionalProvider;
import com.mongodb.internal.inject.SameObjectProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import static java.lang.Long.MAX_VALUE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * These tests are racy, so doing them in Java instead of Groovy to reduce chance of failure.
 */
public class DefaultConnectionPoolTest {
    private static final ServerId SERVER_ID = new ServerId(new ClusterId(), new ServerAddress());
    private static final long TEST_WAIT_TIMEOUT_MILLIS = SECONDS.toMillis(5);
    private static final int DEFAULT_MAX_CONNECTING = ConnectionPoolSettings.builder().build().getMaxConnecting();

    private TestInternalConnectionFactory connectionFactory;

    private DefaultConnectionPool provider;
    private ExecutorService cachedExecutor;

    @BeforeEach
    public void setUp() {
        connectionFactory = new TestInternalConnectionFactory();
        cachedExecutor = Executors.newCachedThreadPool();
    }

    @AfterEach
    @SuppressWarnings("try")
    public void cleanup() throws InterruptedException {
        //noinspection unused
        try (DefaultConnectionPool closed = provider) {
            cachedExecutor.shutdownNow();
            //noinspection ResultOfMethodCallIgnored
            cachedExecutor.awaitTermination(MAX_VALUE, NANOSECONDS);
        }
    }

    @Test
    public void shouldThrowOnTimeout() throws InterruptedException {
        // given
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                ConnectionPoolSettings.builder()
                        .maxSize(1)
                        .maxWaitTime(50, MILLISECONDS)
                        .build(),
                mockSdamProvider());
        provider.ready();
        provider.get();

        // when
        TimeoutTrackingConnectionGetter connectionGetter = new TimeoutTrackingConnectionGetter(provider);
        new Thread(connectionGetter).start();

        connectionGetter.getLatch().await();

        // then
        assertTrue(connectionGetter.isGotTimeout());
    }

    @Test
    public void shouldThrowOnPoolClosed() {
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                ConnectionPoolSettings.builder()
                        .maxSize(1)
                        .maxWaitTime(50, MILLISECONDS)
                        .build(),
                mockSdamProvider());
        provider.close();

        String expectedExceptionMessage = "The server at 127.0.0.1:27017 is no longer available";
        MongoServerUnavailableException exception;
        exception = assertThrows(MongoServerUnavailableException.class, () -> provider.get());
        assertEquals(expectedExceptionMessage, exception.getMessage());
        SupplyingCallback<InternalConnection> supplyingCallback = new SupplyingCallback<>();
        provider.getAsync(supplyingCallback);
        exception = assertThrows(MongoServerUnavailableException.class, supplyingCallback::get);
        assertEquals(expectedExceptionMessage, exception.getMessage());
    }

    @Test
    public void shouldExpireConnectionAfterMaxLifeTime() throws InterruptedException {
        // given
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                ConnectionPoolSettings.builder()
                        .maxSize(1)
                        .maintenanceInitialDelay(5, MINUTES)
                        .maxConnectionLifeTime(50, MILLISECONDS)
                        .build(),
                mockSdamProvider());
        provider.ready();

        // when
        provider.get().close();
        Thread.sleep(100);
        provider.doMaintenance();
        provider.get();

        // then
        assertTrue(connectionFactory.getNumCreatedConnections() >= 2);  // should really be two, but it's racy
    }

    @Test
    public void shouldExpireConnectionAfterLifeTimeOnClose() throws InterruptedException {
        // given
        provider = new DefaultConnectionPool(SERVER_ID,
                connectionFactory,
                ConnectionPoolSettings.builder()
                        .maxSize(1)
                        .maxConnectionLifeTime(20, MILLISECONDS).build(),
                mockSdamProvider());
        provider.ready();

        // when
        InternalConnection connection = provider.get();
        Thread.sleep(50);
        connection.close();

        // then
        assertTrue(connectionFactory.getCreatedConnections().get(0).isClosed());
    }

    @Test
    public void shouldExpireConnectionAfterMaxIdleTime() throws InterruptedException {
        // given
        provider = new DefaultConnectionPool(SERVER_ID,
                connectionFactory,
                ConnectionPoolSettings.builder()
                        .maxSize(1)
                        .maintenanceInitialDelay(5, MINUTES)
                        .maxConnectionIdleTime(50, MILLISECONDS).build(),
                mockSdamProvider());
        provider.ready();

        // when
        provider.get().close();
        Thread.sleep(100);
        provider.doMaintenance();
        provider.get();

        // then
        assertTrue(connectionFactory.getNumCreatedConnections() >= 2);  // should really be two, but it's racy
    }

    @Test
    public void shouldCloseConnectionAfterExpiration() throws InterruptedException {
        // given
        provider = new DefaultConnectionPool(SERVER_ID,
                connectionFactory,
                ConnectionPoolSettings.builder()
                        .maxSize(1)
                        .maintenanceInitialDelay(5, MINUTES)
                        .maxConnectionLifeTime(20, MILLISECONDS).build(),
                mockSdamProvider());
        provider.ready();

        // when
        provider.get().close();
        Thread.sleep(50);
        provider.doMaintenance();
        provider.get();

        // then
        assertTrue(connectionFactory.getCreatedConnections().get(0).isClosed());
    }

    @Test
    public void shouldCreateNewConnectionAfterExpiration() throws InterruptedException {
        // given
        provider = new DefaultConnectionPool(SERVER_ID,
                connectionFactory,
                ConnectionPoolSettings.builder()
                        .maxSize(1)
                        .maintenanceInitialDelay(5, MINUTES)
                        .maxConnectionLifeTime(20, MILLISECONDS).build(),
                mockSdamProvider());
        provider.ready();

        // when
        provider.get().close();
        Thread.sleep(50);
        provider.doMaintenance();
        InternalConnection secondConnection = provider.get();

        // then
        assertNotNull(secondConnection);
        assertEquals(2, connectionFactory.getNumCreatedConnections());
    }

    @Test
    public void shouldPruneAfterMaintenanceTaskRuns() throws InterruptedException {
        // given
        provider = new DefaultConnectionPool(SERVER_ID,
                connectionFactory,
                ConnectionPoolSettings.builder()
                        .maxSize(10)
                        .maxConnectionLifeTime(1, MILLISECONDS)
                        .maintenanceInitialDelay(5, MINUTES)
                        .build(),
                mockSdamProvider());
        provider.ready();
        provider.get().close();


        // when
        Thread.sleep(10);
        provider.doMaintenance();

        // then
        assertTrue(connectionFactory.getCreatedConnections().get(0).isClosed());
    }

    @Test
    void infiniteMaxSize() {
        int defaultMaxSize = ConnectionPoolSettings.builder().build().getMaxSize();
        provider = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                ConnectionPoolSettings.builder().maxSize(0).build(), EmptyProvider.instance());
        provider.ready();
        List<InternalConnection> connections = new ArrayList<>();
        try {
            for (int i = 0; i < 2 * defaultMaxSize; i++) {
                connections.add(provider.get());
            }
        } finally {
            connections.forEach(connection -> {
                try {
                    connection.close();
                } catch (RuntimeException e) {
                    // ignore
                }
            });
        }
    }

    @ParameterizedTest
    @MethodSource("concurrentUsageArguments")
    @Tag("Slow")
    public void concurrentUsage(final int minSize, final int maxSize, final boolean limitConnectionLifeIdleTime,
                                final int concurrentUsersCount,
                                final boolean checkoutSync, final boolean checkoutAsync,
                                final float invalidateAndReadyProb, final float invalidateProb, final float readyProb)
            throws InterruptedException {
        ControllableConnectionFactory controllableConnFactory = newControllableConnectionFactory(cachedExecutor);
        provider = new DefaultConnectionPool(SERVER_ID, controllableConnFactory.factory,
                ConnectionPoolSettings.builder()
                    .minSize(minSize)
                    .maxSize(maxSize)
                    .maxWaitTime(TEST_WAIT_TIMEOUT_MILLIS, MILLISECONDS)
                    .maintenanceInitialDelay(0, NANOSECONDS)
                    .maintenanceFrequency(100, MILLISECONDS)
                    .maxConnectionLifeTime(limitConnectionLifeIdleTime ? 350 : 0, MILLISECONDS)
                    .maxConnectionIdleTime(limitConnectionLifeIdleTime ? 50 : 0, MILLISECONDS)
                    .build(),
                mockSdamProvider());
        provider.ready();
        assertUseConcurrently(provider, concurrentUsersCount,
                checkoutSync, checkoutAsync,
                invalidateAndReadyProb, invalidateProb, readyProb,
                cachedExecutor, SECONDS.toNanos(10));
    }

    private static Stream<Arguments> concurrentUsageArguments() {
        return Stream.of(// variants marked with (*) have proved their usefulness by detecting bugs
                Arguments.of(0, 1, true, 8, true, false, 0.02f, 0, 0),
                Arguments.of(0, 1, false, 8, false, true, 0.02f, 0, 0), // (*)
                Arguments.of(DEFAULT_MAX_CONNECTING, DEFAULT_MAX_CONNECTING, true, 8, true, true, 0, 0, 0),
                Arguments.of(DEFAULT_MAX_CONNECTING + 1, DEFAULT_MAX_CONNECTING + 5, true, 2 * (DEFAULT_MAX_CONNECTING + 5),
                        true, true, 0.02f, 0, 0),
                Arguments.of(DEFAULT_MAX_CONNECTING + 5, DEFAULT_MAX_CONNECTING + 5, false, 2 * (DEFAULT_MAX_CONNECTING + 5),
                        true, true, 0.02f, 0, 0), // (*)
                Arguments.of(DEFAULT_MAX_CONNECTING + 1, DEFAULT_MAX_CONNECTING + 5, false, 2 * (DEFAULT_MAX_CONNECTING + 5),
                        true, true, 0.3f, 0.1f, 0.1f),
                Arguments.of(DEFAULT_MAX_CONNECTING + 1, DEFAULT_MAX_CONNECTING + 5, true, 2 * (DEFAULT_MAX_CONNECTING + 5),
                        true, true, 0, 0.5f, 0.05f));
    }

    @Test
    @Tag("Slow")
    public void callbackShouldNotBlockCheckoutIfOpenAsyncWorksNotInCurrentThread() throws InterruptedException, TimeoutException {
        int maxAvailableConnections = 7;
        ControllableConnectionFactory controllableConnFactory = newControllableConnectionFactory(cachedExecutor);
        TestConnectionPoolListener listener = new TestConnectionPoolListener();
        provider = new DefaultConnectionPool(SERVER_ID, controllableConnFactory.factory,
                ConnectionPoolSettings.builder()
                    .maxSize(DEFAULT_MAX_CONNECTING + maxAvailableConnections)
                    .addConnectionPoolListener(listener)
                    .maxWaitTime(TEST_WAIT_TIMEOUT_MILLIS, MILLISECONDS)
                    .maintenanceInitialDelay(MAX_VALUE, NANOSECONDS)
                    .build(),
                mockSdamProvider());
        provider.ready();
        acquireOpenPermits(provider, DEFAULT_MAX_CONNECTING, InfiniteCheckoutEmulation.INFINITE_CALLBACK,
                controllableConnFactory, listener);
        assertUseConcurrently(provider, 2 * maxAvailableConnections,
                true, true,
                0.02f, 0, 0,
                cachedExecutor, SECONDS.toNanos(10));
    }

    /**
     * The idea of this test is as follows:
     * <ol>
     *     <li>Check out some connections from the pool
     *     ({@link #DEFAULT_MAX_CONNECTING} connections must not be checked out to make the next step possible).</li>
     *     <li>Acquire all permits to open a connection and leave them acquired.</li>
     *     <li>Check in the checked out connections and concurrently check them out again.</li>
     * </ol>
     * If the hand-over mechanism fails, then some checkouts may be infinitely stuck trying to open a connection:
     * since there are no permits to open available, the hand-over mechanism is the only way to get a connection.
     */
    @Test
    @Tag("Slow")
    public void checkoutHandOverMechanism() throws InterruptedException, TimeoutException {
        int openConnectionsCount = 5_000;
        int maxConcurrentlyHandedOver = 7;
        ControllableConnectionFactory controllableConnFactory = newControllableConnectionFactory(cachedExecutor);
        TestConnectionPoolListener listener = new TestConnectionPoolListener();
        provider = new DefaultConnectionPool(SERVER_ID, controllableConnFactory.factory,
                ConnectionPoolSettings.builder()
                    .maxSize(DEFAULT_MAX_CONNECTING
                            + openConnectionsCount
                            /* This wiggle room is needed to open opportunities to create new connections from the standpoint of
                             * the max pool size, and then check that no connections were created nonetheless. */
                            + maxConcurrentlyHandedOver)
                    .addConnectionPoolListener(listener)
                    .maxWaitTime(TEST_WAIT_TIMEOUT_MILLIS, MILLISECONDS)
                    .maintenanceInitialDelay(MAX_VALUE, NANOSECONDS)
                    .build(),
                mockSdamProvider());
        provider.ready();
        List<InternalConnection> connections = new ArrayList<>();
        for (int i = 0; i < openConnectionsCount; i++) {
            connections.add(provider.get(0, NANOSECONDS));
        }
        acquireOpenPermits(provider, DEFAULT_MAX_CONNECTING, InfiniteCheckoutEmulation.INFINITE_OPEN, controllableConnFactory, listener);
        int previousIdx = 0;
        // concurrently check in / check out and assert the hand-over mechanism works
        for (int idx = 0; idx < connections.size(); idx += maxConcurrentlyHandedOver) {
            Collection<Future<ConnectionId>> handedOverFutures = new ArrayList<>();
            Collection<Future<ConnectionId>> receivedFutures = new ArrayList<>();
            while (previousIdx < idx) {
                int currentIdx = previousIdx;
                previousIdx++;
                Runnable checkIn = () -> handedOverFutures.add(cachedExecutor.submit(() -> {
                    InternalConnection connection = connections.get(currentIdx);
                    ConnectionId connectionId = connection.getDescription().getConnectionId();
                    connections.get(currentIdx).close();
                    return connectionId;
                }));
                Runnable checkOut = () -> receivedFutures.add(cachedExecutor.submit(() -> {
                    InternalConnection connection = provider.get(TEST_WAIT_TIMEOUT_MILLIS, MILLISECONDS);
                    return connection.getDescription().getConnectionId();
                }));
                if (ThreadLocalRandom.current().nextBoolean()) {
                    checkIn.run();
                    checkOut.run();
                } else {
                    checkOut.run();
                    checkIn.run();
                }
            }
            try {
                Set<ConnectionId> handedOver = new HashSet<>();
                Set<ConnectionId> received = new HashSet<>();
                for (Future<ConnectionId> handedOverFuture : handedOverFutures) {
                    handedOver.add(handedOverFuture.get(TEST_WAIT_TIMEOUT_MILLIS, MILLISECONDS));
                }
                for (Future<ConnectionId> receivedFuture : receivedFutures) {
                    received.add(receivedFuture.get(TEST_WAIT_TIMEOUT_MILLIS, MILLISECONDS));
                }
                assertEquals(handedOver, received);
            } catch (TimeoutException | ExecutionException e) {
                throw new AssertionError(e);
            }
        }
    }

    @Test
    public void readyAfterCloseMustNotThrow() {
        provider = new DefaultConnectionPool(
                SERVER_ID,
                connectionFactory,
                ConnectionPoolSettings.builder().maxSize(1).build(),
                mockSdamProvider());
        provider.close();
        provider.ready();
    }

    @Test
    public void invalidateAfterCloseMustNotThrow() {
        provider = new DefaultConnectionPool(
                SERVER_ID,
                connectionFactory,
                ConnectionPoolSettings.builder().maxSize(1).build(),
                mockSdamProvider());
        provider.ready();
        provider.close();
        provider.invalidate(null);
    }

    @Test
    public void readyInvalidateConcurrentWithCloseMustNotThrow() throws ExecutionException, InterruptedException {
        Future<?> readyAndInvalidateResult = null;
        for (int i = 0; i < 3_000; i++) {
            provider = new DefaultConnectionPool(
                    SERVER_ID,
                    connectionFactory,
                    ConnectionPoolSettings.builder().maxSize(1).build(),
                    mockSdamProvider());
            try {
                readyAndInvalidateResult = cachedExecutor.submit(() -> {
                    provider.ready();
                    provider.invalidate(null);
                });
            } finally {
                provider.close();
                if (readyAndInvalidateResult != null) {
                    readyAndInvalidateResult.get();
                }
            }
        }
    }

    private static void assertUseConcurrently(final DefaultConnectionPool pool, final int concurrentUsersCount,
                                              final boolean sync, final boolean async,
                                              final float invalidateAndReadyProb, final float invalidateProb, final float readyProb,
                                              final ExecutorService executor, final long durationNanos) throws InterruptedException {
        try {
            useConcurrently(pool, concurrentUsersCount,
                    sync, async,
                    invalidateAndReadyProb, invalidateProb, readyProb,
                    executor, durationNanos);
        } catch (TimeoutException | ExecutionException e) {
            throw new AssertionError(e);
        }
    }

    private static void useConcurrently(final DefaultConnectionPool pool, final int concurrentUsersCount,
                                        final boolean checkoutSync, final boolean checkoutAsync,
                                        final float invalidateAndReadyProb, final float invalidateProb, final float readyProb,
                                        final ExecutorService executor, final long durationNanos)
            throws ExecutionException, InterruptedException, TimeoutException {
        assertTrue(invalidateAndReadyProb >= 0 && invalidateAndReadyProb <= 1);
        Runnable spontaneouslyInvalidateReady = () -> {
            if (ThreadLocalRandom.current().nextFloat() < invalidateAndReadyProb) {
                pool.invalidate(null);
                pool.ready();
            }
            if (ThreadLocalRandom.current().nextFloat() < invalidateProb) {
                pool.invalidate(null);
            }
            if (ThreadLocalRandom.current().nextFloat() < readyProb) {
                pool.ready();
            }
        };
        Collection<Future<?>> tasks = new ArrayList<>();
        Timeout duration = Timeout.startNow(durationNanos);
        for (int i = 0; i < concurrentUsersCount; i++) {
            if ((checkoutSync && checkoutAsync) ? i % 2 == 0 : checkoutSync) {//check out synchronously and check in
                tasks.add(executor.submit(() -> {
                    while (!(duration.expired() || Thread.currentThread().isInterrupted())) {
                        spontaneouslyInvalidateReady.run();
                        InternalConnection conn = null;
                        try {
                            conn = pool.get(TEST_WAIT_TIMEOUT_MILLIS, MILLISECONDS);
                        } catch (MongoConnectionPoolClearedException e) {
                            // expected because we spontaneously invalidate `pool`
                        } finally {
                            if (conn != null) {
                                conn.close();
                            }
                        }
                    }
                }));
            } else if (checkoutAsync) {//check out asynchronously and check in
                tasks.add(executor.submit(() -> {
                    while (!(duration.expired() || Thread.currentThread().isInterrupted())) {
                        spontaneouslyInvalidateReady.run();
                        CompletableFuture<InternalConnection> futureCheckOutCheckIn = new CompletableFuture<>();
                        pool.getAsync((conn, t) -> {
                            if (t != null) {
                                if (t instanceof MongoConnectionPoolClearedException) {
                                    futureCheckOutCheckIn.complete(null); // expected because we spontaneously invalidate `pool`
                                } else {
                                    futureCheckOutCheckIn.completeExceptionally(t);
                                }
                            } else {
                                conn.close();
                                futureCheckOutCheckIn.complete(null);
                            }
                        });
                        try {
                            futureCheckOutCheckIn.get(TEST_WAIT_TIMEOUT_MILLIS, MILLISECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        } catch (ExecutionException | TimeoutException e) {
                            throw new AssertionError(e);
                        }
                    }
                }));
            }
        }
        for (Future<?> task : tasks) {
            task.get(durationNanos + MILLISECONDS.toNanos(TEST_WAIT_TIMEOUT_MILLIS), NANOSECONDS);
        }
    }

    /**
     * Returns early if {@linkplain Thread#interrupt() interrupted}.
     */
    private static void sleepMillis(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * This method starts asynchronously checking out {@code openPermitsCount} connections in such a way that checkout never completes.
     * This results in acquiring permits to open a connection and leaving them acquired.
     */
    private static void acquireOpenPermits(final DefaultConnectionPool pool, final int openPermitsCount,
                                           final InfiniteCheckoutEmulation infiniteEmulation,
                                           final ControllableConnectionFactory controllableConnFactory,
                                           final TestConnectionPoolListener listener) throws TimeoutException, InterruptedException {
        assertTrue(openPermitsCount <= DEFAULT_MAX_CONNECTING);
        int initialCreatedEventCount = listener.countEvents(ConnectionCreatedEvent.class);
        switch (infiniteEmulation) {
            case INFINITE_CALLBACK: {
                for (int i = 0; i < openPermitsCount; i++) {
                    SingleResultCallback<InternalConnection> infiniteCallback = (result, t) -> sleepMillis(MAX_VALUE);
                    pool.getAsync(infiniteCallback);
                }
                break;
            }
            case INFINITE_OPEN: {
                controllableConnFactory.openDurationHandle.set(Duration.ofMillis(MAX_VALUE), openPermitsCount);
                for (int i = 0; i < openPermitsCount; i++) {
                    pool.getAsync((result, t) -> {});
                }
                controllableConnFactory.openDurationHandle.await(Duration.ofMillis(TEST_WAIT_TIMEOUT_MILLIS));
                break;
            }
            default: {
                fail();
            }
        }
        listener.waitForEvent(//wait until openPermitsCount are guaranteed to be acquired
                ConnectionCreatedEvent.class, initialCreatedEventCount + openPermitsCount, TEST_WAIT_TIMEOUT_MILLIS, MILLISECONDS);
    }

    private static ControllableConnectionFactory newControllableConnectionFactory(final ExecutorService asyncOpenExecutor) {
        ControllableConnectionFactory.OpenDurationHandle openDurationHandle = new ControllableConnectionFactory.OpenDurationHandle();
        InternalConnectionFactory connectionFactory = (serverId, connectionGenerationSupplier) -> {
            InternalConnection connection = mock(InternalConnection.class, withSettings().stubOnly());
            when(connection.getGeneration()).thenReturn(connectionGenerationSupplier.getGeneration());
            when(connection.getDescription()).thenReturn(new ConnectionDescription(serverId));
            AtomicBoolean open = new AtomicBoolean(false);
            when(connection.opened()).thenAnswer(invocation -> open.get());
            Runnable doOpen = () -> {
                sleepMillis(openDurationHandle.getDurationAndCountDown().toMillis());
                if (ThreadLocalRandom.current().nextFloat() < 0.2) { // add a bit more randomness
                    sleepMillis(ThreadLocalRandom.current().nextInt(7, 15));
                }
                open.set(true);
            };
            doAnswer(invocation -> {
                doOpen.run();
                return null;
            }).when(connection).open();
            doAnswer(invocation -> {
                SingleResultCallback<?> callback = invocation.getArgument(0, SingleResultCallback.class);
                asyncOpenExecutor.execute(() -> {
                    doOpen.run();
                    callback.onResult(null, null);
                });
                return null;
            }).when(connection).openAsync(any());
            return connection;
        };
        return new ControllableConnectionFactory(connectionFactory, openDurationHandle);
    }

    private OptionalProvider<SdamServerDescriptionManager> mockSdamProvider() {
        return SameObjectProvider.initialized(mock(SdamServerDescriptionManager.class));
    }

    private static class ControllableConnectionFactory {
        private final InternalConnectionFactory factory;
        private final OpenDurationHandle openDurationHandle;

        ControllableConnectionFactory(final InternalConnectionFactory factory, final OpenDurationHandle openDurationHandle) {
            this.factory = factory;
            this.openDurationHandle = openDurationHandle;
        }

        static final class OpenDurationHandle {
            private Duration duration;
            private long count;
            private final Lock lock;
            private final Condition countIsZeroCondition;

            private OpenDurationHandle() {
                duration = Duration.ZERO;
                lock = new ReentrantLock();
                countIsZeroCondition = lock.newCondition();
            }

            /**
             * Sets the specified {@code duration} for the next {@code count} connections.
             */
            void set(final Duration duration, final long count) {
                lock.lock();
                try {
                    assertEquals(this.count, 0);
                    if (count > 0) {
                        this.duration = duration;
                        this.count = count;
                    }
                } finally {
                    lock.unlock();
                }
            }

            private Duration getDurationAndCountDown() {
                lock.lock();
                try {
                    Duration result = duration;
                    if (count > 0) {
                        count--;
                        if (count == 0) {
                            duration = Duration.ZERO;
                            countIsZeroCondition.signalAll();
                        }
                    }
                    return result;
                } finally {
                    lock.unlock();
                }
            }

            /**
             * Wait until {@link #factory} has started opening as many connections as were specified via {@link #set(Duration, long)}.
             */
            void await(final Duration timeout) throws InterruptedException {
                long remainingNanos = timeout.toNanos();
                lock.lock();
                try {
                    while (count > 0) {
                        assertTrue(remainingNanos > 0, "Timed out after " + timeout);
                        remainingNanos = countIsZeroCondition.awaitNanos(remainingNanos);
                    }
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    private enum InfiniteCheckoutEmulation {
        INFINITE_OPEN,
        INFINITE_CALLBACK
    }
}
