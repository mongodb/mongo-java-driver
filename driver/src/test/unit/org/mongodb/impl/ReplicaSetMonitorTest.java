/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.impl;

import org.junit.Before;
import org.junit.Test;
import org.mongodb.MongoException;
import org.mongodb.MongoInternalException;
import org.mongodb.MongoTimeoutException;
import org.mongodb.ServerAddress;
import org.mongodb.command.IsMasterCommandResult;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mongodb.assertions.Assertions.isTrue;

public class ReplicaSetMonitorTest {
    private ServerAddress seedAddress;
    private ReplicaSetMonitor replicaSetMonitor;
    private PredicableMongoServerStateNotifierFactory serverStateNotifierFactory;
    private PredictableScheduledExecutorService scheduledExecutorService;
    private Map<ServerAddress, PredictableMongoServerStateNotifier> stateNotifierMap = new HashMap<ServerAddress,
            PredictableMongoServerStateNotifier>();

    @Before
    public void setUp() throws UnknownHostException {
        seedAddress = new ServerAddress("localhost:27017");
        replicaSetMonitor = new ReplicaSetMonitor(Arrays.asList(seedAddress));
        serverStateNotifierFactory = new PredicableMongoServerStateNotifierFactory();
        scheduledExecutorService = new PredictableScheduledExecutorService();
    }

    @Test
    public void testShutdown() throws UnknownHostException {
        final PredictableMongoServerStateNotifier stateNotifier = new PredictableMongoServerStateNotifier(seedAddress);
        stateNotifierMap.put(seedAddress, stateNotifier);
        replicaSetMonitor.start(serverStateNotifierFactory, scheduledExecutorService);
        replicaSetMonitor.shutdownNow();
        assertTrue(stateNotifier.isClosed());
        try {
            replicaSetMonitor.start(serverStateNotifierFactory, scheduledExecutorService);
            fail();
        } catch (IllegalStateException e) { // NOPMD
            // all good
        }

        replicaSetMonitor.notify(seedAddress, stateNotifier.isMasterCommandResult);
        replicaSetMonitor.notify(seedAddress, new MongoException("ignore this"));
    }

    @Test
    public void testNotificationFromPrimary() throws UnknownHostException {
        PredictableMongoServerStateNotifier stateNotifier = new PredictableMongoServerStateNotifier(seedAddress);
        stateNotifierMap.put(seedAddress, stateNotifier);
        stateNotifier.isMasterCommandResult = IsMasterCommandResult.builder().ok(true).primary(true).setName("test").build();
        replicaSetMonitor.start(serverStateNotifierFactory, scheduledExecutorService);
        scheduledExecutorService.runAll();
        assertTrue(replicaSetMonitor.getCurrentReplicaSetDescription().hasPrimary());
        assertEquals(seedAddress, replicaSetMonitor.getCurrentReplicaSetDescription().getPrimary().getServerAddress());
    }

    @Test
    public void testDiscovery() throws UnknownHostException {
        PredictableMongoServerStateNotifier stateNotifierForSecondary = new PredictableMongoServerStateNotifier(seedAddress);
        stateNotifierMap.put(seedAddress, stateNotifierForSecondary);
        stateNotifierForSecondary.isMasterCommandResult = IsMasterCommandResult.builder().ok(true).secondary(true)
                .hosts(Arrays.asList("localhost:27017", "localhost:27018")).passives(Arrays.asList("localhost:27019"))
                .build();

        ServerAddress primaryAddress = new ServerAddress("localhost:27018");
        PredictableMongoServerStateNotifier stateNotifierForPrimary =
                new PredictableMongoServerStateNotifier(primaryAddress);
        stateNotifierMap.put(primaryAddress, stateNotifierForPrimary);
        stateNotifierForPrimary.isMasterCommandResult = IsMasterCommandResult.builder().ok(true).primary(true)
                .hosts(Arrays.asList("localhost:27017", "localhost:27018")).passives(Arrays.asList("localhost:27019"))
                .build();

        ServerAddress passiveAddress = new ServerAddress("localhost:27019");
        PredictableMongoServerStateNotifier stateNotifierForPassive =
                new PredictableMongoServerStateNotifier(passiveAddress);
        stateNotifierMap.put(passiveAddress, stateNotifierForPassive);
        stateNotifierForPassive.isMasterCommandResult = IsMasterCommandResult.builder().ok(true)
                .hosts(Arrays.asList("localhost:27017", "localhost:27018")).passives(Arrays.asList("localhost:27019"))
                .build();

        replicaSetMonitor.start(serverStateNotifierFactory, scheduledExecutorService);
        scheduledExecutorService.runAll();
        scheduledExecutorService.runAll();
        assertTrue(replicaSetMonitor.getCurrentReplicaSetDescription().hasPrimary());
        assertEquals(primaryAddress, replicaSetMonitor.getCurrentReplicaSetDescription().getPrimary().getServerAddress());
        assertNotNull(replicaSetMonitor.getCurrentReplicaSetDescription().getMember(new ServerAddress("localhost:27019")));
    }

    @Test
    public void testWaitForAllFirstNotifications() throws UnknownHostException {
        PredictableMongoServerStateNotifier stateNotifierForPrimary = new PredictableMongoServerStateNotifier(seedAddress);
        stateNotifierMap.put(seedAddress, stateNotifierForPrimary);
        stateNotifierForPrimary.isMasterCommandResult = IsMasterCommandResult.builder().ok(true).primary(true)
                .hosts(Arrays.asList("localhost:27017", "localhost:27018")).passives(Arrays.asList("localhost:27019"))
                .build();

        ServerAddress secondaryAddress = new ServerAddress("localhost:27018");
        PredictableMongoServerStateNotifier stateNotifierForSecondary =
                new PredictableMongoServerStateNotifier(secondaryAddress);
        stateNotifierMap.put(secondaryAddress, stateNotifierForSecondary);
        stateNotifierForSecondary.isMasterCommandResult = IsMasterCommandResult.builder().ok(true).primary(true)
                .hosts(Arrays.asList("localhost:27017", "localhost:27018")).passives(Arrays.asList("localhost:27019"))
                .build();

        ServerAddress passiveAddress = new ServerAddress("localhost:27019");
        PredictableMongoServerStateNotifier stateNotifierForPassive =
                new PredictableMongoServerStateNotifier(passiveAddress);
        stateNotifierMap.put(passiveAddress, stateNotifierForPassive);
        stateNotifierForPassive.isMasterCommandResult = IsMasterCommandResult.builder().ok(true)
                .hosts(Arrays.asList("localhost:27017", "localhost:27018")).passives(Arrays.asList("localhost:27019"))
                .build();

        replicaSetMonitor.start(serverStateNotifierFactory, scheduledExecutorService);
        assertTrue(holderTimedOut());
        scheduledExecutorService.futures.get(0).command.run();
        assertTrue(holderTimedOut());
        scheduledExecutorService.futures.get(1).command.run();
        assertTrue(!holderTimedOut());
    }

    private boolean holderTimedOut() {
        try {
            replicaSetMonitor.getCurrentReplicaSetDescription(1, TimeUnit.MILLISECONDS);
            return false;
        } catch (MongoTimeoutException e) {
            return true;
        }
    }

    @Test
    public void testExceptionNotification() {
        PredictableMongoServerStateNotifier stateNotifier = new PredictableMongoServerStateNotifier(seedAddress);
        stateNotifierMap.put(seedAddress, stateNotifier);
        stateNotifier.isMasterCommandResult = IsMasterCommandResult.builder()
                .ok(true).primary(true).setName("test_rs")
                .hosts(Arrays.asList("localhost:27017")).primary("localhost:27017")
                .build();
        replicaSetMonitor.start(serverStateNotifierFactory, scheduledExecutorService);
        scheduledExecutorService.runAll();
        assertTrue(replicaSetMonitor.getCurrentReplicaSetDescription().hasPrimary());
        stateNotifier.exception = new MongoInternalException("uh oh", new Throwable());
        scheduledExecutorService.runAll();
        assertTrue(replicaSetMonitor.getCurrentReplicaSetDescription().getAll().isEmpty());
    }

    @Test
    public void testBadHostName() {
        PredictableMongoServerStateNotifier stateNotifier = new PredictableMongoServerStateNotifier(seedAddress);
        stateNotifierMap.put(seedAddress, stateNotifier);
        stateNotifier.isMasterCommandResult = IsMasterCommandResult.builder().ok(true).primary(true).hosts(Arrays.asList("flllogga"))
                .build();
        replicaSetMonitor.start(serverStateNotifierFactory, scheduledExecutorService);
        scheduledExecutorService.runAll();
    }

    private class PredicableMongoServerStateNotifierFactory implements MongoServerStateNotifierFactory {
        @Override
        public MongoServerStateNotifier create(final ServerAddress serverAddress) {
            isTrue("exists", stateNotifierMap.get(serverAddress) != null);
            return stateNotifierMap.get(serverAddress);
        }
    }

    private final class PredictableMongoServerStateNotifier implements MongoServerStateNotifier {
        private ServerAddress serverAddress;
        private boolean isClosed;
        private IsMasterCommandResult isMasterCommandResult;
        private MongoException exception;

        private PredictableMongoServerStateNotifier(final ServerAddress serverAddress) {
            this.serverAddress = serverAddress;
        }

        @Override
        public void run() {
            if (exception != null) {
                replicaSetMonitor.notify(serverAddress, exception);
            }
            else {
                replicaSetMonitor.notify(serverAddress, isMasterCommandResult);
            }
        }

        @Override
        public void close() {
            isClosed = true;
        }

        public boolean isClosed() {
            return isClosed;
        }
    }

    private class PredictableScheduledExecutorService implements ScheduledExecutorService,
            Iterable<PredictableScheduledExecutorService.PredictablyScheduledFuture<Void>> {
        private boolean isShutdown;
        private final List<PredictablyScheduledFuture<Void>> futures = new ArrayList<PredictablyScheduledFuture<Void>>();

        public void runAll() {
            for (PredictablyScheduledFuture future : new ArrayList<PredictablyScheduledFuture>(futures)) {
                future.command.run();
            }
        }

        @Override
        public Iterator<PredictablyScheduledFuture<Void>> iterator() {
            return new ArrayList<PredictablyScheduledFuture<Void>>(futures).iterator();
        }

        @Override
        public ScheduledFuture<?> schedule(final Runnable command, final long delay, final TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <V> ScheduledFuture<V> schedule(final Callable<V> callable, final long delay, final TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(final Runnable command, final long initialDelay, final long period,
                                                      final TimeUnit unit) {
            PredictablyScheduledFuture retVal = new PredictablyScheduledFuture(command, initialDelay, period, unit);
            futures.add(retVal);
            return retVal;
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(final Runnable command, final long initialDelay, final long delay,
                                                         final TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdown() {
            shutdownNow();
        }

        @Override
        public List<Runnable> shutdownNow() {
            isShutdown = true;
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            return isShutdown;
        }

        @Override
        public boolean isTerminated() {
            return isShutdown;
        }

        @Override
        public boolean awaitTermination(final long timeout, final TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> Future<T> submit(final Callable<T> task) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> Future<T> submit(final Runnable task, final T result) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Future<?> submit(final Runnable task) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks, final long timeout, final TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T invokeAny(final Collection<? extends Callable<T>> tasks) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T invokeAny(final Collection<? extends Callable<T>> tasks, final long timeout, final TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void execute(final Runnable command) {
            throw new UnsupportedOperationException();
        }

        class PredictablyScheduledFuture<V> implements ScheduledFuture<V> {
            private final long initialDelay;
            private final long period;
            private final TimeUnit unit;
            private final Runnable command;

            public PredictablyScheduledFuture(final Runnable command, final long initialDelay, final long period, final TimeUnit unit) {
                //To change body of created methods use File | Settings | File Templates.
                this.command = command;
                this.initialDelay = initialDelay;
                this.period = period;
                this.unit = unit;
            }

            // CHECKSTYLE:OFF
            @Override
            public long getDelay(final TimeUnit unit) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int compareTo(final Delayed o) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean cancel(final boolean mayInterruptIfRunning) {
                futures.remove(this);
                return true;
            }

            @Override
            public boolean isCancelled() {
                return futures.contains(this);
            }

            @Override
            public boolean isDone() {
                throw new UnsupportedOperationException();
            }

            @Override
            public V get() throws InterruptedException, ExecutionException {
                throw new UnsupportedOperationException();
            }

            @Override
            public V get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                throw new UnsupportedOperationException();
            }
            // CHECKSTYLE:ON
        }
    }
}
