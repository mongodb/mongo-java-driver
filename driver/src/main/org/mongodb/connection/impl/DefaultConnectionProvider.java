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

package org.mongodb.connection.impl;

import org.bson.ByteBuf;
import org.mongodb.MongoException;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ConnectionFactory;
import org.mongodb.connection.ConnectionProvider;
import org.mongodb.connection.MongoSocketException;
import org.mongodb.connection.MongoSocketInterruptedReadException;
import org.mongodb.connection.MongoWaitQueueFullException;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ResponseSettings;
import org.mongodb.connection.ServerAddress;
import org.mongodb.management.MBeanServerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

class DefaultConnectionProvider implements ConnectionProvider {

    private final ConcurrentPool<UsageTrackingConnection> pool;
    private final ConnectionProviderSettings settings;
    private final AtomicInteger waitQueueSize = new AtomicInteger(0);
    private final AtomicInteger generation = new AtomicInteger(0);
    private final ExecutorService sizeMaintenanceTimer;
    private final ServerAddress serverAddress;
    private final ConnectionPoolStatistics statistics;
    private final Runnable maintenanceTask;

    public DefaultConnectionProvider(final ServerAddress serverAddress, final ConnectionFactory connectionFactory,
                                     final ConnectionProviderSettings settings) {
        this.serverAddress = serverAddress;
        this.settings = settings;
        pool = new ConcurrentPool<UsageTrackingConnection>(settings.getMaxSize(),
                new UsageTrackingConnectionItemFactory(connectionFactory));
        statistics = new ConnectionPoolStatistics(serverAddress, settings.getMinSize(), settings.getMaxSize(), pool);
        MBeanServerFactory.getMBeanServer().registerMBean(statistics, statistics.getObjectName());
        maintenanceTask = createMaintenanceTask();
        sizeMaintenanceTimer = createTimer();
    }

    @Override
    public Connection get() {
        return get(settings.getMaxWaitTime(MILLISECONDS), MILLISECONDS);
    }

    @Override
    public Connection get(final long timeout, final TimeUnit timeUnit) {
        try {
            if (waitQueueSize.incrementAndGet() > settings.getMaxWaitQueueSize()) {
                throw new MongoWaitQueueFullException(String.format("Too many threads are already waiting for a connection. "
                        + "Max number of threads (maxWaitQueueSize) of %d has been exceeded.",
                        settings.getMaxWaitQueueSize()));
            }
            UsageTrackingConnection connection = pool.get(timeout, timeUnit);
            while (shouldPrune(connection)) {
                pool.release(connection, true);
                connection = pool.get(timeout, timeUnit);
            }
            return new PooledConnection(connection);
        } finally {
            waitQueueSize.decrementAndGet();
        }
    }

    @Override
    public void close() {
        pool.close();
        if (sizeMaintenanceTimer != null) {
            sizeMaintenanceTimer.shutdownNow();
        }
        MBeanServerFactory.getMBeanServer().unregisterMBean(statistics.getObjectName());
    }

    /**
     * Gets the statistics for the connection pool.
     *
     * @return the statistics.
     */
    public ConnectionPoolStatistics getStatistics() {
        return statistics;
    }

    /**
     * Synchronously prune idle connections and ensure the minimum pool size.
     */
    public void doMaintenance() {
        if (maintenanceTask != null) {
            maintenanceTask.run();
        }
    }

    private Runnable createMaintenanceTask() {
        Runnable newMaintenanceTask = null;
        if (shouldPrune() || shouldEnsureMinSize()) {
            newMaintenanceTask = new Runnable() {
                @Override
                public synchronized void run() {
                    if (shouldPrune()) {
                        pool.prune();
                    }
                    if (shouldEnsureMinSize()) {
                        pool.ensureMinSize(settings.getMinSize());
                    }
                }
            };
        }
        return newMaintenanceTask;
    }

    private ExecutorService createTimer() {
        if (maintenanceTask == null) {
            return null;
        }
        else {
            ScheduledExecutorService newTimer = Executors.newSingleThreadScheduledExecutor();
            newTimer.scheduleAtFixedRate(maintenanceTask, 0, settings.getMaintenanceFrequency(MILLISECONDS), MILLISECONDS);
            return newTimer;
        }
    }

    private boolean shouldEnsureMinSize() {
        return settings.getMinSize() > 0;
    }

    private boolean shouldPrune() {
        return settings.getMaxConnectionIdleTime(MILLISECONDS) > 0 || settings.getMaxConnectionLifeTime(MILLISECONDS) > 0;
    }

    private boolean shouldPrune(final UsageTrackingConnection connection) {
        final long curTime = System.currentTimeMillis();
        return generation.get() > connection.getGeneration()
                || expired(connection.getOpenedAt(), curTime, settings.getMaxConnectionLifeTime(MILLISECONDS))
                || expired(connection.getLastUsedAt(), curTime, settings.getMaxConnectionIdleTime(MILLISECONDS));
    }

    private boolean expired(final long startTime, final long curTime, final long maxTime) {
        return maxTime != 0 && curTime - startTime > maxTime;
    }

    /**
     * If there was a socket exception that wasn't some form of interrupted read, increment the generation count so that
     * any connections created prior will be discarded.
     *
     * @param e the exception
     */
    private void incrementGenerationOnSocketException(final MongoException e) {
        if (e instanceof MongoSocketException && !(e instanceof MongoSocketInterruptedReadException)) {
            generation.incrementAndGet();
        }
    }

    private class PooledConnection implements Connection {
        private volatile UsageTrackingConnection wrapped;

        public PooledConnection(final UsageTrackingConnection wrapped) {
            this.wrapped = notNull("wrapped", wrapped);
        }

        @Override
        public void close() {
            if (wrapped != null) {
                pool.release(wrapped, wrapped.isClosed() || shouldPrune(wrapped));
                wrapped = null;
            }
        }

        @Override
        public boolean isClosed() {
            return wrapped == null || wrapped.isClosed();
        }

        @Override
        public ServerAddress getServerAddress() {
            isTrue("open", !isClosed());
            return wrapped.getServerAddress();
        }

        @Override
        public void sendMessage(final List<ByteBuf> byteBuffers) {
            isTrue("open", wrapped != null);
            try {
                wrapped.sendMessage(byteBuffers);
            } catch (MongoException e) {
                incrementGenerationOnSocketException(e);
                throw e;
            }
        }

        @Override
        public ResponseBuffers receiveMessage(final ResponseSettings responseSettings) {
            isTrue("open", wrapped != null);
            try {
                return wrapped.receiveMessage(responseSettings);
            } catch (MongoException e) {
                incrementGenerationOnSocketException(e);
                throw e;
            }
        }
    }

    private class UsageTrackingConnectionItemFactory implements ConcurrentPool.ItemFactory<UsageTrackingConnection> {
        private final ConnectionFactory connectionFactory;

        public UsageTrackingConnectionItemFactory(final ConnectionFactory connectionFactory) {
            this.connectionFactory = connectionFactory;
        }

        @Override
        public UsageTrackingConnection create() {
            return new UsageTrackingConnection(connectionFactory.create(serverAddress), generation.get());
        }

        @Override
        public void close(final UsageTrackingConnection connection) {
            connection.close();
        }

        @Override
        public boolean shouldPrune(final UsageTrackingConnection usageTrackingConnection) {
            return DefaultConnectionProvider.this.shouldPrune(usageTrackingConnection);
        }
    }
}