/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.bson.util.Assertions.notNull;

class PooledConnectionProvider  {
    private static final Logger LOGGER = Loggers.getLogger("connection");

    private final ConcurrentPool<Connection> pool;
    private final ConnectionPoolSettings settings;
    private final AtomicInteger waitQueueSize = new AtomicInteger(0);
    private final AtomicInteger generation = new AtomicInteger(0);
    private final ExecutorService sizeMaintenanceTimer;
    private final String clusterId;
    private final ServerAddress serverAddress;
    private final Runnable maintenanceTask;
    private final ConnectionPoolListener connectionPoolListener;
    private final ConnectionFactory connectionFactory;
    private volatile boolean closed;
    private volatile boolean hasWorked;

    public PooledConnectionProvider(final String clusterId, final ServerAddress serverAddress,
                                    final ConnectionFactory connectionFactory,
                                    final ConnectionPoolSettings settings,
                                    final ConnectionPoolListener connectionPoolListener) {
        this.connectionFactory = connectionFactory;
        this.clusterId = notNull("clusterId", clusterId);
        this.serverAddress = notNull("serverAddress", serverAddress);
        this.settings = notNull("settings", settings);
        ConnectionItemFactory connectionItemFactory = new ConnectionItemFactory();
        pool = new ConcurrentPool<Connection>(settings.getMaxSize(), connectionItemFactory);
        maintenanceTask = createMaintenanceTask();
        sizeMaintenanceTimer = createTimer();
        this.connectionPoolListener = notNull("connectionPoolListener", connectionPoolListener);
        connectionPoolListener.connectionPoolOpened(new ConnectionPoolOpenedEvent(clusterId, serverAddress, settings));
    }

    public Connection get() {
        return get(settings.getMaxWaitTime(MILLISECONDS), MILLISECONDS);
    }

    public Connection get(final long timeout, final TimeUnit timeUnit) {
        try {
            if (waitQueueSize.incrementAndGet() > settings.getMaxWaitQueueSize()) {
                throw new MongoWaitQueueFullException(format("Too many threads are already waiting for a connection. "
                                                             + "Max number of threads (maxWaitQueueSize) of %d has been exceeded.",
                                                             settings.getMaxWaitQueueSize()));
            }
            connectionPoolListener.waitQueueEntered(new ConnectionPoolWaitQueueEvent(clusterId, serverAddress, currentThread().getId()));
            Connection connection = pool.get(timeout, timeUnit);
            hasWorked = true;
            while (shouldPrune(connection)) {
                pool.release(connection, true);
                connection = pool.get(timeout, timeUnit);
            }
            connectionPoolListener.connectionCheckedOut(new ConnectionEvent(clusterId, serverAddress));
            return connection;
        } finally {
            waitQueueSize.decrementAndGet();
            connectionPoolListener.waitQueueExited(new ConnectionPoolWaitQueueEvent(clusterId, serverAddress, currentThread().getId()));
        }
    }

    public void release(final Connection connection) {
        if (!closed) {
            connectionPoolListener.connectionCheckedIn(new ConnectionEvent(clusterId, serverAddress));
        }
        pool.release(connection, connection.isClosed() || shouldPrune(connection));
    }

    public boolean hasWorked() {
        return hasWorked;
    }

    public void close() {
        if (!closed) {
            pool.close();
            if (sizeMaintenanceTimer != null) {
                sizeMaintenanceTimer.shutdownNow();
            }
            closed = true;
            connectionPoolListener.connectionPoolClosed(new ConnectionPoolEvent(clusterId, serverAddress));
        }
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
                        LOGGER.fine(format("Pruning pooled connections to %s", serverAddress));
                        pool.prune();
                    }
                    if (shouldEnsureMinSize()) {
                        LOGGER.fine(format("Ensuring minimum pooled connections to %s", serverAddress));
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
        } else {
            ScheduledExecutorService newTimer = Executors.newSingleThreadScheduledExecutor();
            newTimer.scheduleAtFixedRate(maintenanceTask, settings.getMaintenanceInitialDelay(MILLISECONDS),
                                         settings.getMaintenanceFrequency(MILLISECONDS), MILLISECONDS);
            return newTimer;
        }
    }

    private boolean shouldEnsureMinSize() {
        return settings.getMinSize() > 0;
    }

    private boolean shouldPrune() {
        return settings.getMaxConnectionIdleTime(MILLISECONDS) > 0 || settings.getMaxConnectionLifeTime(MILLISECONDS) > 0;
    }

    private boolean shouldPrune(final Connection connection) {
        return fromPreviousGeneration(connection) || pastMaxLifeTime(connection) || pastMaxIdleTime(connection);
    }

    private boolean pastMaxIdleTime(final Connection connection) {
        return expired(connection.getLastUsedAt(), System.currentTimeMillis(), settings.getMaxConnectionIdleTime(MILLISECONDS));
    }

    private boolean pastMaxLifeTime(final Connection connection) {
        return expired(connection.getOpenedAt(), System.currentTimeMillis(), settings.getMaxConnectionLifeTime(MILLISECONDS));
    }

    private boolean fromPreviousGeneration(final Connection connection) {
        return generation.get() > connection.getGeneration();
    }

    private boolean expired(final long startTime, final long curTime, final long maxTime) {
        return maxTime != 0 && curTime - startTime > maxTime;
    }

    public void invalidate() {
        generation.incrementAndGet();
    }

    private class ConnectionItemFactory implements ConcurrentPool.ItemFactory<Connection> {
        @Override
        public Connection create() {
            Connection connection = connectionFactory.create(serverAddress, PooledConnectionProvider.this, generation.get());
            LOGGER.fine(format("Opened connection to %s", serverAddress));
            connectionPoolListener.connectionAdded(new ConnectionEvent(clusterId, serverAddress));
            return connection;
        }

        @Override
        public void close(final Connection connection) {
            String reason;
            if (fromPreviousGeneration(connection)) {
                reason = "there was a socket exception raised on another connection from this pool";
            } else if (pastMaxLifeTime(connection)) {
                reason = "it is past its maximum allowed life time";
            } else if (pastMaxIdleTime(connection)) {
                reason = "it is past its maximum allowed idle time";
            } else {
                reason = "the pool has been closed";
            }
            if (!closed) {
                connectionPoolListener.connectionRemoved(new ConnectionEvent(clusterId, serverAddress));
            }
            connection.close();
            LOGGER.fine(format("Closed connection to %s because %s.", serverAddress, reason));
        }

        @Override
        public boolean shouldPrune(final Connection connection) {
            return PooledConnectionProvider.this.shouldPrune(connection);
        }
    }
}