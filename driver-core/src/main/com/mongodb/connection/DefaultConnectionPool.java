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

package com.mongodb.connection;

import com.mongodb.MongoException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.MongoWaitQueueFullException;
import com.mongodb.ServerAddress;
import com.mongodb.diagnostics.Loggers;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.event.ConnectionEvent;
import com.mongodb.event.ConnectionPoolEvent;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.ConnectionPoolOpenedEvent;
import com.mongodb.event.ConnectionPoolWaitQueueEvent;
import org.bson.ByteBuf;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class DefaultConnectionPool implements ConnectionPool {
    private static final Logger LOGGER = Loggers.getLogger("connection");

    private final ConcurrentPool<UsageTrackingInternalConnection> pool;
    private final ConnectionPoolSettings settings;
    private final AtomicInteger waitQueueSize = new AtomicInteger(0);
    private final AtomicInteger generation = new AtomicInteger(0);
    private final ExecutorService sizeMaintenanceTimer;
    private final String clusterId;
    private final ServerAddress serverAddress;
    private final Runnable maintenanceTask;
    private final ConnectionPoolListener connectionPoolListener;
    private volatile boolean closed;

    public DefaultConnectionPool(final String clusterId, final ServerAddress serverAddress,
                                 final InternalConnectionFactory internalConnectionFactory, final ConnectionPoolSettings settings,
                                 final ConnectionPoolListener connectionPoolListener) {
        this.clusterId = notNull("clusterId", clusterId);
        this.serverAddress = notNull("serverAddress", serverAddress);
        this.settings = notNull("settings", settings);
        UsageTrackingInternalConnectionItemFactory connectionItemFactory
        = new UsageTrackingInternalConnectionItemFactory(internalConnectionFactory);
        pool = new ConcurrentPool<UsageTrackingInternalConnection>(settings.getMaxSize(), connectionItemFactory);
        maintenanceTask = createMaintenanceTask();
        sizeMaintenanceTimer = createTimer();
        this.connectionPoolListener = notNull("connectionPoolListener", connectionPoolListener);
        connectionPoolListener.connectionPoolOpened(new ConnectionPoolOpenedEvent(clusterId, serverAddress, settings));
    }

    @Override
    public InternalConnection get() {
        return get(settings.getMaxWaitTime(MILLISECONDS), MILLISECONDS);
    }

    @Override
    public InternalConnection get(final long timeout, final TimeUnit timeUnit) {
        try {
            if (waitQueueSize.incrementAndGet() > settings.getMaxWaitQueueSize()) {
                throw new MongoWaitQueueFullException(format("Too many threads are already waiting for a connection. "
                                                             + "Max number of threads (maxWaitQueueSize) of %d has been exceeded.",
                                                             settings.getMaxWaitQueueSize()));
            }
            connectionPoolListener.waitQueueEntered(new ConnectionPoolWaitQueueEvent(clusterId, serverAddress, currentThread().getId()));
            UsageTrackingInternalConnection internalConnection = pool.get(timeout, timeUnit);
            while (shouldPrune(internalConnection)) {
                pool.release(internalConnection, true);
                internalConnection = pool.get(timeout, timeUnit);
            }
            connectionPoolListener.connectionCheckedOut(new ConnectionEvent(clusterId, serverAddress, internalConnection.getId()));
            LOGGER.trace(format("Checked out connection [%s] to server %s", internalConnection.getId(), serverAddress));
            return new PooledConnection(internalConnection);
        } finally {
            waitQueueSize.decrementAndGet();
            connectionPoolListener.waitQueueExited(new ConnectionPoolWaitQueueEvent(clusterId, serverAddress, currentThread().getId()));
        }
    }

    @Override
    public void invalidate() {
        generation.incrementAndGet();
    }

    @Override
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

    ConcurrentPool<UsageTrackingInternalConnection> getPool() {
        return pool;
    }

    private Runnable createMaintenanceTask() {
        Runnable newMaintenanceTask = null;
        if (shouldPrune() || shouldEnsureMinSize()) {
            newMaintenanceTask = new Runnable() {
                @Override
                public synchronized void run() {
                    if (shouldPrune()) {
                        LOGGER.debug(format("Pruning pooled connections to %s", serverAddress));
                        pool.prune();
                    }
                    if (shouldEnsureMinSize()) {
                        LOGGER.debug(format("Ensuring minimum pooled connections to %s", serverAddress));
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

    private boolean shouldPrune(final UsageTrackingInternalConnection connection) {
        return fromPreviousGeneration(connection) || pastMaxLifeTime(connection) || pastMaxIdleTime(connection);
    }

    private boolean pastMaxIdleTime(final UsageTrackingInternalConnection connection) {
        return expired(connection.getLastUsedAt(), System.currentTimeMillis(), settings.getMaxConnectionIdleTime(MILLISECONDS));
    }

    private boolean pastMaxLifeTime(final UsageTrackingInternalConnection connection) {
        return expired(connection.getOpenedAt(), System.currentTimeMillis(), settings.getMaxConnectionLifeTime(MILLISECONDS));
    }

    private boolean fromPreviousGeneration(final UsageTrackingInternalConnection connection) {
        return generation.get() > connection.getGeneration();
    }

    private boolean expired(final long startTime, final long curTime, final long maxTime) {
        return maxTime != 0 && curTime - startTime > maxTime;
    }

    /**
     * If there was a socket exception that wasn't some form of interrupted read, increment the generation count so that any connections
     * created prior will be discarded.
     *
     * @param connection the connection that generated the exception
     * @param e          the exception
     */
    private void incrementGenerationOnSocketException(final InternalConnection connection, final MongoException e) {
        if (e instanceof MongoSocketException && !(e instanceof MongoSocketReadTimeoutException)) {
            LOGGER.warn(format("Got socket exception on connection [%s] to %s. All connections to %s will be closed.",
                               connection.getId(), serverAddress, serverAddress));
            invalidate();
        }
    }

    private class PooledConnection implements InternalConnection {
        private volatile UsageTrackingInternalConnection wrapped;

        public PooledConnection(final UsageTrackingInternalConnection wrapped) {
            this.wrapped = notNull("wrapped", wrapped);
        }

        @Override
        public void close() {
            if (wrapped != null) {
                if (!closed) {
                    connectionPoolListener.connectionCheckedIn(new ConnectionEvent(clusterId, wrapped.getServerAddress(), wrapped.getId()));
                    LOGGER.trace(format("Checked in connection [%s] to server %s", getId(), serverAddress));
                }
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
        public ByteBuf getBuffer(final int capacity) {
            return wrapped.getBuffer(capacity);
        }

        @Override
        public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
            isTrue("open", wrapped != null);
            try {
                wrapped.sendMessage(byteBuffers, lastRequestId);
            } catch (MongoException e) {
                incrementGenerationOnSocketException(this, e);
                throw e;
            }
        }

        @Override
        public ResponseBuffers receiveMessage() {
            isTrue("open", wrapped != null);
            try {
                return wrapped.receiveMessage();
            } catch (MongoException e) {
                incrementGenerationOnSocketException(this, e);
                throw e;
            }
        }

        @Override
        public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
            isTrue("open", wrapped != null);
            wrapped.sendMessageAsync(byteBuffers, lastRequestId, callback);
        }

        @Override
        public void receiveMessageAsync(final SingleResultCallback<ResponseBuffers> callback) {
            isTrue("open", wrapped != null);
            wrapped.receiveMessageAsync(callback);
        }

        @Override
        public String getId() {
            return wrapped.getId();
        }
    }

    private class UsageTrackingInternalConnectionItemFactory implements ConcurrentPool.ItemFactory<UsageTrackingInternalConnection> {
        private final InternalConnectionFactory internalConnectionFactory;

        public UsageTrackingInternalConnectionItemFactory(final InternalConnectionFactory internalConnectionFactory) {
            this.internalConnectionFactory = internalConnectionFactory;
        }

        @Override
        public UsageTrackingInternalConnection create() {
            UsageTrackingInternalConnection internalConnection =
            new UsageTrackingInternalConnection(internalConnectionFactory.create(serverAddress), generation.get());
            LOGGER.info(format("Opened connection [%s] to %s", internalConnection.getId(), serverAddress));
            connectionPoolListener.connectionAdded(new ConnectionEvent(clusterId, serverAddress, internalConnection.getId()));
            return internalConnection;
        }

        @Override
        public void close(final UsageTrackingInternalConnection connection) {
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
                connectionPoolListener.connectionRemoved(new ConnectionEvent(clusterId, serverAddress, connection.getId()));
            }
            LOGGER.info(format("Closed connection [%s] to %s because %s.", connection.getId(), serverAddress, reason));
            connection.close();
        }

        @Override
        public boolean shouldPrune(final UsageTrackingInternalConnection usageTrackingConnection) {
            return DefaultConnectionPool.this.shouldPrune(usageTrackingConnection);
        }
    }
}
