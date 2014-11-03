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
import com.mongodb.MongoInternalException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.MongoWaitQueueFullException;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
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
    private final Runnable maintenanceTask;
    private final ConnectionPoolListener connectionPoolListener;
    private final ServerId serverId;
    private volatile boolean closed;

    public DefaultConnectionPool(final ServerId serverId,
                                 final InternalConnectionFactory internalConnectionFactory, final ConnectionPoolSettings settings,
                                 final ConnectionPoolListener connectionPoolListener) {
        this.serverId = notNull("serverId", serverId);
        this.settings = notNull("settings", settings);
        UsageTrackingInternalConnectionItemFactory connectionItemFactory
        = new UsageTrackingInternalConnectionItemFactory(internalConnectionFactory);
        pool = new ConcurrentPool<UsageTrackingInternalConnection>(settings.getMaxSize(), connectionItemFactory);
        maintenanceTask = createMaintenanceTask();
        sizeMaintenanceTimer = createTimer();
        this.connectionPoolListener = notNull("connectionPoolListener", connectionPoolListener);
        connectionPoolListener.connectionPoolOpened(new ConnectionPoolOpenedEvent(serverId, settings));
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
            connectionPoolListener.waitQueueEntered(new ConnectionPoolWaitQueueEvent(serverId, currentThread().getId()));
            UsageTrackingInternalConnection internalConnection = pool.get(timeout, timeUnit);
            while (shouldPrune(internalConnection)) {
                pool.release(internalConnection, true);
                internalConnection = pool.get(timeout, timeUnit);
            }
            if (!internalConnection.opened()) {
                try {
                    internalConnection.open();
                } catch (Throwable t) {
                    pool.release(internalConnection, true);
                    if (t instanceof MongoException) {
                        throw (MongoException) t;
                    } else {
                        throw new MongoInternalException(t.toString(), t);
                    }
                }
            }
            connectionPoolListener.connectionCheckedOut(new ConnectionEvent(getId(internalConnection)));
            LOGGER.trace(format("Checked out connection [%s] to server %s", getId(internalConnection), serverId.getAddress()));
            return new PooledConnection(internalConnection);
        } finally {
            waitQueueSize.decrementAndGet();
            connectionPoolListener.waitQueueExited(new ConnectionPoolWaitQueueEvent(serverId, currentThread().getId()));
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
            connectionPoolListener.connectionPoolClosed(new ConnectionPoolEvent(serverId));
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
                        LOGGER.debug(format("Pruning pooled connections to %s", serverId.getAddress()));
                        pool.prune();
                    }
                    if (shouldEnsureMinSize()) {
                        LOGGER.debug(format("Ensuring minimum pooled connections to %s", serverId.getAddress()));
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
                               getId(connection), serverId.getAddress(), serverId.getAddress()));
            invalidate();
        }
    }

    private ConnectionId getId(final InternalConnection internalConnection) {
        return internalConnection.getDescription().getConnectionId();
    }

    private class PooledConnection implements InternalConnection {
        private volatile UsageTrackingInternalConnection wrapped;

        public PooledConnection(final UsageTrackingInternalConnection wrapped) {
            this.wrapped = notNull("wrapped", wrapped);
        }

        @Override
        public void open() {
            isTrue("open", wrapped != null);
            wrapped.open();
        }

        @Override
        public MongoFuture<Void> openAsync() {
            isTrue("open", wrapped != null);
            return wrapped.openAsync();
        }

        @Override
        public void close() {
            if (wrapped != null) {
                if (!closed) {
                    connectionPoolListener.connectionCheckedIn(new ConnectionEvent(getId(wrapped)));
                    LOGGER.trace(format("Checked in connection [%s] to server %s", getId(wrapped), serverId.getAddress()));
                }
                pool.release(wrapped, wrapped.isClosed() || shouldPrune(wrapped));
                wrapped = null;
            }
        }

        @Override
        public boolean opened() {
            isTrue("open", wrapped != null);
            return wrapped.opened();
        }

        @Override
        public boolean isClosed() {
            return wrapped == null || wrapped.isClosed();
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
        public ResponseBuffers receiveMessage(final int responseTo) {
            isTrue("open", wrapped != null);
            try {
                return wrapped.receiveMessage(responseTo);
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
        public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
            isTrue("open", wrapped != null);
            wrapped.receiveMessageAsync(responseTo, callback);
        }

        @Override
        public ConnectionDescription getDescription() {
            isTrue("open", wrapped != null);
            return wrapped.getDescription();
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
            new UsageTrackingInternalConnection(internalConnectionFactory.create(serverId), generation.get());
            connectionPoolListener.connectionAdded(new ConnectionEvent(getId(internalConnection)));
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
                connectionPoolListener.connectionRemoved(new ConnectionEvent(getId(connection)));
            }
            LOGGER.info(format("Closed connection [%s] to %s because %s.", getId(connection), serverId.getAddress(), reason));
            connection.close();
        }

        @Override
        public boolean shouldPrune(final UsageTrackingInternalConnection usageTrackingConnection) {
            return DefaultConnectionPool.this.shouldPrune(usageTrackingConnection);
        }
    }
}
