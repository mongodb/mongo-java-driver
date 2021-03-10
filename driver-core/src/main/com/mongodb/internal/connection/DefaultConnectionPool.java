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

import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.annotations.Immutable;
import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.ConnectionCheckOutFailedEvent;
import com.mongodb.event.ConnectionCheckOutFailedEvent.Reason;
import com.mongodb.event.ConnectionCheckOutStartedEvent;
import com.mongodb.event.ConnectionCheckedInEvent;
import com.mongodb.event.ConnectionCheckedOutEvent;
import com.mongodb.event.ConnectionClosedEvent;
import com.mongodb.event.ConnectionCreatedEvent;
import com.mongodb.event.ConnectionPoolClearedEvent;
import com.mongodb.event.ConnectionPoolClosedEvent;
import com.mongodb.event.ConnectionPoolCreatedEvent;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.ConnectionReadyEvent;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.connection.ConcurrentPool.Prune;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.thread.DaemonThreadFactory;
import com.mongodb.lang.Nullable;
import org.bson.ByteBuf;
import org.bson.codecs.Decoder;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.event.EventListenerHelper.getConnectionPoolListener;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@SuppressWarnings("deprecation")
class DefaultConnectionPool implements ConnectionPool {
    private static final Logger LOGGER = Loggers.getLogger("connection");
    private static final int MAX_CONNECTING = 2;

    private final ConcurrentPool<UsageTrackingInternalConnection> pool;
    private final ConnectionPoolSettings settings;
    private final AtomicInteger generation = new AtomicInteger(0);
    private final AtomicInteger lastPrunedGeneration = new AtomicInteger(0);
    private final ScheduledExecutorService sizeMaintenanceTimer;
    private ExecutorService asyncGetter;
    private final Runnable maintenanceTask;
    private final ConnectionPoolListener connectionPoolListener;
    private final ServerId serverId;
    private volatile boolean closed;
    private final OpenConcurrencyLimiter openConcurrencyLimiter;

    DefaultConnectionPool(final ServerId serverId, final InternalConnectionFactory internalConnectionFactory,
                          final ConnectionPoolSettings settings) {
        this.serverId = notNull("serverId", serverId);
        this.settings = notNull("settings", settings);
        UsageTrackingInternalConnectionItemFactory connectionItemFactory =
                new UsageTrackingInternalConnectionItemFactory(internalConnectionFactory);
        pool = new ConcurrentPool<UsageTrackingInternalConnection>(settings.getMaxSize(), connectionItemFactory);
        this.connectionPoolListener = getConnectionPoolListener(settings);
        maintenanceTask = createMaintenanceTask();
        sizeMaintenanceTimer = createMaintenanceTimer();
        connectionPoolCreated(connectionPoolListener, serverId, settings);
        openConcurrencyLimiter = new OpenConcurrencyLimiter();
    }

    @Override
    public void start() {
        if (sizeMaintenanceTimer != null) {
            sizeMaintenanceTimer.scheduleAtFixedRate(maintenanceTask, settings.getMaintenanceInitialDelay(MILLISECONDS),
                    settings.getMaintenanceFrequency(MILLISECONDS), MILLISECONDS);
        }
    }

    @Override
    public InternalConnection get() {
        return get(settings.getMaxWaitTime(MILLISECONDS), MILLISECONDS);
    }

    @Override
    public InternalConnection get(final long timeoutValue, final TimeUnit timeUnit) {
        Timeout timeout = Timeout.startNow(timeoutValue, timeUnit);
        PooledConnection pooledConnection;
        try {
            connectionPoolListener.connectionCheckOutStarted(new ConnectionCheckOutStartedEvent(serverId));
            pooledConnection = getPooledConnection(timeout);
        } catch (Throwable t) {
            checkOutFailed(t, null);
            throw t;
        }
        if (!pooledConnection.opened()) {
            pooledConnection = openConcurrencyLimiter.openOrGetAvailableAndPruneSpecified(pooledConnection, true, timeout);
        }
        connectionPoolListener.connectionCheckedOut(
                new ConnectionCheckedOutEvent(pooledConnection.getDescription().getConnectionId()));

        return pooledConnection;
    }

    @Override
    public void getAsync(final SingleResultCallback<InternalConnection> callback) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Asynchronously getting a connection from the pool for server %s", serverId));
        }

        final SingleResultCallback<InternalConnection> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        PooledConnection connection = null;

        try {
            connectionPoolListener.connectionCheckOutStarted(new ConnectionCheckOutStartedEvent(serverId));
            connection = getPooledConnection(Timeout.IMMEDIATE);
        } catch (MongoTimeoutException e) {
            // fall through
        } catch (Throwable t) {
            checkOutFailed(t, null);
            callback.onResult(null, t);
            return;
        }

        if (connection != null) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("Asynchronously opening pooled connection %s to server %s",
                                           connection.getDescription().getConnectionId(), serverId));
            }
            openAsync(connection, errHandlingCallback);
        } else {
            final Timeout timeout = Timeout.startNow(settings.getMaxWaitTime(NANOSECONDS));
            getAsyncGetter().submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (timeout.remainingNanos() <= 0) {
                            errHandlingCallback.onResult(null, createTimeoutException());
                        } else {
                            PooledConnection connection = getPooledConnection(timeout);
                            openAsync(connection, errHandlingCallback);
                        }
                    } catch (MongoTimeoutException e) {
                        Exception exception = new MongoTimeoutException(format("Timeout waiting for a pooled connection after %d %s",
                                settings.getMaxWaitTime(MILLISECONDS), MILLISECONDS));
                        checkOutFailed(exception, null);
                        errHandlingCallback.onResult(null, exception);
                    } catch (Throwable t) {
                        checkOutFailed(t, null);
                        errHandlingCallback.onResult(null, t);
                    }
                }
            });
        }
    }

    /**
     * Must not throw {@link Exception}s.
     */
    private void checkOutFailed(@Nullable final Throwable t, @Nullable final Reason reason) {
        if (t instanceof MongoTimeoutException) {
            connectionPoolListener.connectionCheckOutFailed(new ConnectionCheckOutFailedEvent(serverId, Reason.TIMEOUT));
        } else if (t instanceof IllegalStateException && t.getMessage().equals("The pool is closed")) {
            connectionPoolListener.connectionCheckOutFailed(new ConnectionCheckOutFailedEvent(serverId, Reason.POOL_CLOSED));
        } else {
            connectionPoolListener.connectionCheckOutFailed(
                    new ConnectionCheckOutFailedEvent(serverId, reason == null ? Reason.UNKNOWN : reason));
        }
    }

    private void openAsync(final PooledConnection pooledConnection,
                           final SingleResultCallback<InternalConnection> callback) {
        if (pooledConnection.opened()) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("Pooled connection %s to server %s is already open",
                                           pooledConnection.getDescription().getConnectionId(), serverId));
            }
            connectionPoolListener.connectionCheckedOut(
                    new ConnectionCheckedOutEvent(pooledConnection.getDescription().getConnectionId()));
            callback.onResult(pooledConnection, null);
        } else {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("Pooled connection %s to server %s is not yet open",
                                           pooledConnection.getDescription().getConnectionId(), serverId));
            }
            pooledConnection.openAsync(new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    if (t != null) {
                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.trace(format("Pooled connection %s to server %s failed to open",
                                                       pooledConnection.getDescription().getConnectionId(), serverId));
                        }
                        checkOutFailed(null, Reason.CONNECTION_ERROR);
                        pool.release(pooledConnection.wrapped, true);
                    } else {
                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.trace(format("Pooled connection %s to server %s is now open",
                                                       pooledConnection.getDescription().getConnectionId(), serverId));
                        }
                        connectionPoolListener.connectionCheckedOut(
                                new ConnectionCheckedOutEvent(pooledConnection.getDescription().getConnectionId()));
                        callback.onResult(pooledConnection, null);
                    }
                }
            });
        }
    }

    private synchronized ExecutorService getAsyncGetter() {
        if (asyncGetter == null) {
            asyncGetter = Executors.newSingleThreadExecutor(new DaemonThreadFactory("AsyncGetter"));
        }
        return asyncGetter;
    }

    private synchronized void shutdownAsyncGetter() {
        if (asyncGetter != null) {
            asyncGetter.shutdownNow();
        }
    }

    @Override
    public void invalidate() {
        LOGGER.debug("Invalidating the connection pool");
        generation.incrementAndGet();
        connectionPoolListener.connectionPoolCleared(new ConnectionPoolClearedEvent(serverId));
    }

    @Override
    public void close() {
        if (!closed) {
            pool.close();
            if (sizeMaintenanceTimer != null) {
                sizeMaintenanceTimer.shutdownNow();
            }
            shutdownAsyncGetter();
            closed = true;
            connectionPoolListener.connectionPoolClosed(new ConnectionPoolClosedEvent(serverId));
        }
    }

    @Override
    public int getGeneration() {
        return generation.get();
    }

    /**
     * Synchronously prune idle connections and ensure the minimum pool size.
     */
    public void doMaintenance() {
        if (maintenanceTask != null) {
            maintenanceTask.run();
        }
    }

    private PooledConnection getPooledConnection(final Timeout timeout) throws MongoTimeoutException {
        boolean externalTimeoutException = false;
        try {
            UsageTrackingInternalConnection internalConnection = pool.get(timeout.remainingNanos(), TimeUnit.NANOSECONDS);
            while (shouldPrune(internalConnection)) {
                pool.release(internalConnection, true);
                long remainingNanos = timeout.remainingNanos();
                if (remainingNanos <= 0) {
                    externalTimeoutException = true;
                    throw createTimeoutException();
                }
                internalConnection = pool.get(remainingNanos, TimeUnit.NANOSECONDS);
            }
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("Got connection [%s] to server %s", getId(internalConnection), serverId.getAddress()));
            }
            return new PooledConnection(internalConnection);
        } catch (MongoTimeoutException externalOrInternalTimeoutException) {
            if (externalTimeoutException) {
                throw externalOrInternalTimeoutException;
            } else {
                MongoTimeoutException timeoutException = createTimeoutException();
                timeoutException.addSuppressed(externalOrInternalTimeoutException);
                throw timeoutException;
            }
        }
    }

    private MongoTimeoutException createTimeoutException() {
        return new MongoTimeoutException(format("Timed out after %d ms while waiting for a connection to server %s.",
                                                settings.getMaxWaitTime(MILLISECONDS), serverId.getAddress()));
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
                    try {
                        int curGeneration = generation.get();
                        if (shouldPrune() || curGeneration > lastPrunedGeneration.get()) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(format("Pruning pooled connections to %s", serverId.getAddress()));
                            }
                            pool.prune();
                        }
                        lastPrunedGeneration.set(curGeneration);
                        if (shouldEnsureMinSize()) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(format("Ensuring minimum pooled connections to %s", serverId.getAddress()));
                            }
                            pool.ensureMinSize(settings.getMinSize(), newConnection -> {
                                if (!newConnection.opened()) {
                                    openConcurrencyLimiter.openOrGetAvailableAndPruneSpecified(
                                            new PooledConnection(newConnection), false, Timeout.IMMEDIATE);
                                }
                                return Optional.of(newConnection);
                            });
                        }
                    } catch (MongoInterruptedException | MongoTimeoutException e) {
                        //complete the maintenance task
                    } catch (Exception e) {
                        LOGGER.warn("Exception thrown during connection pool background maintenance task", e);
                    }
                }
            };
        }
        return newMaintenanceTask;
    }

    private ScheduledExecutorService createMaintenanceTimer() {
        if (maintenanceTask == null) {
            return null;
        } else {
            return Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("MaintenanceTimer"));
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
     * Send both current and deprecated events in order to preserve backwards compatibility.
     * Must not throw {@link Exception}s.
     */
    private void connectionPoolCreated(final ConnectionPoolListener connectionPoolListener, final ServerId serverId,
                                             final ConnectionPoolSettings settings) {
        connectionPoolListener.connectionPoolCreated(new ConnectionPoolCreatedEvent(serverId, settings));
        connectionPoolListener.connectionPoolOpened(new com.mongodb.event.ConnectionPoolOpenedEvent(serverId, settings));
    }

    /**
     * Send both current and deprecated events in order to preserve backwards compatibility.
     * Must not throw {@link Exception}s.
     */
    private void connectionCreated(final ConnectionPoolListener connectionPoolListener, final ConnectionId connectionId) {
        connectionPoolListener.connectionAdded(new com.mongodb.event.ConnectionAddedEvent(connectionId));
        connectionPoolListener.connectionCreated(new ConnectionCreatedEvent(connectionId));
    }

    /**
     * Send both current and deprecated events in order to preserve backwards compatibility.
     * Must not throw {@link Exception}s.
     */
    private void connectionClosed(final ConnectionPoolListener connectionPoolListener, final ConnectionId connectionId,
                                  final ConnectionClosedEvent.Reason reason) {
        connectionPoolListener.connectionRemoved(new com.mongodb.event.ConnectionRemovedEvent(connectionId, getReasonForRemoved(reason)));
        connectionPoolListener.connectionClosed(new ConnectionClosedEvent(connectionId, reason));
    }

    private com.mongodb.event.ConnectionRemovedEvent.Reason getReasonForRemoved(final ConnectionClosedEvent.Reason reason) {
        com.mongodb.event.ConnectionRemovedEvent.Reason removedReason = com.mongodb.event.ConnectionRemovedEvent.Reason.UNKNOWN;
        switch (reason) {
            case STALE:
                removedReason = com.mongodb.event.ConnectionRemovedEvent.Reason.STALE;
                break;
            case IDLE:
                removedReason = com.mongodb.event.ConnectionRemovedEvent.Reason.MAX_IDLE_TIME_EXCEEDED;
                break;
            case ERROR:
                removedReason = com.mongodb.event.ConnectionRemovedEvent.Reason.ERROR;
                break;
            case POOL_CLOSED:
                removedReason = com.mongodb.event.ConnectionRemovedEvent.Reason.POOL_CLOSED;
                break;
            default:
                break;
        }
        return removedReason;
    }

    private ConnectionId getId(final InternalConnection internalConnection) {
        return internalConnection.getDescription().getConnectionId();
    }

    private class PooledConnection implements InternalConnection {
        private final UsageTrackingInternalConnection wrapped;
        private final AtomicBoolean isClosed = new AtomicBoolean();

        PooledConnection(final UsageTrackingInternalConnection wrapped) {
            this.wrapped = notNull("wrapped", wrapped);
        }

        @Override
        public int getGeneration() {
            return wrapped.getGeneration();
        }

        @Override
        public void open() {
            isTrue("open", !isClosed.get());
            wrapped.open();
            connectionPoolListener.connectionReady(new ConnectionReadyEvent(getDescription().getConnectionId()));
        }

        @Override
        public void openAsync(final SingleResultCallback<Void> callback) {
            isTrue("open", !isClosed.get());
            wrapped.openAsync(new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    if (t != null) {
                        checkOutFailed(null, Reason.CONNECTION_ERROR);
                        callback.onResult(null, t);
                    } else {
                        connectionPoolListener.connectionReady(new ConnectionReadyEvent(getDescription().getConnectionId()));
                        callback.onResult(result, null);
                    }
                }
            });
        }

        @Override
        public void close() {
            // All but the first call is a no-op
            if (!isClosed.getAndSet(true)) {
                connectionPoolListener.connectionCheckedIn(new ConnectionCheckedInEvent(getId(wrapped)));
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(format("Checked in connection [%s] to server %s", getId(wrapped), serverId.getAddress()));
                }
                boolean success = false;
                try {
                    if (wrapped.isClosed() || shouldPrune(wrapped)) {
                        pool.release(wrapped, true);
                    } else if (!openConcurrencyLimiter.tryHandOver(new PooledConnection(wrapped))) {
                        pool.release(wrapped);
                    }
                    success = true;
                } finally {
                    if (!success) {
                        pool.release(wrapped, true);
                    }
                }
            }
        }

        /**
         * Close this connection without sending a {@link ConnectionClosedEvent}.
         * This method must be used if and only if {@link ConnectionCreatedEvent} was not emitted for the connection.
         */
        void closeSilently() {
            try {
                wrapped.setCloseSilently();
            } finally {
                pool.release(wrapped, true);
            }
        }

        @Override
        public boolean opened() {
            isTrue("open", !isClosed.get());
            return wrapped.opened();
        }

        @Override
        public boolean isClosed() {
            return isClosed.get() || wrapped.isClosed();
        }

        @Override
        public ByteBuf getBuffer(final int capacity) {
            return wrapped.getBuffer(capacity);
        }

        @Override
        public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
            isTrue("open", !isClosed.get());
            wrapped.sendMessage(byteBuffers, lastRequestId);
        }

        @Override
        public <T> T sendAndReceive(final CommandMessage message, final Decoder<T> decoder, final SessionContext sessionContext) {
            isTrue("open", !isClosed.get());
            return wrapped.sendAndReceive(message, decoder, sessionContext);
        }

        @Override
        public <T> void send(final CommandMessage message, final Decoder<T> decoder, final SessionContext sessionContext) {
            isTrue("open", !isClosed.get());
            wrapped.send(message, decoder, sessionContext);
        }

        @Override
        public <T> T receive(final Decoder<T> decoder, final SessionContext sessionContext) {
            isTrue("open", !isClosed.get());
            return wrapped.receive(decoder, sessionContext);
        }

        @Override
        public boolean supportsAdditionalTimeout() {
            isTrue("open", !isClosed.get());
            return wrapped.supportsAdditionalTimeout();
        }

        @Override
        public <T> T receive(final Decoder<T> decoder, final SessionContext sessionContext, final int additionalTimeout) {
            isTrue("open", !isClosed.get());
            return wrapped.receive(decoder, sessionContext, additionalTimeout);
        }

        @Override
        public boolean hasMoreToCome() {
            isTrue("open", !isClosed.get());
            return wrapped.hasMoreToCome();
        }

        @Override
        public <T> void sendAndReceiveAsync(final CommandMessage message, final Decoder<T> decoder,
                                            final SessionContext sessionContext, final SingleResultCallback<T> callback) {
            isTrue("open", !isClosed.get());
            wrapped.sendAndReceiveAsync(message, decoder, sessionContext, new SingleResultCallback<T>() {
                @Override
                public void onResult(final T result, final Throwable t) {
                    callback.onResult(result, t);
                }
            });
        }

        @Override
        public ResponseBuffers receiveMessage(final int responseTo) {
            isTrue("open", !isClosed.get());
            return wrapped.receiveMessage(responseTo);
        }

        @Override
        public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
            isTrue("open", !isClosed.get());
            wrapped.sendMessageAsync(byteBuffers, lastRequestId, new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    callback.onResult(null, t);
                }
            });
        }

        @Override
        public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
            isTrue("open", !isClosed.get());
            wrapped.receiveMessageAsync(responseTo, new SingleResultCallback<ResponseBuffers>() {
                @Override
                public void onResult(final ResponseBuffers result, final Throwable t) {
                    callback.onResult(result, t);
                }
            });
        }

        @Override
        public ConnectionDescription getDescription() {
            return wrapped.getDescription();
        }

        @Override
        public ServerDescription getInitialServerDescription() {
            isTrue("open", !isClosed.get());
            return wrapped.getInitialServerDescription();
        }
    }

    private class UsageTrackingInternalConnectionItemFactory implements ConcurrentPool.ItemFactory<UsageTrackingInternalConnection> {
        private final InternalConnectionFactory internalConnectionFactory;

        UsageTrackingInternalConnectionItemFactory(final InternalConnectionFactory internalConnectionFactory) {
            this.internalConnectionFactory = internalConnectionFactory;
        }

        @Override
        public UsageTrackingInternalConnection create() {
            return new UsageTrackingInternalConnection(internalConnectionFactory.create(serverId), generation.get());
        }

        @Override
        public void close(final UsageTrackingInternalConnection connection) {
            try {
                if (connection.isCloseSilently()) {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace(format("Silently closed connection [%s] to server %s", getId(connection), serverId.getAddress()));
                    }
                } else {
                    connectionClosed(connectionPoolListener, getId(connection), getReasonForClosing(connection));
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info(format("Closed connection [%s] to %s because %s.", getId(connection), serverId.getAddress(),
                                getReasonStringForClosing(connection)));
                    }
                }
            } finally {
                connection.close();
            }
        }

        private String getReasonStringForClosing(final UsageTrackingInternalConnection connection) {
            String reason;
            if (connection.isClosed()) {
                reason = "there was a socket exception raised by this connection";
            } else if (fromPreviousGeneration(connection)) {
                reason = "there was a socket exception raised on another connection from this pool";
            } else if (pastMaxLifeTime(connection)) {
                reason = "it is past its maximum allowed life time";
            } else if (pastMaxIdleTime(connection)) {
                reason = "it is past its maximum allowed idle time";
            } else {
                reason = "the pool has been closed";
            }
            return reason;
        }

        private ConnectionClosedEvent.Reason getReasonForClosing(final UsageTrackingInternalConnection connection) {
            ConnectionClosedEvent.Reason reason;
            if (connection.isClosed()) {
                reason = ConnectionClosedEvent.Reason.ERROR;
            } else if (fromPreviousGeneration(connection)) {
                reason = ConnectionClosedEvent.Reason.STALE;
            } else if (pastMaxIdleTime(connection)) {
                reason = ConnectionClosedEvent.Reason.IDLE;
            } else {
                reason = ConnectionClosedEvent.Reason.POOL_CLOSED;
            }
            return reason;
        }

        @Override
        public Prune shouldPrune(final UsageTrackingInternalConnection usageTrackingConnection) {
            return DefaultConnectionPool.this.shouldPrune(usageTrackingConnection) ? Prune.YES : Prune.NO;
        }
    }

    /**
     * A <a href="https://docs.oracle.com/javase/8/docs/api/java/lang/doc-files/ValueBased.html">value-based</a> class
     * useful for tracking timeouts.
     */
    @Immutable
    private static final class Timeout {
        static final Timeout INFINITE = new Timeout(-1, 0);
        static final Timeout IMMEDIATE = new Timeout(0, 0);
        /**
         * {@link Long#MAX_VALUE} / 2.
         */
        static final long MAX_DURATION_NANOS = Long.MAX_VALUE / 2;

        private final long durationNanos;
        private final long startNanos;

        private Timeout(final long durationNanos, final long startNanos) {
            assert durationNanos <= MAX_DURATION_NANOS : durationNanos;
            this.durationNanos = durationNanos;
            this.startNanos = startNanos;
        }

        /**
         * @see #startNow(long)
         */
        static Timeout startNow(final long duration, final TimeUnit unit) {
            return startNow(unit.toNanos(duration));
        }

        /**
         * Returns an object equal to {@link #INFINITE} if {@code durationNanos} is either negative
         * or is greater than {@link #MAX_DURATION_NANOS},
         * equal to {@link #IMMEDIATE} if {@code durationNanos} is 0,
         * otherwise an object that represents the specified {@code durationNanos}.
         */
        static Timeout startNow(final long durationNanos) {
            if (durationNanos < 0 || MAX_DURATION_NANOS < durationNanos) {
                return INFINITE;
            } else if (durationNanos == 0) {
                return IMMEDIATE;
            } else {
                return new Timeout(durationNanos, System.nanoTime());
            }
        }

        /**
         * Returns 0 for objects equal to {@link #INFINITE}/{@link #IMMEDIATE}, 0 or a positive value for others.
         */
        private long elapsedNanos() {
            return durationNanos <= 0 ? 0 : System.nanoTime() - startNanos;
        }

        /**
         * Returns 0 for objects equal to {@link #INFINITE}/{@link #IMMEDIATE}, 0 or a positive value for others.
         */
        long remainingNanos() {
            return durationNanos <= 0 ? 0 : Math.max(0, durationNanos - elapsedNanos());
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Timeout other = (Timeout) o;
            return durationNanos == other.durationNanos && startNanos == other.startNanos;
        }

        @Override
        public int hashCode() {
            return Objects.hash(durationNanos, startNanos);
        }
    }

    /**
     * Package-private methods are thread-safe,
     * and only they should be called outside of the {@link OpenConcurrencyLimiter}'s code.
     */
    @ThreadSafe
    private final class OpenConcurrencyLimiter {
        private final Lock lock;
        private final Condition condition;
        private int permits;
        private final Deque<MutableReference<PooledConnection>> desiredConnectionSlots;

        OpenConcurrencyLimiter() {
            lock = new ReentrantLock(true);
            condition = lock.newCondition();
            permits = MAX_CONNECTING;
            desiredConnectionSlots = new LinkedList<>();
        }

        /**
         * Returns either {@linkplain InternalConnection#opened() opened} {@code connection} that is specified,
         * or a different opened connection if {@code tryGetAvailable} is {@code true} and
         * one becomes available while waiting for a permit to open the specified {@code connection}.
         * If a different connection is returned, then the specified {@code connection} is
         * {@linkplain PooledConnection#closeSilently() silently closed}.
         * This method emits {@link ConnectionCreatedEvent} if and only if it acquires a permit to open the specified {@code connection}.
         * @throws MongoTimeoutException If timed out.
         */
        PooledConnection openOrGetAvailableAndPruneSpecified(
                final PooledConnection connection, final boolean tryGetAvailable, final Timeout timeout)
                throws MongoTimeoutException {
            PooledConnection availableConnection;
            try {
                availableConnection = acquirePermitOrGetAvailableOpenConnection(tryGetAvailable, timeout);
            } catch (RuntimeException e) {
                checkOutFailed(e, null);
                throw e;
            }
            if (availableConnection != null) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(format("Received opened connection [%s] to server %s", getId(availableConnection), serverId.getAddress()));
                }
                connection.closeSilently();
                return availableConnection;
            } else {//acquired a permit
                try {
                    connectionCreated(//a connection is considered created only when it is ready to be open
                            connectionPoolListener, getId(connection));
                    open(connection);
                } finally {
                    releasePermit();
                }
                return connection;
            }
        }

        /**
         * @return Either {@code null} if a permit has been acquired, or a {@link PooledConnection}
         * if {@code tryGetAvailable} is {@code true} and an {@linkplain PooledConnection#opened() opened} one becomes available while
         * waiting for a permit.
         * @throws MongoTimeoutException If timed out.
         */
        @Nullable
        private PooledConnection acquirePermitOrGetAvailableOpenConnection(final boolean tryGetAvailable, final Timeout timeout)
                throws MongoTimeoutException {
            PooledConnection availableConnection = null;
            tryLock(timeout);
            try {
                if (tryGetAvailable) {
                    addLastDesiredConnectionSlot();
                }
                long remainingNanos = timeout.remainingNanos();
                while (permits == 0
                        && (availableConnection = tryGetAvailable ? tryGetAndRemoveFirstDesiredConnectionSlot() : null) == null) {
                    if (remainingNanos <= 0L) {
                        throw createTimeoutException();
                    }
                    remainingNanos = awaitNanos(remainingNanos);
                }
                if (availableConnection == null) {
                    assert permits > 0 : permits;
                    permits--;
                } else {
                    assert availableConnection.opened();
                }
                return availableConnection;
            } finally {
                try {
                    if (tryGetAvailable && availableConnection == null) {//the desired connection slot has not yet been removed
                        removeLastDesiredConnectionSlot();
                    }
                } finally {
                    lock.unlock();
                }
            }
        }

        private void releasePermit() {
            lock();
            try {
                assert permits < MAX_CONNECTING : permits;
                permits++;
                condition.signal();
            } finally {
                lock.unlock();
            }
        }

        private void addLastDesiredConnectionSlot() {
            desiredConnectionSlots.addLast(new MutableReference<>());
        }

        @Nullable
        private PooledConnection tryGetAndRemoveFirstDesiredConnectionSlot() {
            assert !desiredConnectionSlots.isEmpty();
            PooledConnection result = desiredConnectionSlots.peekFirst().reference;
            if (result != null) {
                desiredConnectionSlots.removeFirst();
            }
            return result;
        }

        private void removeLastDesiredConnectionSlot() {
            assert !desiredConnectionSlots.isEmpty();
            PooledConnection connection = desiredConnectionSlots.removeLast().reference;
            if (connection != null) {
                pool.release(connection.wrapped);
            }
        }

        boolean tryHandOver(final PooledConnection openConnection) {
            lock();
            try {
                for (//iterate from first (head) to last (tail)
                        MutableReference<PooledConnection> desiredConnectionSlot : desiredConnectionSlots) {
                    if (desiredConnectionSlot.reference == null) {
                        desiredConnectionSlot.reference = openConnection;
                        condition.signal();
                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.trace(format("Handed over opened connection [%s] to server %s",
                                    getId(openConnection), serverId.getAddress()));
                        }
                        return true;
                    }
                }
                return false;
            } finally {
                lock.unlock();
            }
        }

        private void open(final PooledConnection connection) {
            boolean success = false;
            try {
                connection.open();
                success = true;
            } finally {
                if (!success) {
                    pool.release(connection.wrapped, true);
                    checkOutFailed(null, Reason.CONNECTION_ERROR);
                }
            }
        }

        private void lock() {
            tryLock(Timeout.INFINITE);
        }

        private void tryLock(final Timeout timeout) throws MongoTimeoutException {
            boolean success;
            try {
                if (timeout.equals(Timeout.INFINITE)) {
                    lock.lock();
                    success = true;
                } else {
                    success = lock.tryLock(timeout.remainingNanos(), TimeUnit.NANOSECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            if (!success) {
                throw createTimeoutException();
            }
        }

        private long awaitNanos(final long timeoutNanos) {
            try {
                return condition.awaitNanos(timeoutNanos);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    @NotThreadSafe
    private static final class MutableReference<T> {
        @Nullable
        private T reference;

        private MutableReference() {
        }
    }
}
