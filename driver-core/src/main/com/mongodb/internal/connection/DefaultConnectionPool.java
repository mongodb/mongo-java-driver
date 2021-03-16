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
import com.mongodb.internal.Timeout;
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
        openConcurrencyLimiter = new OpenConcurrencyLimiter(2);
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
        connectionPoolListener.connectionCheckOutStarted(new ConnectionCheckOutStartedEvent(serverId));
        Timeout timeout = Timeout.startNow(timeoutValue, timeUnit);
        try {
            PooledConnection connection = getPooledConnection(timeout);
            if (!connection.opened()) {
                connection = openConcurrencyLimiter.openOrGetAvailableAndSilentlyCloseSpecified(connection, true, timeout);
            }
            connectionPoolListener.connectionCheckedOut(new ConnectionCheckedOutEvent(getId(connection)));
            return connection;
        } catch (RuntimeException e) {
            checkOutFailed(e, null);
            throw e;
        }
    }

    @Override
    public void getAsync(final SingleResultCallback<InternalConnection> callback) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Asynchronously getting a connection from the pool for server %s", serverId));
        }
        connectionPoolListener.connectionCheckOutStarted(new ConnectionCheckOutStartedEvent(serverId));
        final Timeout timeout = Timeout.startNow(settings.getMaxWaitTime(NANOSECONDS));
        final SingleResultCallback<InternalConnection> eventSendingCallback = (result, failure) -> {
            SingleResultCallback<InternalConnection> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
            if (failure == null) {
                connectionPoolListener.connectionCheckedOut(new ConnectionCheckedOutEvent(getId(result)));
                errHandlingCallback.onResult(result, null);
            } else {
                checkOutFailed(failure, null);
                errHandlingCallback.onResult(null, failure);
            }
        };
        PooledConnection immediateConnection = null;
        try {
            immediateConnection = getPooledConnection(Timeout.immediate());
        } catch (MongoTimeoutException e) {
            // fall through
        } catch (RuntimeException e) {
            eventSendingCallback.onResult(null, e);
            return;
        }
        if (immediateConnection != null) {
            openAsync(immediateConnection, eventSendingCallback, timeout);
        } else {
            getAsyncGetter().execute(() -> {
                if (timeout.expired()) {
                    eventSendingCallback.onResult(null, createTimeoutException(timeout));
                    return;
                }
                PooledConnection connection;
                try {
                    connection = getPooledConnection(timeout);
                } catch (RuntimeException e) {
                    eventSendingCallback.onResult(null, e);
                    return;
                }
                openAsync(connection, eventSendingCallback, timeout);
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
                           final SingleResultCallback<InternalConnection> callback,
                           final Timeout timeout) {
        if (pooledConnection.opened()) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("Pooled connection %s to server %s is already open",
                        getId(pooledConnection), serverId));
            }
            callback.onResult(pooledConnection, null);
        } else {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("Pooled connection %s to server %s is not yet open",
                        getId(pooledConnection), serverId));
            }
            getAsyncGetter().execute(() -> openConcurrencyLimiter.openOrGetAvailableAndSilentlyCloseSpecified(
                    pooledConnection, true, timeout, callback));
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
            UsageTrackingInternalConnection internalConnection = pool.get(timeout.remainingNanosOrInfinite(), TimeUnit.NANOSECONDS);
            while (shouldPrune(internalConnection)) {
                pool.release(internalConnection, true);
                long remainingNanos = timeout.remainingNanosOrInfinite();
                if (Timeout.expired(remainingNanos)) {
                    externalTimeoutException = true;
                    throw createTimeoutException(timeout);
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
                MongoTimeoutException timeoutException = createTimeoutException(timeout);
                timeoutException.addSuppressed(externalOrInternalTimeoutException);
                throw timeoutException;
            }
        }
    }

    private MongoTimeoutException createTimeoutException(final Timeout timeout) {
        return new MongoTimeoutException(format("Timed out after %s while waiting for a connection to server %s.",
                                                timeout.duration(MILLISECONDS), serverId.getAddress()));
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
                                PooledConnection pooledConnection = new PooledConnection(newConnection);
                                if (!newConnection.opened()) {
                                    pooledConnection = openConcurrencyLimiter.openOrGetAvailableAndSilentlyCloseSpecified(
                                            pooledConnection, false, Timeout.immediate());
                                }
                                return Optional.of(pooledConnection.wrapped);
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
            assert !isClosed.get();
            try {
                wrapped.open();
                handleOpenSuccess();
            } catch (RuntimeException e) {
                try {
                    throw e;
                } finally {
                    closeAndHandleOpenFailure();
                }
            }
        }

        @Override
        public void openAsync(final SingleResultCallback<Void> callback) {
            assert !isClosed.get();
            wrapped.openAsync((nullResult, failure) -> {
                if (failure == null) {
                    try {
                        handleOpenSuccess();
                    } finally {
                        callback.onResult(nullResult, null);
                    }
                } else {
                    try {
                        closeAndHandleOpenFailure();
                    } finally {
                        callback.onResult(null, failure);
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

        void release() {
            if (!isClosed.getAndSet(true)) {
                pool.release(wrapped);
            }
        }

        /**
         * {@linkplain ConcurrentPool#release(Object, boolean) Prune} this connection without sending a {@link ConnectionClosedEvent}.
         * This method must be used if and only if {@link ConnectionCreatedEvent} was not emitted for the connection.
         */
        void closeSilently() {
            if (!isClosed.getAndSet(true)) {
                try {
                    wrapped.setCloseSilently();
                } finally {
                    pool.release(wrapped, true);
                }
            }
        }

        private void closeAndHandleOpenFailure() {
            if (!isClosed.getAndSet(true)) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(format("Pooled connection %s to server %s failed to open", getId(this), serverId));
                }
                pool.release(wrapped, true);
            }
        }

        private void handleOpenSuccess() {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("Pooled connection %s to server %s is now open", getId(this), serverId));
            }
            connectionPoolListener.connectionReady(new ConnectionReadyEvent(getId(this)));
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
     * Package-private methods are thread-safe,
     * and only they should be called outside of the {@link OpenConcurrencyLimiter}'s code.
     */
    @ThreadSafe
    private final class OpenConcurrencyLimiter {
        private final Lock lock;
        private final Condition condition;
        private final int maxPermits;
        private int permits;
        private final Deque<MutableReference<PooledConnection>> desiredConnectionSlots;

        OpenConcurrencyLimiter(final int maxConnecting) {
            lock = new ReentrantLock(true);
            condition = lock.newCondition();
            maxPermits = maxConnecting;
            permits = maxPermits;
            desiredConnectionSlots = new LinkedList<>();
        }

        /**
         * @see #openOrGetAvailableAndSilentlyCloseSpecified(PooledConnection, boolean, Timeout, SingleResultCallback)
         */
        PooledConnection openOrGetAvailableAndSilentlyCloseSpecified(
                final PooledConnection connection, final boolean tryGetAvailable, final Timeout timeout) throws MongoTimeoutException {
            PooledConnection result = openOrGetAvailableAndSilentlyCloseSpecified(connection, tryGetAvailable, timeout, null);
            assert result != null;
            return result;
        }

        /**
         * Regardless of the {@code asyncCallback}, this method has a synchronous phase.
         * In this phase it tries to synchronously acquire a permit to open the {@code connection}
         * or get a different {@linkplain PooledConnection#opened() opened} connection if {@code tryGetAvailable} is {@code true} and
         * one becomes available while waiting for a permit.
         * The first synchronous phase has one of the following outcomes:
         * <ol>
         *     <li>A {@link MongoTimeoutException} or a different {@link Exception} is thrown<sup>1</sup>.</li>
         *     <li>An opened connection different from the specified one is returned<sup>1</sup>,
         *     and the specified {@code connection} is {@linkplain PooledConnection#closeSilently() silently closed}.
         *     </li>
         *     <li>A permit is acquired, {@link #connectionCreated(ConnectionPoolListener, ConnectionId)} is reported
         *     and an attempt to open the specified {@code connection} is made. This is the second phase in which
         *     the {@code connection} is {@linkplain PooledConnection#open() opened synchronously} if {@code asyncCallback} is null,
         *     or {@linkplain PooledConnection#openAsync(SingleResultCallback) opened asynchronously} otherwise.
         *     The attempt to open the {@code connection} has one of the following outcomes
         *     combined with releasing the acquired permit:</li>
         *     <ol>
         *         <li>A {@link MongoTimeoutException} or a different {@link Exception} is thrown<sup>1</sup>
         *         and the {@code connection} is {@linkplain PooledConnection#closeAndHandleOpenFailure() closed}.</li>
         *         <li>The specified {@code connection}, which is now opened, is returned<sup>1</sup>.</li>
         *     </ol>
         * </ol>
         * <hr>
         * <sup>1</sup> Throwing and returning behavior depends on the value if {@code asyncCallback}:
         * <ul>
         *     <li>if {@code null}, then the words "return"/"throw" mean usual {@code return}/{@code throw} in Java;</li>
         *     <li>otherwise "return"/"throw" mean calling
         *     {@code asyncCallback.}{@link SingleResultCallback#onResult(Object, Throwable) onResult(result, failure)}
         *     and passing either a {@link PooledConnection} or an {@link Exception}.</li>
         * </ul>
         *
         * @param timeout Applies only to the first synchronous phase.
         * @return If {@code asyncCallback} is {@code null}, then an {@linkplain PooledConnection#opened() opened} connection which is
         * either the specified {@code connection} or a different one; otherwise returns {@code null}.
         * @throws MongoTimeoutException If {@code asyncCallback} is {@code null} and the method timed out;
         * otherwise does not throw this exception.
         */
        @Nullable
        PooledConnection openOrGetAvailableAndSilentlyCloseSpecified(
                final PooledConnection connection, final boolean tryGetAvailable, final Timeout timeout,
                @Nullable final SingleResultCallback<InternalConnection> asyncCallback) throws MongoTimeoutException {
            PooledConnection availableConnection;
            try {//phase one, synchronous
                availableConnection = acquirePermitOrGetAvailableOpenConnection(tryGetAvailable, timeout);
            } catch (RuntimeException e) {
                try {
                    return throwException(e, asyncCallback);
                } finally {
                    connection.closeSilently();
                }
            }
            if (availableConnection != null) {
                try {
                    return returnValue(availableConnection, asyncCallback);
                } finally {
                    connection.closeSilently();
                }
            } else {//acquired a permit, phase two
                connectionCreated(//a connection is considered created only when it is ready to be open
                        connectionPoolListener, getId(connection));
                return openAndReleasePermit(connection, asyncCallback);
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
                long remainingNanos = timeout.remainingNanosOrInfinite();
                while (permits == 0
                        && (availableConnection = tryGetAvailable ? tryGetAndRemoveFirstDesiredConnectionSlot() : null) == null) {
                    if (Timeout.expired(remainingNanos)) {
                        throw createTimeoutException(timeout);
                    }
                    remainingNanos = awaitNanos(remainingNanos);
                }
                if (availableConnection == null) {
                    assert permits > 0 : permits;
                    permits--;
                } else {
                    assert availableConnection.opened();
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace(format("Received opened connection [%s] to server %s",
                                getId(availableConnection), serverId.getAddress()));
                    }
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
                assert permits < maxPermits : permits;
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
                connection.release();
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

        @Nullable
        private PooledConnection openAndReleasePermit(
                final PooledConnection connection,
                @Nullable final SingleResultCallback<InternalConnection> callback) {
            if (callback == null) {
                try {
                    connection.open();
                    return connection;
                } finally {
                    releasePermit();
                }
            } else {
                connection.openAsync((nullResult, failure) -> {
                    try {
                        releasePermit();
                    } finally {
                        if (failure == null) {
                            callback.onResult(connection, null);
                        } else {
                            callback.onResult(null, failure);
                        }
                    }
                });
                return null;
            }
        }

        @Nullable
        private <T> T returnValue(T value, @Nullable SingleResultCallback<? super T> callback) {
            if (callback == null) {
                return value;
            } else {
                callback.onResult(value, null);
                return null;
            }
        }

        @Nullable
        private <T> T throwException(RuntimeException e, @Nullable SingleResultCallback<? super T> callback) throws RuntimeException {
            if (callback == null) {
                throw e;
            } else {
                callback.onResult(null, e);
                return null;
            }
        }

        private void lock() {
            tryLock(Timeout.infinite());
        }

        private void tryLock(final Timeout timeout) throws MongoTimeoutException {
            boolean success;
            try {
                if (timeout.isInfinite()) {
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
                throw createTimeoutException(timeout);
            }
        }

        /**
         * Returns {@code timeoutNanos} if {@code timeoutNanos} is negative, otherwise returns 0 or a positive value.
         *
         * @param timeoutNanos Use a negative value for an infinite timeout,
         *                     in which case {@link Condition#awaitNanos(long)} is called with {@link Long#MAX_VALUE}.
         */
        private long awaitNanos(final long timeoutNanos) {
            try {
                if (timeoutNanos < 0) {
                    condition.awaitNanos(Long.MAX_VALUE);
                    return timeoutNanos;
                } else {
                    return Math.max(0, condition.awaitNanos(timeoutNanos));
                }
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
