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
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;
import org.bson.ByteBuf;
import org.bson.codecs.Decoder;
import org.bson.types.ObjectId;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.fail;
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
    private static final Executor SAME_THREAD_EXECUTOR = Runnable::run;
    /**
     * Is package-access for the purpose of testing and must not be used for any other purpose outside of this class.
     */
    static final int MAX_CONNECTING = 2;

    private final ConcurrentPool<UsageTrackingInternalConnection> pool;
    private final ConnectionPoolSettings settings;
    private final AtomicInteger generation = new AtomicInteger(0);
    private final ScheduledExecutorService sizeMaintenanceTimer;
    private ExecutorService asyncGetter;
    private final Runnable maintenanceTask;
    private final ConnectionPoolListener connectionPoolListener;
    private final ServerId serverId;
    private final PinnedStatsManager pinnedStatsManager = new PinnedStatsManager();
    private final ServiceStateManager serviceStateManager = new ServiceStateManager();
    private final ConnectionGenerationSupplier connectionGenerationSupplier;
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
        openConcurrencyLimiter = new OpenConcurrencyLimiter(MAX_CONNECTING);
        connectionGenerationSupplier = new ConnectionGenerationSupplier() {
            @Override
            public int getGeneration() {
                return generation.get();
            }

            @Override
            public int getGeneration(@NonNull final ObjectId serviceId) {
                return serviceStateManager.getGeneration(serviceId);
            }
        };
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
                connection = openConcurrencyLimiter.openOrGetAvailable(connection, timeout);
            }
            connectionPoolListener.connectionCheckedOut(new ConnectionCheckedOutEvent(getId(connection)));
            return connection;
        } catch (RuntimeException e) {
            throw (RuntimeException) checkOutFailed(e);
        }
    }

    @Override
    public void getAsync(final SingleResultCallback<InternalConnection> callback) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Asynchronously getting a connection from the pool for server %s", serverId));
        }
        connectionPoolListener.connectionCheckOutStarted(new ConnectionCheckOutStartedEvent(serverId));
        Timeout timeout = Timeout.startNow(settings.getMaxWaitTime(NANOSECONDS));
        SingleResultCallback<InternalConnection> eventSendingCallback = (result, failure) -> {
            SingleResultCallback<InternalConnection> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
            if (failure == null) {
                connectionPoolListener.connectionCheckedOut(new ConnectionCheckedOutEvent(getId(result)));
                errHandlingCallback.onResult(result, null);
            } else {
                errHandlingCallback.onResult(null, checkOutFailed(failure));
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
            openAsync(immediateConnection, timeout, getAsyncGetter(), eventSendingCallback);
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
                openAsync(connection, timeout, SAME_THREAD_EXECUTOR, eventSendingCallback);
            });
        }
    }

    /**
     * Sends {@link ConnectionCheckOutFailedEvent}
     * and returns {@code t} if it is not {@link MongoOpenConnectionInternalException},
     * or returns {@code t.}{@linkplain MongoOpenConnectionInternalException#getCause() getCause()} otherwise.
     */
    private Throwable checkOutFailed(final Throwable t) {
        Throwable result = t;
        if (t instanceof MongoTimeoutException) {
            connectionPoolListener.connectionCheckOutFailed(new ConnectionCheckOutFailedEvent(serverId, Reason.TIMEOUT));
        } else if (t instanceof MongoOpenConnectionInternalException) {
            connectionPoolListener.connectionCheckOutFailed(new ConnectionCheckOutFailedEvent(serverId, Reason.CONNECTION_ERROR));
            result = assertNotNull(t.getCause());
        } else if (t instanceof IllegalStateException && t.getMessage().equals("The pool is closed")) {
            connectionPoolListener.connectionCheckOutFailed(new ConnectionCheckOutFailedEvent(serverId, Reason.POOL_CLOSED));
        } else {
            connectionPoolListener.connectionCheckOutFailed(new ConnectionCheckOutFailedEvent(serverId, Reason.UNKNOWN));
        }
        return result;
    }

    private void openAsync(final PooledConnection pooledConnection, final Timeout timeout, final Executor executor,
                           final SingleResultCallback<InternalConnection> callback) {
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
            executor.execute(() -> openConcurrencyLimiter.openAsyncOrGetAvailable(pooledConnection, timeout, callback));
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

    public void invalidate(final ObjectId serviceId, final int generation) {
        if (generation == InternalConnection.NOT_INITIALIZED_GENERATION) {
            return;
        }
        if (serviceStateManager.incrementGeneration(serviceId, generation)) {
            LOGGER.debug("Invalidating the connection pool for server id " + serviceId);
            connectionPoolListener.connectionPoolCleared(new ConnectionPoolClearedEvent(this.serverId, serviceId));
        }
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
        try {
            UsageTrackingInternalConnection internalConnection = pool.get(timeout.remainingOrInfinite(NANOSECONDS), NANOSECONDS);
            while (shouldPrune(internalConnection)) {
                pool.release(internalConnection, true);
                internalConnection = pool.get(timeout.remainingOrInfinite(NANOSECONDS), NANOSECONDS);
            }
            return new PooledConnection(internalConnection);
        } catch (MongoTimeoutException e) {
            throw createTimeoutException(timeout);
        }
    }

    @Nullable
    private PooledConnection getPooledConnectionImmediately() {
        UsageTrackingInternalConnection internalConnection = pool.getImmediately();
        while (internalConnection != null && shouldPrune(internalConnection)) {
            pool.release(internalConnection, true);
            internalConnection = pool.getImmediately();
        }
        return internalConnection == null ? null : new PooledConnection(internalConnection);
    }

    private MongoTimeoutException createTimeoutException(final Timeout timeout) {
        int numPinnedToCursor = pinnedStatsManager.getNumPinnedToCursor();
        int numPinnedToTransaction = pinnedStatsManager.getNumPinnedToTransaction();
        if (numPinnedToCursor == 0 && numPinnedToTransaction == 0) {
            return new MongoTimeoutException(format("Timed out after %s while waiting for a connection to server %s.",
                    timeout.toUserString(), serverId.getAddress()));
        } else {
            int maxSize = settings.getMaxSize();
            int numInUse = pool.getInUseCount();
            /* At this point in an execution we consider at least one of `numPinnedToCursor`, `numPinnedToTransaction` to be positive.
             * `numPinnedToCursor`, `numPinnedToTransaction` and `numInUse` are not a snapshot view,
             * but we still must maintain the following invariants:
             * - numInUse > 0
             *     we consider at least one of `numPinnedToCursor`, `numPinnedToTransaction` to be positive,
             *     so if we observe `numInUse` to be 0, we have to estimate it based on `numPinnedToCursor` and `numPinnedToTransaction`;
             * - numInUse < maxSize
             *     `numInUse` must not exceed the limit in situations when we estimate `numInUse`;
             * - numPinnedToCursor + numPinnedToTransaction <= numInUse
             *     otherwise the numbers do not make sense.
             */
            if (numInUse == 0) {
                numInUse = Math.min(
                        numPinnedToCursor + numPinnedToTransaction, // must be at least a big as this sum but not bigger than `maxSize`
                        maxSize);
            }
            numPinnedToTransaction = Math.min(
                    numPinnedToTransaction, // prefer the observed value, but it must not be bigger than `numInUse` - `numPinnedToCursor`
                    numInUse - numPinnedToCursor);
            int numOtherInUse = numInUse - numPinnedToCursor - numPinnedToTransaction;
            assertTrue(numOtherInUse >= 0);
            assertTrue(numPinnedToCursor + numPinnedToTransaction + numOtherInUse <= maxSize);
            return new MongoTimeoutException(format("Timed out after %s while waiting for a connection to server %s. Details: "
                            + "maxPoolSize: %d, connections in use by cursors: %d, connections in use by transactions: %d, "
                            + "connections in use by other operations: %d",
                    timeout.toUserString(), serverId.getAddress(),
                    maxSize, numPinnedToCursor, numPinnedToTransaction, numOtherInUse));
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
                    try {
                        if (shouldPrune()) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(format("Pruning pooled connections to %s", serverId.getAddress()));
                            }
                            pool.prune();
                        }
                        if (shouldEnsureMinSize()) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(format("Ensuring minimum pooled connections to %s", serverId.getAddress()));
                            }
                            pool.ensureMinSize(settings.getMinSize(), newConnection ->
                                    openConcurrencyLimiter.openImmediately(new PooledConnection(newConnection)));
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
        int generation = connection.getGeneration();
        if (generation == InternalConnection.NOT_INITIALIZED_GENERATION) {
            return false;
        }
        ObjectId serviceId = connection.getDescription().getServiceId();
        if (serviceId != null) {
            return serviceStateManager.getGeneration(serviceId) > generation;
        } else {
            return this.generation.get() > generation;
        }
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

    /**
     * Must not throw {@link Exception}s.
     */
    private ConnectionId getId(final InternalConnection internalConnection) {
        return internalConnection.getDescription().getConnectionId();
    }

    private class PooledConnection implements InternalConnection {
        private final UsageTrackingInternalConnection wrapped;
        private final AtomicBoolean isClosed = new AtomicBoolean();
        private Connection.PinningMode pinningMode;

        PooledConnection(final UsageTrackingInternalConnection wrapped) {
            this.wrapped = notNull("wrapped", wrapped);
        }

        @Override
        public int getGeneration() {
            return wrapped.getGeneration();
        }

        @Override
        public void open() {
            assertFalse(isClosed.get());
            try {
                connectionCreated(connectionPoolListener, wrapped.getDescription().getConnectionId());
                wrapped.open();
            } catch (RuntimeException e) {
                closeAndHandleOpenFailure();
                throw new MongoOpenConnectionInternalException(e);
            }
            handleOpenSuccess();
        }

        @Override
        public void openAsync(final SingleResultCallback<Void> callback) {
            assertFalse(isClosed.get());
            wrapped.openAsync((nullResult, failure) -> {
                if (failure != null) {
                    closeAndHandleOpenFailure();
                    callback.onResult(null, new MongoOpenConnectionInternalException(failure));
                } else {
                    handleOpenSuccess();
                    callback.onResult(nullResult, null);
                }
            });
        }

        @Override
        public void close() {
            // All but the first call is a no-op
            if (!isClosed.getAndSet(true)) {
                unmarkAsPinned();
                connectionPoolListener.connectionCheckedIn(new ConnectionCheckedInEvent(getId(wrapped)));
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(format("Checked in connection [%s] to server %s", getId(wrapped), serverId.getAddress()));
                }
                if (wrapped.isClosed() || shouldPrune(wrapped)) {
                    pool.release(wrapped, true);
                } else {
                    openConcurrencyLimiter.tryHandOverOrRelease(wrapped);
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
         * This method must be used if and only if {@link ConnectionCreatedEvent} was not sent for the connection.
         * Must not throw {@link Exception}s.
         */
        void closeSilently() {
            if (!isClosed.getAndSet(true)) {
                wrapped.setCloseSilently();
                pool.release(wrapped, true);
            }
        }

        /**
         * Must not throw {@link Exception}s.
         */
        private void closeAndHandleOpenFailure() {
            if (!isClosed.getAndSet(true)) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(format("Pooled connection %s to server %s failed to open", getId(this), serverId));
                }
                if (wrapped.getDescription().getServiceId() != null) {
                    invalidate(wrapped.getDescription().getServiceId(), wrapped.getGeneration());
                }
                pool.release(wrapped, true);
            }
        }

        /**
         * Must not throw {@link Exception}s.
         */
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
        public void markAsPinned(final Connection.PinningMode pinningMode) {
            assertNotNull(pinningMode);
            // if the connection is already pinned for some other mode, the additional mode can be ignored.
            // The typical case is the connection is first pinned for a transaction, then pinned for a cursor withing that transaction
            // In this case, the cursor pinning is subsumed by the transaction pinning.
            if (this.pinningMode == null) {
                this.pinningMode = pinningMode;
                pinnedStatsManager.increment(pinningMode);
            }
        }

        void unmarkAsPinned() {
            if (pinningMode != null) {
                pinnedStatsManager.decrement(pinningMode);
            }
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

    /**
     * This internal exception is used to express an exceptional situation encountered when opening a connection.
     * It exists because it allows consolidating the code that sends events for exceptional situations in a
     * {@linkplain #checkOutFailed(Throwable) single place}, it must not be observable by an external code.
     */
    private static final class MongoOpenConnectionInternalException extends RuntimeException {
        private static final long serialVersionUID = 1;

        MongoOpenConnectionInternalException(@NonNull final Throwable cause) {
            super(null, cause);
        }
    }

    private class UsageTrackingInternalConnectionItemFactory implements ConcurrentPool.ItemFactory<UsageTrackingInternalConnection> {
        private final InternalConnectionFactory internalConnectionFactory;

        UsageTrackingInternalConnectionItemFactory(final InternalConnectionFactory internalConnectionFactory) {
            this.internalConnectionFactory = internalConnectionFactory;
        }

        @Override
        public UsageTrackingInternalConnection create() {
            return new UsageTrackingInternalConnection(internalConnectionFactory.create(serverId, connectionGenerationSupplier),
                    serviceStateManager);
        }

        @Override
        public void close(final UsageTrackingInternalConnection connection) {
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
            connection.close();
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

        PooledConnection openOrGetAvailable(final PooledConnection connection, final Timeout timeout) throws MongoTimeoutException {
            return openOrGetAvailable(connection, true, timeout);
        }

        void openImmediately(final PooledConnection connection) throws MongoTimeoutException {
            PooledConnection result = openOrGetAvailable(connection, false, Timeout.immediate());
            assertTrue(result == connection);
        }

        /**
         * This method can be thought of as operating in two phases.
         * In the first phase it tries to synchronously acquire a permit to open the {@code connection}
         * or get a different {@linkplain PooledConnection#opened() opened} connection if {@code tryGetAvailable} is {@code true} and
         * one becomes available while waiting for a permit.
         * The first phase has one of the following outcomes:
         * <ol>
         *     <li>A {@link MongoTimeoutException} or a different {@link Exception} is thrown.</li>
         *     <li>An opened connection different from the specified one is returned,
         *     and the specified {@code connection} is {@linkplain PooledConnection#closeSilently() silently closed}.
         *     </li>
         *     <li>A permit is acquired, {@link #connectionCreated(ConnectionPoolListener, ConnectionId)} is reported
         *     and an attempt to open the specified {@code connection} is made. This is the second phase in which
         *     the {@code connection} is {@linkplain PooledConnection#open() opened synchronously}.
         *     The attempt to open the {@code connection} has one of the following outcomes
         *     combined with releasing the acquired permit:</li>
         *     <ol>
         *         <li>An {@link Exception} is thrown
         *         and the {@code connection} is {@linkplain PooledConnection#closeAndHandleOpenFailure() closed}.</li>
         *         <li>The specified {@code connection}, which is now opened, is returned.</li>
         *     </ol>
         * </ol>
         *
         * @param timeout Applies only to the first phase.
         * @return An {@linkplain PooledConnection#opened() opened} connection which is
         * either the specified {@code connection} or a different one.
         * @throws MongoTimeoutException If the first phase timed out.
         */
        private PooledConnection openOrGetAvailable(
                final PooledConnection connection, final boolean tryGetAvailable, final Timeout timeout) throws MongoTimeoutException {
            PooledConnection availableConnection;
            try {//phase one
                availableConnection = acquirePermitOrGetAvailableOpenedConnection(tryGetAvailable, timeout);
            } catch (RuntimeException e) {
                connection.closeSilently();
                throw e;
            }
            if (availableConnection != null) {
                connection.closeSilently();
                return availableConnection;
            } else {//acquired a permit, phase two
                try {
                    connection.open();
                } finally {
                    releasePermit();
                }
                return connection;
            }
        }

        /**
         * This method is similar to {@link #openOrGetAvailable(PooledConnection, boolean, Timeout)} with the following differences:
         * <ul>
         *     <li>It does not have the {@code tryGetAvailable} parameter and acts as if this parameter were {@code true}.</li>
         *     <li>While the first phase is still synchronous, the {@code connection} is
         *     {@linkplain PooledConnection#openAsync(SingleResultCallback) opened asynchronously} in the second phase.</li>
         *     <li>Instead of returning a result or throwing an exception via Java {@code return}/{@code throw} statements,
         *     it calls {@code callback.}{@link SingleResultCallback#onResult(Object, Throwable) onResult(result, failure)}
         *     and passes either a {@link PooledConnection} or an {@link Exception}.</li>
         * </ul>
         */
        void openAsyncOrGetAvailable(
                final PooledConnection connection, final Timeout timeout, final SingleResultCallback<InternalConnection> callback) {
            PooledConnection availableConnection;
            try {//phase one
                availableConnection = acquirePermitOrGetAvailableOpenedConnection(true, timeout);
            } catch (RuntimeException e) {
                connection.closeSilently();
                callback.onResult(null, e);
                return;
            }
            if (availableConnection != null) {
                connection.closeSilently();
                callback.onResult(availableConnection, null);
            } else {//acquired a permit, phase two
                connectionCreated(//a connection is considered created only when it is ready to be open
                        connectionPoolListener, getId(connection));
                connection.openAsync((nullResult, failure) -> {
                    releasePermit();
                    if (failure != null) {
                        callback.onResult(null, failure);
                    } else {
                        callback.onResult(connection, null);
                    }
                });
            }
        }

        /**
         * @return Either {@code null} if a permit has been acquired, or a {@link PooledConnection}
         * if {@code tryGetAvailable} is {@code true} and an {@linkplain PooledConnection#opened() opened} one becomes available while
         * waiting for a permit.
         * @throws MongoTimeoutException If timed out.
         */
        @Nullable
        private PooledConnection acquirePermitOrGetAvailableOpenedConnection(final boolean tryGetAvailable, final Timeout timeout)
                throws MongoTimeoutException {
            PooledConnection availableConnection = null;
            tryLock(timeout);
            try {
                if (tryGetAvailable) {
                    /* An attempt to get an available opened connection from the pool (must be done while holding the lock)
                     * happens here at most once to prevent the race condition in the following execution
                     * (actions are specified in the execution total order,
                     * which by definition exists if an execution is either sequentially consistent or linearizable):
                     * 1. Thread#1 starts checking out and gets a non-opened connection.
                     * 2. Thread#2 checks in a connection. Tries to hand it over, but there are no threads desiring to get one.
                     * 3. Thread#1 executes the current code. Expresses the desire to get a connection via the hand-over mechanism,
                     *   but thread#2 has already tried handing over and released its connection to the pool.
                     * As a result, thread#1 is waiting for a permit to open a connection despite one being available in the pool.*/
                    availableConnection = getPooledConnectionImmediately();
                    if (availableConnection != null) {
                        return availableConnection;
                    }
                    expressDesireToGetAvailableConnection();
                }
                long remainingNanos = timeout.remainingOrInfinite(NANOSECONDS);
                while (permits == 0) {
                    availableConnection = tryGetAvailable ? tryGetAvailableConnection() : null;
                    if (availableConnection != null) {
                        break;
                    }
                    if (Timeout.expired(remainingNanos)) {
                        throw createTimeoutException(timeout);
                    }
                    remainingNanos = awaitNanos(remainingNanos);
                }
                if (availableConnection == null) {
                    assertTrue(permits > 0);
                    permits--;
                }
                return availableConnection;
            } finally {
                try {
                    if (tryGetAvailable && availableConnection == null) {//the desired connection slot has not yet been removed
                        giveUpOnTryingToGetAvailableConnection();
                    }
                } finally {
                    lock.unlock();
                }
            }
        }

        private void releasePermit() {
            lock.lock();
            try {
                assertTrue(permits < maxPermits);
                permits++;
                condition.signal();
            } finally {
                lock.unlock();
            }
        }

        private void expressDesireToGetAvailableConnection() {
            desiredConnectionSlots.addLast(new MutableReference<>());
        }

        @Nullable
        private PooledConnection tryGetAvailableConnection() {
            assertFalse(desiredConnectionSlots.isEmpty());
            PooledConnection result = desiredConnectionSlots.peekFirst().reference;
            if (result != null) {
                desiredConnectionSlots.removeFirst();
                assertTrue(result.opened());
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(format("Received opened connection [%s] to server %s", getId(result), serverId.getAddress()));
                }
            }
            return result;
        }

        private void giveUpOnTryingToGetAvailableConnection() {
            assertFalse(desiredConnectionSlots.isEmpty());
            PooledConnection connection = desiredConnectionSlots.removeLast().reference;
            if (connection != null) {
                connection.release();
            }
        }

        /**
         * The hand-over mechanism is needed to prevent other threads doing checkout from stealing newly released connections
         * from threads that are waiting for a permit to open a connection.
         */
        void tryHandOverOrRelease(final UsageTrackingInternalConnection openConnection) {
            lock.lock();
            try {
                for (//iterate from first (head) to last (tail)
                        MutableReference<PooledConnection> desiredConnectionSlot : desiredConnectionSlots) {
                    if (desiredConnectionSlot.reference == null) {
                        desiredConnectionSlot.reference = new PooledConnection(openConnection);
                        condition.signal();
                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.trace(format("Handed over opened connection [%s] to server %s",
                                    getId(openConnection), serverId.getAddress()));
                        }
                        return;
                    }
                }
                pool.release(openConnection);
            } finally {
                lock.unlock();
            }
        }

        /**
         * @param timeout If {@linkplain Timeout#isInfinite() infinite},
         *                then the lock is {@linkplain Lock#lockInterruptibly() acquired interruptibly}.
         */
        private void tryLock(final Timeout timeout) throws MongoTimeoutException {
            boolean success;
            try {
                if (timeout.isInfinite()) {
                    lock.lockInterruptibly();
                    success = true;
                } else {
                    success = lock.tryLock(timeout.remaining(NANOSECONDS), NANOSECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new MongoInterruptedException(null, e);
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
                    //noinspection ResultOfMethodCallIgnored
                    condition.awaitNanos(Long.MAX_VALUE);
                    return timeoutNanos;
                } else {
                    return Math.max(0, condition.awaitNanos(timeoutNanos));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new MongoInterruptedException(null, e);
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

    static final class ServiceStateManager {
        private final ConcurrentHashMap<ObjectId, ServiceState> stateByServiceId = new ConcurrentHashMap<>();

        void addConnection(final ObjectId serviceId) {
            stateByServiceId.compute(serviceId, (k, v) -> {
                if (v == null) {
                    v = new ServiceState();
                }
                v.incrementConnectionCount();
                return v;
            });
        }

        /**
         * Removes the mapping from {@code serviceId} to a {@link ServiceState} if its connection count reaches 0.
         * This is done to prevent memory leaks.
         * <p>
         * This method must be called once for any connection for which {@link #addConnection(ObjectId)} was called.
         */
        void removeConnection(final ObjectId serviceId) {
            stateByServiceId.compute(serviceId, (k, v) -> {
                assertNotNull(v);
                return v.decrementAndGetConnectionCount() == 0 ? null : v;
            });
        }

        /**
         * In some cases we may increment the generation even for an unregistered serviceId, as when open fails on the only connection to
         * a given serviceId. In this case this method does not track the generation increment but does return true.
         *
         * @return true if the generation was incremented
         */
        boolean incrementGeneration(final ObjectId serviceId, final int expectedGeneration) {
            ServiceState state = stateByServiceId.get(serviceId);
            return state == null || state.incrementGeneration(expectedGeneration);
        }

        int getGeneration(final ObjectId serviceId) {
            ServiceState state = stateByServiceId.get(serviceId);
            return state == null ? 0 : state.getGeneration();
        }

        private static final class ServiceState {
            private final AtomicInteger generation = new AtomicInteger();
            private final AtomicInteger connectionCount = new AtomicInteger();

            void incrementConnectionCount() {
                connectionCount.incrementAndGet();
            }

            int decrementAndGetConnectionCount() {
                return connectionCount.decrementAndGet();
            }

            boolean incrementGeneration(final int expectedGeneration) {
                return generation.compareAndSet(expectedGeneration, expectedGeneration + 1);
            }

            public int getGeneration() {
                return generation.get();
            }
        }
    }

    private static final class PinnedStatsManager {
        private final LongAdder numPinnedToCursor = new LongAdder();
        private final LongAdder numPinnedToTransaction = new LongAdder();

        void increment(final Connection.PinningMode pinningMode) {
            switch (pinningMode) {
                case CURSOR:
                    numPinnedToCursor.increment();
                    break;
                case TRANSACTION:
                    numPinnedToTransaction.increment();
                    break;
                default:
                    fail();
            }
        }

        void decrement(final Connection.PinningMode pinningMode) {
            switch (pinningMode) {
                case CURSOR:
                    numPinnedToCursor.decrement();
                    break;
                case TRANSACTION:
                    numPinnedToTransaction.decrement();
                    break;
                default:
                    fail();
            }
        }

        int getNumPinnedToCursor() {
            return numPinnedToCursor.intValue();
        }

        int getNumPinnedToTransaction() {
            return numPinnedToTransaction.intValue();
        }
    }
}
