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

package com.mongodb.internal.operation;

import com.mongodb.MongoNamespace;
import com.mongodb.MongoSocketException;
import com.mongodb.ServerCursor;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.binding.ReferenceCounted;
import com.mongodb.internal.connection.Connection;
import com.mongodb.lang.Nullable;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.fail;
import static com.mongodb.internal.Locks.withLock;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.MESSAGE_IF_CONCURRENT_OPERATION;

/**
 * This is the resource manager for {@link CommandBatchCursor} or {@link AsyncCommandBatchCursor} implementations.
 * <p>
 * This class maintains all resources that must be released in {@link CommandBatchCursor#close()} /
 * {@link AsyncCommandBatchCursor#close()}. The abstract {@linkplain #doClose() deferred close action} is such that it is totally
 * ordered with other operations of {@link CommandBatchCursor} / {@link AsyncCommandBatchCursor} (methods {@link #tryStartOperation()}/
 * {@link #endOperation()} must be used properly to enforce the order) despite the method {@link CommandBatchCursor#close()} /
 * {@link AsyncCommandBatchCursor#close()} being called concurrently with those operations.
 * <p>
 * This total order induces the happens-before order.
 * <p>
 * The deferred close action does not violate externally observable idempotence of {@link CommandBatchCursor#close()} /
 * {@link AsyncCommandBatchCursor#close()}, because the close method is allowed to release resources "eventually".
 * <p>
 * Only methods explicitly documented as thread-safe are thread-safe,
 * others are not and rely on the total order mentioned above.
 */
@ThreadSafe
abstract class CursorResourceManager<CS extends ReferenceCounted, C extends ReferenceCounted> {
    private final Lock lock;
    private final TimeoutContext timeoutContext;
    private final TimeoutMode timeoutMode;
    private final MongoNamespace namespace;
    private volatile State state;
    @Nullable
    private volatile CS connectionSource;
    @Nullable
    private volatile C pinnedConnection;
    @Nullable
    private volatile ServerCursor serverCursor;
    private volatile boolean skipReleasingServerResourcesOnClose;
    private boolean closeWithoutTimeoutReset;

    CursorResourceManager(
            final TimeoutContext timeoutContext,
            final TimeoutMode timeoutMode,
            final MongoNamespace namespace,
            final CS connectionSource,
            @Nullable final C connectionToPin,
            @Nullable final ServerCursor serverCursor) {
        this.lock = new ReentrantLock();
        this.timeoutContext = timeoutContext;
        this.timeoutMode = timeoutMode;
        this.namespace = namespace;
        this.state = State.IDLE;
        if (serverCursor != null) {
            connectionSource.retain();
            this.connectionSource = connectionSource;
            if (connectionToPin != null) {
                connectionToPin.retain();
                markAsPinned(connectionToPin, Connection.PinningMode.CURSOR);
                this.pinnedConnection = connectionToPin;
            }
        }
        this.skipReleasingServerResourcesOnClose = false;
        this.serverCursor = serverCursor;
        this.closeWithoutTimeoutReset = false;
    }

    /**
     * Thread-safe.
     */
    MongoNamespace getNamespace() {
        return namespace;
    }

    /**
     * Thread-safe.
     */
    State getState() {
        return state;
    }

    /**
     * Thread-safe.
     */
    @Nullable
    CS getConnectionSource() {
        return connectionSource;
    }

    /**
     * Thread-safe.
     */
    @Nullable
    C getPinnedConnection() {
        return pinnedConnection;
    }

    /**
     * Thread-safe.
     */
    boolean isSkipReleasingServerResourcesOnClose() {
        return skipReleasingServerResourcesOnClose;
    }

    @SuppressWarnings("SameParameterValue")
    abstract void markAsPinned(C connectionToPin, Connection.PinningMode pinningMode);

    void checkTimeoutModeAndResetTimeoutContextIfIteration() {
        if (timeoutMode == TimeoutMode.ITERATION) {
            resetTimeout();
        }
    }

    void resetTimeout() {
        if (!closeWithoutTimeoutReset && timeoutContext.hasTimeoutMS()) {
            timeoutContext.resetTimeout();
        }
    }

    void setCloseWithoutTimeoutReset(final boolean closeImmediately) {
        this.closeWithoutTimeoutReset = closeImmediately;
    }

    /**
     * Thread-safe.
     */
    boolean operable() {
        return state.operable();
    }

    /**
     * Thread-safe.
     * Returns {@code true} iff started an operation.
     * If {@linkplain #operable() closed}, then returns false, otherwise completes abruptly.
     *
     * @throws IllegalStateException Iff another operation is in progress.
     */
    boolean tryStartOperation() throws IllegalStateException {
        return withLock(lock, () -> {
            State localState = state;
            if (!localState.operable()) {
                return false;
            } else if (localState == State.IDLE) {
                state = State.OPERATION_IN_PROGRESS;
                return true;
            } else if (localState == State.OPERATION_IN_PROGRESS) {
                throw new IllegalStateException(MESSAGE_IF_CONCURRENT_OPERATION);
            } else {
                throw fail(state.toString());
            }
        });
    }

    /**
     * Thread-safe.
     */
    void endOperation() {
        boolean doClose = withLock(lock, () -> {
            State localState = state;
            if (localState == State.OPERATION_IN_PROGRESS) {
                state = State.IDLE;
            } else if (localState == State.CLOSE_PENDING) {
                state = State.CLOSED;
                return true;
            } else if (localState != State.CLOSED) {
                throw fail(localState.toString());
            }
            return false;
        });
        if (doClose) {
            doClose();
        }
    }

    /**
     * Thread-safe.
     */
    void close() {
        boolean doClose = withLock(lock, () -> {
            State localState = state;
            if (localState == State.OPERATION_IN_PROGRESS) {
                state = State.CLOSE_PENDING;
            } else if (localState != State.CLOSED) {
                state = State.CLOSED;
                return true;
            }
            return false;
        });
        if (doClose) {
            doClose();
        }
    }

    /**
     * This method is never executed concurrently with either itself or other operations
     * demarcated by {@link #tryStartOperation()}/{@link #endOperation()}.
     */
    abstract void doClose();

    void onCorruptedConnection(@Nullable final C corruptedConnection, final MongoSocketException e) {
        // if `pinnedConnection` is corrupted, then we cannot kill `serverCursor` via such a connection
        C localPinnedConnection = pinnedConnection;
        if (localPinnedConnection != null) {
            if (corruptedConnection != localPinnedConnection) {
                e.addSuppressed(new AssertionError("Corrupted connection does not equal the pinned connection."));
            }
            skipReleasingServerResourcesOnClose = true;
        }
    }

    /**
     * Thread-safe.
     */
    @Nullable
    ServerCursor getServerCursor() {
        return serverCursor;
    }

    void setServerCursor(@Nullable final ServerCursor serverCursor) {
        assertTrue(state.inProgress());
        assertNotNull(this.serverCursor);
        // without `connectionSource` we will not be able to kill `serverCursor` later
        assertNotNull(connectionSource);
        this.serverCursor = serverCursor;
        if (serverCursor == null) {
            releaseClientResources();
        }
    }

    void unsetServerCursor() {
        this.serverCursor = null;
    }

    void releaseClientResources() {
        assertNull(serverCursor);
        CS localConnectionSource = connectionSource;
        if (localConnectionSource != null) {
            localConnectionSource.release();
            connectionSource = null;
        }
        C localPinnedConnection = pinnedConnection;
        if (localPinnedConnection != null) {
            localPinnedConnection.release();
            pinnedConnection = null;
        }
    }

    enum State {
        IDLE(true, false),
        OPERATION_IN_PROGRESS(true, true),
        /**
         * Implies {@link #OPERATION_IN_PROGRESS}.
         */
        CLOSE_PENDING(false, true),
        CLOSED(false, false);

        private final boolean operable;
        private final boolean inProgress;

        State(final boolean operable, final boolean inProgress) {
            this.operable = operable;
            this.inProgress = inProgress;
        }

        boolean operable() {
            return operable;
        }

        boolean inProgress() {
            return inProgress;
        }
    }
}
