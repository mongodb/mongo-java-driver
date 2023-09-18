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

import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.async.AsyncAggregateResponseBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.function.AsyncCallbackSupplier;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Connection;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.fail;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.FIRST_BATCH;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.MESSAGE_IF_CLOSED_AS_CURSOR;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.MESSAGE_IF_CONCURRENT_OPERATION;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.NEXT_BATCH;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.NO_OP_FIELD_NAME_VALIDATOR;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.getMoreCommandDocument;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.translateCommandException;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

class AsyncCommandBatchCursor<T> implements AsyncAggregateResponseBatchCursor<T> {

    private final MongoNamespace namespace;
    private final int limit;
    private final Decoder<T> decoder;
    private final long maxTimeMS;
    @Nullable
    private final BsonValue comment;

    private final AtomicInteger count = new AtomicInteger();
    private final AtomicBoolean processedInitial = new AtomicBoolean();
    private final AtomicReference<CommandCursorResult<T>> commandCursorResult = new AtomicReference<>();
    private final int maxWireVersion;
    private final boolean firstBatchEmpty;
    private final ResourceManager resourceManager;
    private int batchSize;

    AsyncCommandBatchCursor(
            final ServerAddress serverAddress,
            final BsonDocument commandCursorDocument,
            final int limit, final int batchSize, final long maxTimeMS,
            final Decoder<T> decoder,
            @Nullable final BsonValue comment,
            final AsyncConnectionSource connectionSource,
            final AsyncConnection connection) {
        isTrueArgument("maxTimeMS >= 0", maxTimeMS >= 0);
        CommandCursorResult<T> commandCursor = initFromCommandCursorDocument(serverAddress, FIRST_BATCH, commandCursorDocument);
        this.namespace = commandCursor.getNamespace();
        this.limit = limit;
        this.batchSize = batchSize;
        this.maxTimeMS = maxTimeMS;
        this.decoder = notNull("decoder", decoder);
        this.comment = comment;
        this.maxWireVersion = connection.getDescription().getMaxWireVersion();
        this.firstBatchEmpty = commandCursor.getResults().isEmpty();

        AsyncConnection connectionToPin = null;
        boolean releaseServerAndResources = false;
        if (limitReached()) {
            releaseServerAndResources = true;
        } else if (connectionSource.getServerDescription().getType() == ServerType.LOAD_BALANCER) {
            connectionToPin = connection;
        }

        resourceManager = new ResourceManager(connectionSource, connectionToPin, commandCursor.getServerCursor());
        if (releaseServerAndResources) {
            resourceManager.releaseServerAndClientResources(connection.retain());
        }
    }

    @Override
    public void next(final SingleResultCallback<List<T>> callback) {
        if (isClosed()) {
            callback.onResult(null, new MongoException(MESSAGE_IF_CLOSED_AS_CURSOR));
            return;
        } else if (!resourceManager.tryStartOperation()) {
            callback.onResult(null, new MongoException(MESSAGE_IF_CONCURRENT_OPERATION));
            return;
        }

        ServerCursor localServerCursor = resourceManager.serverCursor();
        boolean cursorClosed = localServerCursor == null;
        List<T> batchResults = emptyList();
        if (!processedInitial.getAndSet(true) && !firstBatchEmpty) {
            batchResults = commandCursorResult.get().getResults();
        }

        if (cursorClosed || !batchResults.isEmpty()) {
            resourceManager.endOperation();
            if (cursorClosed) {
                close();
            }
            callback.onResult(batchResults, null);
        } else {
            getMore(localServerCursor, callback);
        }
    }

    @Override
    public void setBatchSize(final int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public boolean isClosed() {
        return !resourceManager.operable();
    }

    @Override
    public void close() {
        resourceManager.close();
    }

    @Override
    public BsonDocument getPostBatchResumeToken() {
        return commandCursorResult.get().getPostBatchResumeToken();
    }

    @Override
    public BsonTimestamp getOperationTime() {
        return commandCursorResult.get().getOperationTime();
    }

    @Override
    public boolean isFirstBatchEmpty() {
        return firstBatchEmpty;
    }

    @Override
    public int getMaxWireVersion() {
        return maxWireVersion;
    }

    @Nullable
    @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
    ServerCursor getServerCursor() {
        return resourceManager.serverCursor();
    }

    private void getMore(final ServerCursor cursor, final SingleResultCallback<List<T>> callback) {
        resourceManager.executeWithConnection(callback, connection -> getMore(connection, cursor, callback));
    }

    private void getMore(final AsyncConnection connection, final ServerCursor cursor, final SingleResultCallback<List<T>> callback) {
        connection.commandAsync(namespace.getDatabaseName(),
                getMoreCommandDocument(cursor.getId(), connection.getDescription(), namespace,
                        limit, batchSize, count.get(), maxTimeMS, comment),
                NO_OP_FIELD_NAME_VALIDATOR, ReadPreference.primary(),
                CommandResultDocumentCodec.create(decoder, NEXT_BATCH), assertNotNull(resourceManager.connectionSource),
                (commandResult, t) -> {
                    if (t != null) {
                        Throwable translatedException =
                                    t instanceof MongoCommandException
                                            ? translateCommandException((MongoCommandException) t, cursor)
                                            : t;

                        connection.release();
                        resourceManager.endOperation();
                        callback.onResult(null, translatedException);
                        return;
                    }

                    CommandCursorResult<T> commandCursor =
                            initFromCommandCursorDocument(connection.getDescription().getServerAddress(), NEXT_BATCH, assertNotNull(commandResult));
                    resourceManager.setServerCursor(commandCursor.getServerCursor());

                    if (!resourceManager.operable()) {
                        resourceManager.releaseServerAndClientResources(connection);
                        callback.onResult(emptyList(), null);
                        return;
                    }

                    if (commandCursor.getResults().isEmpty() && commandCursor.getServerCursor() != null) {
                        connection.release();
                        getMore(commandCursor.getServerCursor(), callback);
                    } else {
                        resourceManager.endOperation();
                        if (limitReached()) {
                            resourceManager.releaseServerAndClientResources(connection);
                        } else {
                            connection.release();
                        }
                        callback.onResult(commandCursor.getResults(), null);
                    }
                });
    }

    private CommandCursorResult<T> initFromCommandCursorDocument(
            final ServerAddress serverAddress,
            final String fieldNameContainingBatch,
            final BsonDocument commandCursorDocument) {
        CommandCursorResult<T> cursorResult = new CommandCursorResult<>(serverAddress, fieldNameContainingBatch, commandCursorDocument);
        count.addAndGet(cursorResult.getResults().size());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Received batch of %d documents with cursorId %d from server %s", cursorResult.getResults().size(),
                    cursorResult.getCursorId(), cursorResult.getServerAddress()));
        }
        this.commandCursorResult.set(cursorResult);
        return cursorResult;
    }

    private boolean limitReached() {
        return Math.abs(limit) != 0 && count.get() >= Math.abs(limit);
    }

    /**
     * This class maintains all resources that must be released in {@link AsyncCommandBatchCursor#close()}.
     * It also implements a {@linkplain #doClose() deferred close action} such that it is totally ordered with other operations of
     * {@link AsyncCommandBatchCursor} (methods {@link #tryStartOperation()}/{@link #endOperation()} must be used properly to enforce the order)
     * despite the method {@link AsyncCommandBatchCursor#close()} being called concurrently with those operations.
     * This total order induces the happens-before order.
     * <p>
     * The deferred close action does not violate externally observable idempotence of {@link AsyncCommandBatchCursor#close()},
     * because {@link AsyncCommandBatchCursor#close()} is allowed to release resources "eventually".
     * <p>
     * Only methods explicitly documented as thread-safe are thread-safe,
     * others are not and rely on the total order mentioned above.
     */
    @ThreadSafe
    private final class ResourceManager {
        private final Lock lock;
        private volatile State state = State.IDLE;
        @Nullable
        private volatile AsyncConnectionSource connectionSource;
        @Nullable
        private volatile AsyncConnection pinnedConnection;
        @Nullable
        private volatile ServerCursor serverCursor;
        private volatile boolean skipReleasingServerResourcesOnClose;

        ResourceManager(final AsyncConnectionSource connectionSource,
                @Nullable final AsyncConnection connectionToPin,
                @Nullable final ServerCursor serverCursor) {
            lock = new ReentrantLock();
            if (serverCursor != null) {
                this.connectionSource = notNull("connectionSource", connectionSource).retain();
                if (connectionToPin != null) {
                    this.pinnedConnection = connectionToPin.retain();
                    connectionToPin.markAsPinned(Connection.PinningMode.CURSOR);
                }
            }
            skipReleasingServerResourcesOnClose = false;
            this.serverCursor = serverCursor;
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
         * If {@linkplain State#operable() closed}, then returns false, otherwise completes abruptly.
         *
         * @throws IllegalStateException Iff another operation is in progress.
         */
        private boolean tryStartOperation() throws IllegalStateException {
            lock.lock();
            try {
                State localState = state;
                if (!localState.operable()) {
                    return false;
                } else if (localState == State.IDLE) {
                    state = State.OPERATION_IN_PROGRESS;
                    return true;
                } else if (localState == State.OPERATION_IN_PROGRESS) {
                    return false;
                } else {
                    throw fail(state.toString());
                }
            } finally {
                lock.unlock();
            }
        }

        /**
         * Thread-safe.
         */
        private void endOperation() {
            boolean doClose = false;
            lock.lock();
            try {
                State localState = state;
                if (localState == State.OPERATION_IN_PROGRESS) {
                    state = State.IDLE;
                } else if (localState == State.CLOSE_PENDING) {
                    state = State.CLOSED;
                    doClose = true;
                } else if (localState != State.CLOSED) {
                    fail(localState.toString());
                }
            } finally {
                lock.unlock();
            }
            if (doClose) {
                doClose();
            }
        }

        /**
         * Thread-safe.
         */
        void close() {
            boolean doClose = false;
            lock.lock();
            try {
                State localState = state;
                if (localState == State.OPERATION_IN_PROGRESS) {
                    state = State.CLOSE_PENDING;
                } else if (localState != State.CLOSED) {
                    state = State.CLOSED;
                    doClose = true;
                }
            } finally {
                lock.unlock();
            }
            if (doClose) {
                doClose();
            }
        }

        /**
         * This method is never executed concurrently with either itself or other operations
         * demarcated by {@link #tryStartOperation()}/{@link #endOperation()}.
         */
        private void doClose() {
            if (skipReleasingServerResourcesOnClose) {
                serverCursor = null;
                releaseClientResources();
            } else if (serverCursor != null) {
                executeWithConnection((connection, t) -> {
                    // guarantee that regardless of exceptions, `serverCursor` is null and client resources are released
                    serverCursor = null;
                    releaseClientResources();
                }, resourceManager::releaseServerAndClientResources);
            }
        }

        void onCorruptedConnection(@Nullable final AsyncConnection corruptedConnection) {
            // if `pinnedConnection` is corrupted, then we cannot kill `serverCursor` via such a connection
            AsyncConnection localPinnedConnection = pinnedConnection;
            if (localPinnedConnection != null) {
                assertTrue(corruptedConnection == localPinnedConnection);
                skipReleasingServerResourcesOnClose = true;
            }
        }

        <R> void executeWithConnection(
                final SingleResultCallback<R> callback,
                final Consumer<AsyncConnection> function) {
            assertTrue(state != State.IDLE);
            if (pinnedConnection != null) {
                executeWithConnection(assertNotNull(pinnedConnection).retain(), null, callback, function);
            } else {
                assertNotNull(connectionSource).getConnection((conn, t) -> executeWithConnection(conn, t, callback, function));
            }
        }

        <R> void executeWithConnection(
                @Nullable final AsyncConnection connection,
                @Nullable final Throwable t,
                final SingleResultCallback<R> callback,
                final Consumer<AsyncConnection> function) {
            assertTrue(connection != null || t != null);
            if (t != null) {
                try {
                    onCorruptedConnection(connection);
                } catch (Exception suppressed) {
                    t.addSuppressed(suppressed);
                }
                callback.onResult(null, t);
            } else {
                AsyncCallbackSupplier<R> curriedFunction = c -> function.accept(connection);
                curriedFunction.whenComplete(connection::release).get(callback);
            }
        }

        /**
         * Thread-safe.
         */
        @Nullable
        ServerCursor serverCursor() {
            return serverCursor;
        }

        void setServerCursor(@Nullable final ServerCursor serverCursor) {
            assertTrue(state.inProgress());
            assertNotNull(connectionSource);
            this.serverCursor = serverCursor;
            if (serverCursor == null) {
                releaseClientResources();
            }
        }

        private void releaseServerAndClientResources(final AsyncConnection connection) {
            lock.lock();
            ServerCursor localServerCursor = serverCursor;
            serverCursor = null;
            lock.unlock();
            if (localServerCursor != null) {
                killServerCursor(namespace, localServerCursor, connection);
            } else {
                connection.release();
                releaseClientResources();
            }
        }

        private void killServerCursor(final MongoNamespace namespace, final ServerCursor serverCursor, final AsyncConnection connection) {
            connection
                    .commandAsync(namespace.getDatabaseName(), asKillCursorsCommandDocument(namespace, serverCursor),
                            NO_OP_FIELD_NAME_VALIDATOR, ReadPreference.primary(), new BsonDocumentCodec(), assertNotNull(connectionSource),
                            (r, t) -> {
                                connection.release();
                                releaseClientResources();
                            });
        }

        private BsonDocument asKillCursorsCommandDocument(final MongoNamespace namespace, final ServerCursor serverCursor) {
            return new BsonDocument("killCursors", new BsonString(namespace.getCollectionName()))
                    .append("cursors", new BsonArray(singletonList(new BsonInt64(serverCursor.getId()))));
        }

        private void releaseClientResources() {
            assertNull(serverCursor);
            lock.lock();
            AsyncConnectionSource localConnectionSource = connectionSource;
            connectionSource = null;

            AsyncConnection localPinnedConnection = pinnedConnection;
            pinnedConnection = null;
            lock.unlock();

            if (localConnectionSource != null) {
                localConnectionSource.release();
            }
            if (localPinnedConnection != null) {
                localPinnedConnection.release();
            }
        }
    }

    private enum State {
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
