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
import com.mongodb.MongoSocketException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.binding.ConnectionSource;
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
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.fail;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.FIRST_BATCH;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.MESSAGE_IF_CLOSED_AS_CURSOR;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.MESSAGE_IF_CLOSED_AS_ITERATOR;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.MESSAGE_IF_CONCURRENT_OPERATION;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.NEXT_BATCH;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.NO_OP_FIELD_NAME_VALIDATOR;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.getMoreCommandDocument;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.translateCommandException;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static java.lang.String.format;
import static java.util.Collections.singletonList;

class CommandBatchCursor<T> implements AggregateResponseBatchCursor<T> {

    private final MongoNamespace namespace;
    private final int limit;
    private final Decoder<T> decoder;
    private final long maxTimeMS;
    @Nullable
    private final BsonValue comment;

    private CommandCursorResult<T> commandCursorResult;
    private int batchSize;
    @Nullable
    private List<T> nextBatch;
    private int count = 0;

    private final boolean firstBatchEmpty;
    private final int maxWireVersion;
    private final ResourceManager resourceManager;

    CommandBatchCursor(
            final ServerAddress serverAddress,
            final BsonDocument commandCursorDocument,
            final int limit, final int batchSize, final long maxTimeMS,
            final Decoder<T> decoder,
            @Nullable final BsonValue comment,
            final ConnectionSource connectionSource,
            final Connection connection) {
        isTrueArgument("maxTimeMS >= 0", maxTimeMS >= 0);
        initFromCommandCursorDocument(serverAddress, FIRST_BATCH, commandCursorDocument);
        this.namespace = commandCursorResult.getNamespace();
        this.limit = limit;
        this.batchSize = batchSize;
        this.maxTimeMS = maxTimeMS;
        this.decoder = notNull("decoder", decoder);
        this.comment = comment;

        firstBatchEmpty = commandCursorResult.getResults().isEmpty();
        Connection connectionToPin = null;
        boolean releaseServerAndResources = false;

        this.maxWireVersion = connection.getDescription().getMaxWireVersion();
        if (limitReached()) {
            releaseServerAndResources = true;
        } else if (connectionSource.getServerDescription().getType() == ServerType.LOAD_BALANCER) {
            connectionToPin = connection;
        }

        resourceManager = new ResourceManager(connectionSource, connectionToPin, commandCursorResult.getServerCursor());
        if (releaseServerAndResources) {
            resourceManager.releaseServerAndClientResources(connection);
        }
    }

    @Override
    public boolean hasNext() {
        return assertNotNull(resourceManager.execute(MESSAGE_IF_CLOSED_AS_CURSOR, this::doHasNext));
    }

    private boolean doHasNext() {
        if (nextBatch != null) {
            return true;
        }

        if (limitReached()) {
            return false;
        }

        while (resourceManager.serverCursor() != null) {
            getMore();
            if (!resourceManager.operable()) {
                throw new IllegalStateException(MESSAGE_IF_CLOSED_AS_CURSOR);
            }
            if (nextBatch != null) {
                return true;
            }
        }

        return false;
    }

    @Override
    public List<T> next() {
        return assertNotNull(resourceManager.execute(MESSAGE_IF_CLOSED_AS_ITERATOR, this::doNext));
    }

    @Override
    public int available() {
        return !resourceManager.operable() || nextBatch == null ? 0 : nextBatch.size();
    }

    @Nullable
    private List<T> doNext() {
        if (!doHasNext()) {
            throw new NoSuchElementException();
        }

        List<T> retVal = nextBatch;
        nextBatch = null;
        return retVal;
    }


    @VisibleForTesting(otherwise = PRIVATE)
    boolean isClosed() {
        return !resourceManager.operable();
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
    public void remove() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void close() {
        resourceManager.close();
    }

    @Nullable
    @Override
    public List<T> tryNext() {
        return resourceManager.execute(MESSAGE_IF_CLOSED_AS_CURSOR, () -> {
            if (!tryHasNext()) {
                return null;
            }
            return doNext();
        });
    }

    private boolean tryHasNext() {
        if (nextBatch != null) {
            return true;
        }

        if (limitReached()) {
            return false;
        }

        if (resourceManager.serverCursor() != null) {
            getMore();
        }

        return nextBatch != null;
    }

    @Override
    @Nullable
    public ServerCursor getServerCursor() {
        if (!resourceManager.operable()) {
            throw new IllegalStateException(MESSAGE_IF_CLOSED_AS_ITERATOR);
        }
        return resourceManager.serverCursor();
    }

    @Override
    public ServerAddress getServerAddress() {
        if (!resourceManager.operable()) {
            throw new IllegalStateException(MESSAGE_IF_CLOSED_AS_ITERATOR);
        }

        return commandCursorResult.getServerAddress();
    }

    @Override
    public BsonDocument getPostBatchResumeToken() {
        return commandCursorResult.getPostBatchResumeToken();
    }

    @Override
    public BsonTimestamp getOperationTime() {
        return commandCursorResult.getOperationTime();
    }

    @Override
    public boolean isFirstBatchEmpty() {
        return firstBatchEmpty;
    }

    @Override
    public int getMaxWireVersion() {
        return maxWireVersion;
    }

    private void getMore() {
        ServerCursor serverCursor = assertNotNull(resourceManager.serverCursor());
        resourceManager.executeWithConnection(connection -> {
            ServerCursor nextServerCursor;
            try {
                initFromCommandCursorDocument(connection.getDescription().getServerAddress(), NEXT_BATCH,
                        assertNotNull(
                                connection.command(namespace.getDatabaseName(),
                                 getMoreCommandDocument(serverCursor.getId(), connection.getDescription(), namespace,
                                                limit, batchSize, count, maxTimeMS, comment),
                                NO_OP_FIELD_NAME_VALIDATOR,
                                ReadPreference.primary(),
                                CommandResultDocumentCodec.create(decoder, NEXT_BATCH),
                                assertNotNull(resourceManager.connectionSource))));
                nextServerCursor = commandCursorResult.getServerCursor();
            } catch (MongoCommandException e) {
                throw translateCommandException(e, serverCursor);
            }

            resourceManager.setServerCursor(nextServerCursor);
            if (!resourceManager.operable() || limitReached() || nextServerCursor == null) {
                resourceManager.releaseServerAndClientResources(connection);
            }
        });
    }

    private void initFromCommandCursorDocument(final ServerAddress serverAddress, final String fieldNameContainingBatch,
            final BsonDocument commandCursorDocument) {
        this.commandCursorResult = new CommandCursorResult<>(serverAddress, fieldNameContainingBatch, commandCursorDocument);
        this.nextBatch = commandCursorResult.getResults().isEmpty() ? null : commandCursorResult.getResults();
        this.count += commandCursorResult.getResults().size();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Received batch of %d documents with cursorId %d from server %s", commandCursorResult.getResults().size(),
                    commandCursorResult.getCursorId(), commandCursorResult.getServerAddress()));
        }
    }

    private boolean limitReached() {
        return Math.abs(limit) != 0 && count >= Math.abs(limit);
    }

    /**
     * This class maintains all resources that must be released in {@link CommandBatchCursor#close()}.
     * It also implements a {@linkplain #doClose() deferred close action} such that it is totally ordered with other operations of
     * {@link CommandBatchCursor} (methods {@link #tryStartOperation()}/{@link #endOperation()} must be used properly to enforce the order)
     * despite the method {@link CommandBatchCursor#close()} being called concurrently with those operations.
     * This total order induces the happens-before order.
     * <p>
     * The deferred close action does not violate externally observable idempotence of {@link CommandBatchCursor#close()},
     * because {@link CommandBatchCursor#close()} is allowed to release resources "eventually".
     * <p>
     * Only methods explicitly documented as thread-safe are thread-safe,
     * others are not and rely on the total order mentioned above.
     */
    @ThreadSafe
    private final class ResourceManager {
        private final Lock lock;
        private volatile State state;
        @Nullable
        private volatile ConnectionSource connectionSource;
        @Nullable
        private volatile Connection pinnedConnection;
        @Nullable
        private volatile ServerCursor serverCursor;
        private volatile boolean skipReleasingServerResourcesOnClose;

        ResourceManager(final ConnectionSource connectionSource,
                @Nullable final Connection connectionToPin,
                @Nullable final ServerCursor serverCursor) {
            lock = new StampedLock().asWriteLock();
            state = State.IDLE;
            if (serverCursor != null) {
                this.connectionSource =  notNull("connectionSource", connectionSource).retain();
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
         * Executes {@code operation} within the {@link #tryStartOperation()}/{@link #endOperation()} bounds.
         *
         * @throws IllegalStateException If {@linkplain CommandBatchCursor#close() closed}.
         */
        @Nullable
        <R> R execute(final String exceptionMessageIfClosed, final Supplier<R> operation) throws IllegalStateException {
            if (!tryStartOperation()) {
                throw new IllegalStateException(exceptionMessageIfClosed);
            }
            try {
                return operation.get();
            } finally {
                endOperation();
            }
        }

        /**
         * Thread-safe.
         * Returns {@code true} iff started an operation.
         * If {@linkplain #operable() closed}, then returns false, otherwise completes abruptly.
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
                    throw new IllegalStateException(MESSAGE_IF_CONCURRENT_OPERATION);
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
                } else {
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
            try {
                if (skipReleasingServerResourcesOnClose) {
                    serverCursor = null;
                } else if (serverCursor != null) {
                    Connection connection = connection();
                    try {
                        releaseServerResources(connection);
                    } finally {
                        connection.release();
                    }
                }
            } catch (MongoException e) {
                // ignore exceptions when releasing server resources
            } finally {
                // guarantee that regardless of exceptions, `serverCursor` is null and client resources are released
                serverCursor = null;
                releaseClientResources();
            }
        }

        void onCorruptedConnection(final Connection corruptedConnection) {
            assertTrue(state.inProgress());
            // if `pinnedConnection` is corrupted, then we cannot kill `serverCursor` via such a connection
            Connection localPinnedConnection = pinnedConnection;
            if (localPinnedConnection != null) {
                assertTrue(corruptedConnection == localPinnedConnection);
                skipReleasingServerResourcesOnClose = true;
            }
        }

        void executeWithConnection(final Consumer<Connection> action) {
            Connection connection = connection();
            try {
                action.accept(connection);
            } catch (MongoSocketException e) {
                try {
                    onCorruptedConnection(connection);
                } catch (Exception suppressed) {
                    e.addSuppressed(suppressed);
                }
                throw e;
            } finally {
                connection.release();
            }
        }

        private Connection connection() {
            assertTrue(state != State.IDLE);
            if (pinnedConnection != null) {
                return assertNotNull(pinnedConnection).retain();
            } else {
                return assertNotNull(connectionSource).getConnection();
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
            assertNotNull(this.serverCursor);
            // without `connectionSource` we will not be able to kill `serverCursor` later
            assertNotNull(connectionSource);
            this.serverCursor = serverCursor;
            if (serverCursor == null) {
                releaseClientResources();
            }
        }


        void releaseServerAndClientResources(final Connection connection) {
            try {
                releaseServerResources(assertNotNull(connection));
            } finally {
                releaseClientResources();
            }
        }

        private void releaseServerResources(final Connection connection) {
            try {
                ServerCursor localServerCursor = serverCursor;
                if (localServerCursor != null) {
                    killServerCursor(namespace, localServerCursor, assertNotNull(connection));
                }
            } finally {
                serverCursor = null;
            }
        }

        private void killServerCursor(final MongoNamespace namespace, final ServerCursor serverCursor, final Connection connection) {
            connection.command(namespace.getDatabaseName(), asKillCursorsCommandDocument(namespace, serverCursor),
                    NO_OP_FIELD_NAME_VALIDATOR, ReadPreference.primary(), new BsonDocumentCodec(), assertNotNull(connectionSource));
        }

        private BsonDocument asKillCursorsCommandDocument(final MongoNamespace namespace, final ServerCursor serverCursor) {
            return new BsonDocument("killCursors", new BsonString(namespace.getCollectionName()))
                    .append("cursors", new BsonArray(singletonList(new BsonInt64(serverCursor.getId()))));
        }

        private void releaseClientResources() {
            assertNull(serverCursor);
            ConnectionSource localConnectionSource = connectionSource;
            if (localConnectionSource != null) {
                localConnectionSource.release();
                connectionSource = null;
            }
            Connection localPinnedConnection = pinnedConnection;
            if (localPinnedConnection != null) {
                localPinnedConnection.release();
                pinnedConnection = null;
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
