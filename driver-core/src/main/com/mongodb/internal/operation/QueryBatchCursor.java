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
import com.mongodb.RequestContext;
import com.mongodb.ServerAddress;
import com.mongodb.ServerApi;
import com.mongodb.ServerCursor;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.ServerType;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.QueryResult;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.FieldNameValidator;
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
import static com.mongodb.internal.operation.CursorHelper.getNumberToReturn;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.internal.operation.OperationHelper.getMoreCursorDocumentToQueryResult;
import static com.mongodb.internal.operation.QueryHelper.translateCommandException;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionThreeDotTwo;
import static java.lang.String.format;
import static java.util.Collections.singletonList;

class QueryBatchCursor<T> implements AggregateResponseBatchCursor<T> {
    private static final Logger LOGGER = Loggers.getLogger("operation");
    private static final FieldNameValidator NO_OP_FIELD_NAME_VALIDATOR = new NoOpFieldNameValidator();
    private static final String CURSOR = "cursor";
    private static final String POST_BATCH_RESUME_TOKEN = "postBatchResumeToken";
    private static final String OPERATION_TIME = "operationTime";
    private static final String MESSAGE_IF_CLOSED_AS_CURSOR = "Cursor has been closed";
    private static final String MESSAGE_IF_CLOSED_AS_ITERATOR = "Iterator has been closed";

    private final MongoNamespace namespace;
    @Nullable
    private final ServerApi serverApi;
    private final ServerAddress serverAddress;
    private final int limit;
    private final Decoder<T> decoder;
    private final long maxTimeMS;
    private int batchSize;
    private BsonValue comment;
    private List<T> nextBatch;
    private int count;
    private BsonDocument postBatchResumeToken;
    private BsonTimestamp operationTime;
    private final boolean firstBatchEmpty;
    private int maxWireVersion = 0;
    private final ResourceManager resourceManager;

    QueryBatchCursor(final QueryResult<T> firstQueryResult, final int limit, final int batchSize, final Decoder<T> decoder) {
        this(firstQueryResult, limit, batchSize, decoder, null, null);
    }

    QueryBatchCursor(final QueryResult<T> firstQueryResult, final int limit, final int batchSize, final Decoder<T> decoder,
            final BsonValue comment, final ConnectionSource connectionSource) {
        this(firstQueryResult, limit, batchSize, 0, decoder, comment, connectionSource, null, null);
    }

    QueryBatchCursor(final QueryResult<T> firstQueryResult, final int limit, final int batchSize, final long maxTimeMS,
            final Decoder<T> decoder, final BsonValue comment, final ConnectionSource connectionSource, final Connection connection) {
        this(firstQueryResult, limit, batchSize, maxTimeMS, decoder, comment, connectionSource, connection, null);
    }

    QueryBatchCursor(final QueryResult<T> firstQueryResult, final int limit, final int batchSize, final long maxTimeMS,
            final Decoder<T> decoder, final BsonValue comment, final ConnectionSource connectionSource, final Connection connection,
            final BsonDocument result) {
        isTrueArgument("maxTimeMS >= 0", maxTimeMS >= 0);
        this.maxTimeMS = maxTimeMS;
        this.namespace = firstQueryResult.getNamespace();
        this.serverApi = connectionSource == null ? null : connectionSource.getServerApi();
        this.serverAddress = firstQueryResult.getAddress();
        this.limit = limit;
        this.comment = comment;
        this.batchSize = batchSize;
        this.decoder = notNull("decoder", decoder);
        if (result != null) {
            this.operationTime = result.getTimestamp(OPERATION_TIME, null);
            this.postBatchResumeToken = getPostBatchResumeTokenFromResponse(result);
        }
        ServerCursor serverCursor = initFromQueryResult(firstQueryResult);
        if (serverCursor != null) {
            notNull("connectionSource", connectionSource);
        }
        firstBatchEmpty = firstQueryResult.getResults().isEmpty();
        Connection connectionToPin = null;
        boolean releaseServerAndResources = false;
        if (connection != null) {
            this.maxWireVersion = connection.getDescription().getMaxWireVersion();
            if (limitReached()) {
                releaseServerAndResources = true;
            } else {
                assertNotNull(connectionSource);
                if (connectionSource.getServerDescription().getType() == ServerType.LOAD_BALANCER) {
                    connectionToPin = connection;
                }
            }
        }
        resourceManager = new ResourceManager(connectionSource, connectionToPin, serverCursor);
        if (releaseServerAndResources) {
            resourceManager.releaseServerAndClientResources(assertNotNull(connection));
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

    private List<T> doNext() {
        if (!doHasNext()) {
            throw new NoSuchElementException();
        }

        List<T> retVal = nextBatch;
        nextBatch = null;
        return retVal;
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

        return serverAddress;
    }

    @Override
    public BsonDocument getPostBatchResumeToken() {
        return postBatchResumeToken;
    }

    @Override
    public BsonTimestamp getOperationTime() {
        return operationTime;
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
            if (serverIsAtLeastVersionThreeDotTwo(connection.getDescription())) {
                try {
                    nextServerCursor = initFromCommandResult(connection.command(namespace.getDatabaseName(),
                            asGetMoreCommandDocument(serverCursor),
                            NO_OP_FIELD_NAME_VALIDATOR,
                            ReadPreference.primary(),
                            CommandResultDocumentCodec.create(decoder, "nextBatch"),
                            resourceManager.sessionContext(),
                            serverApi, resourceManager.requestContext()));
                } catch (MongoCommandException e) {
                    throw translateCommandException(e, serverCursor);
                }
            } else {
                QueryResult<T> getMore = connection.getMore(namespace, serverCursor.getId(),
                        getNumberToReturn(limit, batchSize, count), decoder, resourceManager.requestContext());
                nextServerCursor = initFromQueryResult(getMore);
            }
            resourceManager.setServerCursor(nextServerCursor);
            if (limitReached()) {
                resourceManager.releaseServerAndClientResources(connection);
            }
        });
    }

    private BsonDocument asGetMoreCommandDocument(final ServerCursor serverCursor) {
        BsonDocument document = new BsonDocument("getMore", new BsonInt64(serverCursor.getId()))
                                .append("collection", new BsonString(namespace.getCollectionName()));

        int batchSizeForGetMoreCommand = Math.abs(getNumberToReturn(limit, this.batchSize, count));
        if (batchSizeForGetMoreCommand != 0) {
            document.append("batchSize", new BsonInt32(batchSizeForGetMoreCommand));
        }
        if (maxTimeMS != 0) {
            document.append("maxTimeMS", new BsonInt64(maxTimeMS));
        }
        putIfNotNull(document, "comment", comment);
        return document;
    }

    @Nullable
    private ServerCursor initFromQueryResult(final QueryResult<T> queryResult) {
        nextBatch = queryResult.getResults().isEmpty() ? null : queryResult.getResults();
        count += queryResult.getResults().size();
        LOGGER.debug(format("Received batch of %d documents with cursorId %d from server %s", queryResult.getResults().size(),
                queryResult.getCursorId(), queryResult.getAddress()));
        return queryResult.getCursor();
    }

    @Nullable
    private ServerCursor initFromCommandResult(final BsonDocument getMoreCommandResultDocument) {
        QueryResult<T> queryResult = getMoreCursorDocumentToQueryResult(getMoreCommandResultDocument.getDocument(CURSOR), serverAddress);
        postBatchResumeToken = getPostBatchResumeTokenFromResponse(getMoreCommandResultDocument);
        operationTime = getMoreCommandResultDocument.getTimestamp(OPERATION_TIME, null);
        return initFromQueryResult(queryResult);
    }

    private boolean limitReached() {
        return Math.abs(limit) != 0 && count >= Math.abs(limit);
    }

    private BsonDocument getPostBatchResumeTokenFromResponse(final BsonDocument result) {
        BsonDocument cursor = result.getDocument(CURSOR, null);
        if (cursor != null) {
            return cursor.getDocument(POST_BATCH_RESUME_TOKEN, null);
        }
        return null;
    }

    /**
     * This class maintains all resources that must be released in {@link QueryBatchCursor#close()}.
     * It also implements a {@linkplain #doClose() deferred close action} such that it is totally ordered with other operations of
     * {@link QueryBatchCursor} (methods {@link #tryStartOperation()}/{@link #endOperation()} must be used properly to enforce the order)
     * despite the method {@link QueryBatchCursor#close()} being called concurrently with those operations.
     * This total order induces the happens-before order.
     * <p>
     * The deferred close action does not violate externally observable idempotence of {@link QueryBatchCursor#close()},
     * because {@link QueryBatchCursor#close()} is allowed to release resources "eventually".
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

        ResourceManager(@Nullable final ConnectionSource connectionSource,
                @Nullable final Connection connectionToPin, @Nullable final ServerCursor serverCursor) {
            lock = new StampedLock().asWriteLock();
            state = State.IDLE;
            if (serverCursor != null) {
                this.connectionSource = (assertNotNull(connectionSource)).retain();
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
         * @throws IllegalStateException If {@linkplain QueryBatchCursor#close() closed}.
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
                    throw new IllegalStateException("Another operation is currently in progress, concurrent operations are not supported");
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
                } catch (RuntimeException suppressed) {
                    e.addSuppressed(suppressed);
                }
                throw e;
            } finally {
                connection.release();
            }
        }

        private Connection connection() {
            assertTrue(state != State.IDLE);
            if (pinnedConnection == null) {
                return assertNotNull(connectionSource).getConnection();
            } else {
                return assertNotNull(pinnedConnection).retain();
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

        @Nullable
        SessionContext sessionContext() {
            return assertNotNull(connectionSource).getSessionContext();
        }

        RequestContext requestContext() {
            return assertNotNull(connectionSource).getRequestContext();
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
                    killServerCursor(namespace, localServerCursor, sessionContext(), requestContext(), serverApi,
                            assertNotNull(connection));
                }
            } finally {
                serverCursor = null;
            }
        }

        private void killServerCursor(final MongoNamespace namespace, final ServerCursor serverCursor,
                @Nullable final SessionContext sessionContext, final RequestContext requestContext, @Nullable final ServerApi serverApi,
                final Connection connection) {
            long cursorId = serverCursor.getId();
            if (serverIsAtLeastVersionThreeDotTwo(connection.getDescription())) {
                connection.command(namespace.getDatabaseName(), asKillCursorsCommandDocument(namespace, serverCursor),
                        NO_OP_FIELD_NAME_VALIDATOR, ReadPreference.primary(), new BsonDocumentCodec(), sessionContext, serverApi,
                        requestContext);
            } else {
                connection.killCursor(namespace, singletonList(cursorId), requestContext);
            }
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
