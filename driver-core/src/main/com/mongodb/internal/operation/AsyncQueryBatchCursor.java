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
import com.mongodb.ServerCursor;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.async.AsyncAggregateResponseBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.QueryResult;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.FieldNameValidator;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.CursorHelper.getNumberToReturn;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.getMoreCursorDocumentToQueryResult;
import static com.mongodb.internal.operation.QueryHelper.translateCommandException;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionThreeDotTwo;
import static java.util.Collections.singletonList;

class AsyncQueryBatchCursor<T> implements AsyncAggregateResponseBatchCursor<T> {
    private static final FieldNameValidator NO_OP_FIELD_NAME_VALIDATOR = new NoOpFieldNameValidator();
    private static final String CURSOR = "cursor";
    private static final String POST_BATCH_RESUME_TOKEN = "postBatchResumeToken";
    private static final String OPERATION_TIME = "operationTime";

    private final MongoNamespace namespace;
    private final int limit;
    private final Decoder<T> decoder;
    private final long maxTimeMS;
    private volatile AsyncConnectionSource connectionSource;
    private volatile AsyncConnection pinnedConnection;
    private final AtomicReference<ServerCursor> cursor;
    private volatile QueryResult<T> firstBatch;
    private volatile int batchSize;
    private final AtomicInteger count = new AtomicInteger();
    private volatile BsonDocument postBatchResumeToken;
    private final BsonTimestamp operationTime;
    private final boolean firstBatchEmpty;
    private final int maxWireVersion;

    /* protected by `this` */
    private boolean isOperationInProgress = false;
    private boolean isClosed = false;
    /* protected by `this` */
    private volatile boolean isClosePending = false;

    AsyncQueryBatchCursor(final QueryResult<T> firstBatch, final int limit, final int batchSize, final long maxTimeMS,
                          final Decoder<T> decoder, final AsyncConnectionSource connectionSource, final AsyncConnection connection) {
        this(firstBatch, limit, batchSize, maxTimeMS, decoder, connectionSource, connection, null);
    }

    AsyncQueryBatchCursor(final QueryResult<T> firstBatch, final int limit, final int batchSize, final long maxTimeMS,
                          final Decoder<T> decoder, final AsyncConnectionSource connectionSource, final AsyncConnection connection,
                          final BsonDocument result) {
        isTrueArgument("maxTimeMS >= 0", maxTimeMS >= 0);
        this.maxTimeMS = maxTimeMS;
        this.namespace = firstBatch.getNamespace();
        this.firstBatch = firstBatch;
        this.limit = limit;
        this.batchSize = batchSize;
        this.decoder = decoder;
        this.cursor = new AtomicReference<ServerCursor>(firstBatch.getCursor());
        this.count.addAndGet(firstBatch.getResults().size());
        if (result != null) {
            this.operationTime = result.getTimestamp(OPERATION_TIME, null);
            this.postBatchResumeToken = getPostBatchResumeTokenFromResponse(result);
        } else {
            this.operationTime = null;
        }

        firstBatchEmpty = firstBatch.getResults().isEmpty();
        if (cursor.get() != null) {
            this.connectionSource = notNull("connectionSource", connectionSource).retain();
            assertTrue(connection != null);
            if (limitReached()) {
                killCursor(connection);
            } else {
                if (connectionSource.getServerDescription().getType() == ServerType.LOAD_BALANCER) {
                    this.pinnedConnection = connection.retain();
                    this.pinnedConnection.markAsPinned(Connection.PinningMode.CURSOR);
                }
            }
        }
        this.maxWireVersion = connection == null ? 0 : connection.getDescription().getMaxWireVersion();
    }

    /**
     * {@inheritDoc}
     * <p>
     * From the perspective of the code external to this class, this method is idempotent as required by its specification.
     * However, if this method sets {@link #isClosePending},
     * then it must be called by {@code this} again to release resources.
     * This behavior does not violate externally observable idempotence because this method is allowed to release resources "eventually".
     */
    @Override
    public void close() {
        boolean doClose = false;

        synchronized (this) {
            if (isOperationInProgress) {
                isClosePending = true;
            } else if (!isClosed) {
                isClosed = true;
                isClosePending = false;
                doClose = true;
            }
        }

        if (doClose) {
            killCursorOnClose();
        }
    }

    @Override
    public void next(final SingleResultCallback<List<T>> callback) {
        if (isClosed()) {
            callback.onResult(null, new MongoException("next() called after the cursor was closed."));
        } else if (firstBatch != null && (!firstBatch.getResults().isEmpty())) {
            // May be empty for a tailable cursor
            List<T> results = firstBatch.getResults();
            firstBatch = null;
            if (getServerCursor() == null) {
                close();
            }
            callback.onResult(results, null);
        } else {
            ServerCursor localCursor = getServerCursor();
            if (localCursor == null) {
                close();
                callback.onResult(null, null);
            } else {
                synchronized (this) {
                    if (isClosed()) {
                        callback.onResult(null, new MongoException("next() called after the cursor was closed."));
                        return;
                    }
                    isOperationInProgress = true;
                }
                getMore(localCursor, callback);
            }
        }
    }

    @Override
    public void setBatchSize(final int batchSize) {
        assertFalse(isClosed());
        this.batchSize = batchSize;
    }

    @Override
    public int getBatchSize() {
        assertFalse(isClosed());
        return batchSize;
    }

    @Override
    public boolean isClosed() {
        synchronized (this) {
            return isClosed || isClosePending;
        }
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

    private boolean limitReached() {
        return Math.abs(limit) != 0 && count.get() >= Math.abs(limit);
    }

    private void getMore(final ServerCursor cursor, final SingleResultCallback<List<T>> callback) {
        if (pinnedConnection != null)  {
            getMore(pinnedConnection.retain(), cursor, callback);
        } else {
            connectionSource.getConnection(new SingleResultCallback<AsyncConnection>() {
                @Override
                public void onResult(final AsyncConnection connection, final Throwable t) {
                    if (t != null) {
                        endOperationInProgress();
                        callback.onResult(null, t);
                    } else {
                        getMore(connection, cursor, callback);
                    }
                }
            });
        }
    }

    private void getMore(final AsyncConnection connection, final ServerCursor cursor, final SingleResultCallback<List<T>> callback) {
        if (serverIsAtLeastVersionThreeDotTwo(connection.getDescription())) {
            connection.commandAsync(namespace.getDatabaseName(), asGetMoreCommandDocument(cursor.getId()), NO_OP_FIELD_NAME_VALIDATOR,
                    ReadPreference.primary(), CommandResultDocumentCodec.create(decoder, "nextBatch"),
                    connectionSource.getSessionContext(), connectionSource.getServerApi(), connectionSource.getRequestContext(),
                    new CommandResultSingleResultCallback(connection, cursor, callback));

        } else {
            connection.getMoreAsync(namespace, cursor.getId(), getNumberToReturn(limit, batchSize, count.get()),
                                    decoder, new QueryResultSingleResultCallback(connection, callback));
        }
    }

    private BsonDocument asGetMoreCommandDocument(final long cursorId) {
        BsonDocument document = new BsonDocument("getMore", new BsonInt64(cursorId))
                .append("collection", new BsonString(namespace.getCollectionName()));

        int batchSizeForGetMoreCommand = Math.abs(getNumberToReturn(limit, this.batchSize, count.get()));
        if (batchSizeForGetMoreCommand != 0) {
            document.append("batchSize", new BsonInt32(batchSizeForGetMoreCommand));
        }
        if (maxTimeMS != 0) {
            document.append("maxTimeMS", new BsonInt64(maxTimeMS));
        }
        return document;
    }

    private void killCursorOnClose() {
        final ServerCursor localCursor = getServerCursor();
        if (localCursor != null) {
            if (pinnedConnection != null) {
                killCursorAsynchronouslyAndReleaseConnectionAndSource(pinnedConnection, localCursor);
            } else {
                connectionSource.getConnection(new SingleResultCallback<AsyncConnection>() {
                    @Override
                    public void onResult(final AsyncConnection connection, final Throwable t) {
                        if (t != null) {
                            connectionSource.release();
                        } else {
                            killCursorAsynchronouslyAndReleaseConnectionAndSource(connection, localCursor);
                        }
                    }
                });
            }
        } else if (pinnedConnection != null) {
            pinnedConnection.release();
        }
    }

    private void killCursor(final AsyncConnection connection) {
        ServerCursor localCursor = cursor.getAndSet(null);
        if (localCursor != null) {
            killCursorAsynchronouslyAndReleaseConnectionAndSource(connection.retain(), localCursor);
        } else {
            connectionSource.release();
        }
    }

    private void killCursorAsynchronouslyAndReleaseConnectionAndSource(final AsyncConnection connection, final ServerCursor localCursor) {
        if (serverIsAtLeastVersionThreeDotTwo(connection.getDescription())) {
            connection.commandAsync(namespace.getDatabaseName(), asKillCursorsCommandDocument(localCursor), NO_OP_FIELD_NAME_VALIDATOR,
                    ReadPreference.primary(), new BsonDocumentCodec(), connectionSource.getSessionContext(),
                    connectionSource.getServerApi(), connectionSource.getRequestContext(), new SingleResultCallback<BsonDocument>() {
                        @Override
                        public void onResult(final BsonDocument result, final Throwable t) {
                            connection.release();
                            connectionSource.release();
                        }
                    });
        } else {
            connection.killCursorAsync(namespace, singletonList(localCursor.getId()), new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    connection.release();
                    connectionSource.release();
                }
            });
        }
    }

    private BsonDocument asKillCursorsCommandDocument(final ServerCursor localCursor) {
        return new BsonDocument("killCursors", new BsonString(namespace.getCollectionName()))
                .append("cursors", new BsonArray(singletonList(new BsonInt64(localCursor.getId()))));
    }

    private void endOperationInProgress() {
        boolean closePending;
        synchronized (this) {
            isOperationInProgress = false;
            closePending = this.isClosePending;
        }
        if (closePending) {
            close();
        }
    }

    private void handleGetMoreQueryResult(final AsyncConnection connection, final SingleResultCallback<List<T>> callback,
                                          final QueryResult<T> result) {
        cursor.set(result.getCursor());
        if (isClosePending) {
            try {
                connection.release();
                if (result.getCursor() == null) {
                    connectionSource.release();
                }
                endOperationInProgress();
            } finally {
                callback.onResult(null, null);
            }
        } else if (result.getResults().isEmpty() && result.getCursor() != null) {
            getMore(connection, result.getCursor(), callback);
        } else {
            count.addAndGet(result.getResults().size());
            if (limitReached()) {
                killCursor(connection);
                connection.release();
            } else {
                connection.release();
                if (result.getCursor() == null) {
                    connectionSource.release();
                }
            }
            endOperationInProgress();

            if (result.getResults().isEmpty()) {
                callback.onResult(null, null);
            } else {
                callback.onResult(result.getResults(), null);
            }
        }
    }

    private class CommandResultSingleResultCallback implements SingleResultCallback<BsonDocument> {
        private final AsyncConnection connection;
        private final ServerCursor cursor;
        private final SingleResultCallback<List<T>> callback;

        CommandResultSingleResultCallback(final AsyncConnection connection, final ServerCursor cursor,
                                          final SingleResultCallback<List<T>> callback) {
            this.connection = connection;
            this.cursor = cursor;
            this.callback = errorHandlingCallback(callback, LOGGER);
        }

        @Override
        public void onResult(final BsonDocument result, final Throwable t) {
            if (t != null) {
                Throwable translatedException = t instanceof MongoCommandException
                        ? translateCommandException((MongoCommandException) t, cursor)
                        : t;
                connection.release();
                endOperationInProgress();
                callback.onResult(null, translatedException);
            } else {
                QueryResult<T> queryResult = getMoreCursorDocumentToQueryResult(result.getDocument(CURSOR),
                        connection.getDescription().getServerAddress());
                postBatchResumeToken = getPostBatchResumeTokenFromResponse(result);
                handleGetMoreQueryResult(connection, callback, queryResult);
            }
        }
    }

    private class QueryResultSingleResultCallback implements SingleResultCallback<QueryResult<T>> {
        private final AsyncConnection connection;
        private final SingleResultCallback<List<T>> callback;

        QueryResultSingleResultCallback(final AsyncConnection connection, final SingleResultCallback<List<T>> callback) {
            this.connection = connection;
            this.callback = errorHandlingCallback(callback, LOGGER);
        }

        @Override
        public void onResult(final QueryResult<T> result, final Throwable t) {
            if (t != null) {
                connection.release();
                endOperationInProgress();
                callback.onResult(null, t);
            } else {
                handleGetMoreQueryResult(connection, callback, result);
            }
        }
    }

    ServerCursor getServerCursor() {
        return cursor.get();
    }

    private BsonDocument getPostBatchResumeTokenFromResponse(final BsonDocument result) {
        BsonDocument cursor = result.getDocument(CURSOR, null);
        if (cursor != null) {
            return cursor.getDocument(POST_BATCH_RESUME_TOKEN, null);
        }
        return null;
    }
}
