/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

package com.mongodb.operation;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.ServerCursor;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.QueryResult;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.codecs.Decoder;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.operation.CursorHelper.getNumberToReturn;
import static com.mongodb.operation.OperationHelper.LOGGER;
import static com.mongodb.operation.OperationHelper.getMoreCursorDocumentToQueryResult;
import static com.mongodb.operation.OperationHelper.serverIsAtLeastVersionThreeDotTwo;
import static com.mongodb.operation.QueryHelper.translateCommandException;
import static java.util.Collections.singletonList;

class AsyncQueryBatchCursor<T> implements AsyncBatchCursor<T> {

    private final MongoNamespace namespace;
    private final int limit;
    private final Decoder<T> decoder;
    private final long maxTimeMS;
    private volatile AsyncConnectionSource connectionSource;
    private volatile QueryResult<T> firstBatch;
    private volatile int batchSize;
    private volatile ServerCursor cursor;
    private volatile int count;
    private volatile boolean closed;

    AsyncQueryBatchCursor(final QueryResult<T> firstBatch, final int limit, final int batchSize,
                          final Decoder<T> decoder) {
        this(firstBatch, limit, batchSize, 0, decoder, null, null);
    }

    AsyncQueryBatchCursor(final QueryResult<T> firstBatch, final int limit, final int batchSize, final long maxTimeMS,
                          final Decoder<T> decoder, final AsyncConnectionSource connectionSource, final AsyncConnection connection) {
        isTrueArgument("maxTimeMS >= 0", maxTimeMS >= 0);
        this.maxTimeMS = maxTimeMS;
        this.namespace = firstBatch.getNamespace();
        this.firstBatch = firstBatch;
        this.limit = limit;
        this.batchSize = batchSize;
        this.decoder = decoder;
        this.cursor = firstBatch.getCursor();
        if (this.cursor != null) {
            notNull("connectionSource", connectionSource);
            notNull("connection", connection);
        }
        if (connectionSource != null) {
            this.connectionSource = connectionSource.retain();
        } else {
            this.connectionSource = null;
        }
        this.count += firstBatch.getResults().size();
        if (limitReached()) {
             killCursor(connection);
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            killCursor(null);
        }
    }

    @Override
    public void next(final SingleResultCallback<List<T>> callback) {
        isTrue("open", !closed);
        // may be empty for a tailable cursor
        if (firstBatch  != null && !firstBatch.getResults().isEmpty()) {
            List<T> results = firstBatch.getResults();
            firstBatch = null;
            callback.onResult(results, null);
        } else {
            if (cursor == null) {
                close();
                callback.onResult(null, null);
            } else {
                getMore(callback);
            }
        }
    }

    @Override
    public void setBatchSize(final int batchSize) {
        isTrue("open", !closed);
        this.batchSize = batchSize;
    }

    @Override
    public int getBatchSize() {
        isTrue("open", !closed);
        return batchSize;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    ServerCursor getServerCursor() {
        return cursor;
    }

    private boolean limitReached() {
        return Math.abs(limit) != 0 && count >= Math.abs(limit);
    }

    private void getMore(final SingleResultCallback<List<T>> callback) {
        connectionSource.getConnection(new SingleResultCallback<AsyncConnection>() {
            @Override
            public void onResult(final AsyncConnection connection, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    getMore(connection, callback);
                }
            }
        });
    }

    private void getMore(final AsyncConnection connection, final SingleResultCallback<List<T>> callback) {
        if (serverIsAtLeastVersionThreeDotTwo(connection.getDescription())) {
            connection.commandAsync(namespace.getDatabaseName(), asGetMoreCommandDocument(), false,
                                    new NoOpFieldNameValidator(), CommandResultDocumentCodec.create(decoder, "nextBatch"),
                                    new CommandResultSingleResultCallback(connection, callback));

        } else {
            connection.getMoreAsync(namespace, cursor.getId(), getNumberToReturn(limit, batchSize, count),
                                    decoder, new QueryResultSingleResultCallback(connection, callback));
        }
    }

    private BsonDocument asGetMoreCommandDocument() {
        BsonDocument document = new BsonDocument("getMore", new BsonInt64(cursor.getId()))
                                .append("collection", new BsonString(namespace.getCollectionName()));

        int batchSizeForGetMoreCommand = Math.abs(getNumberToReturn(limit, this.batchSize, count));
        if (batchSizeForGetMoreCommand != 0) {
            document.append("batchSize", new BsonInt32(batchSizeForGetMoreCommand));
        }
        if (maxTimeMS != 0) {
            document.append("maxTimeMS", new BsonInt64(maxTimeMS));
        }
        return document;
    }

    private void killCursor(final AsyncConnection connection) {
        if (cursor != null) {
            final ServerCursor localCursor = cursor;
            final AsyncConnectionSource localConnectionSource = connectionSource;
            cursor = null;
            connectionSource = null;
            if (connection != null) {
                connection.retain();
                killCursorAsynchronouslyAndReleaseConnectionAndSource(connection, localCursor, localConnectionSource);
            } else {
                localConnectionSource.getConnection(new SingleResultCallback<AsyncConnection>() {
                    @Override
                    public void onResult(final AsyncConnection connection, final Throwable connectionException) {
                        if (connectionException == null) {
                            killCursorAsynchronouslyAndReleaseConnectionAndSource(connection, localCursor, localConnectionSource);
                        }
                    }
                });
            }
        } else if (connectionSource != null) {
            connectionSource.release();
            connectionSource = null;
        }
    }

    private void killCursorAsynchronouslyAndReleaseConnectionAndSource(final AsyncConnection connection, final ServerCursor localCursor,
                                                                       final AsyncConnectionSource localConnectionSource) {
        connection.killCursorAsync(namespace, singletonList(localCursor.getId()), new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                connection.release();
                localConnectionSource.release();
            }
        });
    }

    private void handleGetMoreQueryResult(final AsyncConnection connection, final SingleResultCallback<List<T>> callback,
                                          final QueryResult<T> result) {
        if (result.getResults().isEmpty() && result.getCursor() != null) {
            getMore(connection, callback);
        } else {
            cursor = result.getCursor();
            count += result.getResults().size();
            if (limitReached()) {
                killCursor(connection);
            }
            connection.release();
            if (result.getResults().isEmpty()) {
                callback.onResult(null, null);
            } else {
                callback.onResult(result.getResults(), null);
            }
        }
    }

    private class CommandResultSingleResultCallback implements SingleResultCallback<BsonDocument> {
        private final AsyncConnection connection;
        private final SingleResultCallback<List<T>> callback;

        public CommandResultSingleResultCallback(final AsyncConnection connection, final SingleResultCallback<List<T>> callback) {
            this.connection = connection;
            this.callback = errorHandlingCallback(callback, LOGGER);
        }

        @Override
        public void onResult(final BsonDocument result, final Throwable t) {
            if (t != null) {
                Throwable translatedException = t instanceof MongoCommandException
                                                ? translateCommandException((MongoCommandException) t, cursor)
                                                : t;
                connection.release();
                close();
                callback.onResult(null, translatedException);
            } else {
                QueryResult<T> queryResult = getMoreCursorDocumentToQueryResult(result.getDocument("cursor"),
                                                                                connectionSource.getServerDescription().getAddress());
                handleGetMoreQueryResult(connection, callback, queryResult);
            }
        }
    }

    private class QueryResultSingleResultCallback implements SingleResultCallback<QueryResult<T>> {
        private final AsyncConnection connection;
        private final SingleResultCallback<List<T>> callback;

        public QueryResultSingleResultCallback(final AsyncConnection connection, final SingleResultCallback<List<T>> callback) {
            this.connection = connection;
            this.callback = errorHandlingCallback(callback, LOGGER);
        }

        @Override
        public void onResult(final QueryResult<T> result, final Throwable t) {
            if (t != null) {
                connection.release();
                close();
                callback.onResult(null, t);
            } else {
                handleGetMoreQueryResult(connection, callback, result);
            }
        }
    }
}
