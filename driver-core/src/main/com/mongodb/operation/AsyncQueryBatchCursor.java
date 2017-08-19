/*
 * Copyright (c) 2008-2017 MongoDB, Inc.
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
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.ServerCursor;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.QueryResult;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.operation.CursorHelper.getNumberToReturn;
import static com.mongodb.operation.OperationHelper.LOGGER;
import static com.mongodb.operation.OperationHelper.getMoreCursorDocumentToQueryResult;
import static com.mongodb.operation.OperationHelper.serverIsAtLeastVersionThreeDotTwo;
import static com.mongodb.operation.QueryHelper.translateCommandException;
import static java.lang.String.format;
import static java.util.Collections.singletonList;

class AsyncQueryBatchCursor<T> implements AsyncBatchCursor<T> {

    private final MongoNamespace namespace;
    private final int limit;
    private final Decoder<T> decoder;
    private final long maxTimeMS;
    private final AsyncConnectionSource connectionSource;
    private final AtomicBoolean isClosed = new AtomicBoolean();
    private final AtomicReference<ServerCursor> cursor;
    private final Object lock = new Object();
    private volatile QueryResult<T> firstBatch;
    private volatile int batchSize;
    private volatile int count;

    AsyncQueryBatchCursor(final QueryResult<T> firstBatch, final int limit, final int batchSize, final long maxTimeMS,
                          final Decoder<T> decoder, final AsyncConnectionSource connectionSource, final AsyncConnection connection) {
        isTrueArgument("maxTimeMS >= 0", maxTimeMS >= 0);
        this.maxTimeMS = maxTimeMS;
        this.namespace = firstBatch.getNamespace();
        this.firstBatch = firstBatch;
        this.limit = limit;
        this.batchSize = batchSize;
        this.decoder = decoder;
        this.cursor = new AtomicReference<ServerCursor>(firstBatch.getCursor());
        this.connectionSource = notNull("connectionSource", connectionSource);
        this.count += firstBatch.getResults().size();

        connectionSource.retain();
        if (firstBatch.getCursor() != null && limitReached()) {
            connectionSource.retain();
            killCursor(connection);
        }
    }

    @Override
    public void close() {
        if (!isClosed.getAndSet(true)) {
            killCursorOnClose();
        }
    }

    @Override
    public void next(final SingleResultCallback<List<T>> callback) {
        next(callback, false);
    }

    @Override
    public void tryNext(final SingleResultCallback<List<T>> callback) {
        next(callback, true);
    }

    @Override
    public void setBatchSize(final int batchSize) {
        isTrue("open", !isClosed.get());
        this.batchSize = batchSize;
    }

    @Override
    public int getBatchSize() {
        isTrue("open", !isClosed.get());
        return batchSize;
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    private void next(final SingleResultCallback<List<T>> callback, final boolean tryNext) {
        if (isClosed()) {
            callback.onResult(null, new MongoException(format("%s called after the cursor was closed.",
                    tryNext ? "tryNext()" : "next()")));
        } else if (firstBatch != null && (tryNext || !firstBatch.getResults().isEmpty())) {
            // May be empty for a tailable cursor
            List<T> results = firstBatch.getResults();
            if (tryNext && results.isEmpty()) {
                results = null;
            }
            firstBatch = null;
            callback.onResult(results, null);
        } else {
            ServerCursor localCursor = getCursorForNext();
            if (localCursor == null) {
                callback.onResult(null, null);
            } else {
                getMore(localCursor, callback, tryNext);
            }
        }
    }

    private boolean limitReached() {
        return Math.abs(limit) != 0 && count >= Math.abs(limit);
    }

    private void getMore(final ServerCursor cursor, final SingleResultCallback<List<T>> callback, final boolean tryNext) {
        connectionSource.getConnection(new SingleResultCallback<AsyncConnection>() {
            @Override
            public void onResult(final AsyncConnection connection, final Throwable t) {
                if (t != null) {
                    connectionSource.release();
                    callback.onResult(null, t);
                } else {
                    getMore(connection, cursor, callback, tryNext);
                }
            }
        });
    }

    private void getMore(final AsyncConnection connection, final ServerCursor cursor, final SingleResultCallback<List<T>> callback,
                         final boolean tryNext) {
        if (serverIsAtLeastVersionThreeDotTwo(connection.getDescription())) {
            connection.commandAsync(namespace.getDatabaseName(), asGetMoreCommandDocument(cursor.getId()), ReadPreference.primary(),
                                    new NoOpFieldNameValidator(), CommandResultDocumentCodec.create(decoder, "nextBatch"),
                                    new CommandResultSingleResultCallback(connection, cursor, callback, tryNext));

        } else {
            connection.getMoreAsync(namespace, cursor.getId(), getNumberToReturn(limit, batchSize, count),
                                    decoder, new QueryResultSingleResultCallback(connection, callback, tryNext));
        }
    }

    private BsonDocument asGetMoreCommandDocument(final long cursorId) {
        BsonDocument document = new BsonDocument("getMore", new BsonInt64(cursorId))
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

    private void killCursorOnClose() {
        final ServerCursor localCursor = getServerCursor();
        if (localCursor != null) {
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
        } else {
            connectionSource.release();
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
            connection.commandAsync(namespace.getDatabaseName(), asKillCursorsCommandDocument(localCursor), ReadPreference.primary(),
                    new NoOpFieldNameValidator(), new BsonDocumentCodec(), new SingleResultCallback<BsonDocument>() {
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


    private void handleGetMoreQueryResult(final AsyncConnection connection, final SingleResultCallback<List<T>> callback,
                                          final QueryResult<T> result, final boolean tryNext) {
        if (isClosed()) {
            connection.release();
            connectionSource.release();
            callback.onResult(null, new MongoException(format("The cursor was closed before %s completed.",
                    tryNext ? "tryNext()" : "next()")));
            return;
        }

        cursor.getAndSet(result.getCursor());
        if (!tryNext && result.getResults().isEmpty() && result.getCursor() != null) {
            getMore(connection, result.getCursor(), callback, tryNext);
        } else {
            count += result.getResults().size();
            if (limitReached()) {
                killCursor(connection);
                connection.release();
            } else {
                connection.release();
                connectionSource.release();
            }

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
        private final boolean tryNext;

        CommandResultSingleResultCallback(final AsyncConnection connection, final ServerCursor cursor,
                                          final SingleResultCallback<List<T>> callback, final boolean tryNext) {
            this.connection = connection;
            this.cursor = cursor;
            this.callback = errorHandlingCallback(callback, LOGGER);
            this.tryNext = tryNext;
        }

        @Override
        public void onResult(final BsonDocument result, final Throwable t) {
            if (t != null) {
                Throwable translatedException = t instanceof MongoCommandException
                                                ? translateCommandException((MongoCommandException) t, cursor)
                                                : t;
                connection.release();
                connectionSource.release();
                callback.onResult(null, translatedException);
            } else {
                QueryResult<T> queryResult = getMoreCursorDocumentToQueryResult(result.getDocument("cursor"),
                        connection.getDescription().getServerAddress());
                handleGetMoreQueryResult(connection, callback, queryResult, tryNext);
            }
        }
    }

    private class QueryResultSingleResultCallback implements SingleResultCallback<QueryResult<T>> {
        private final AsyncConnection connection;
        private final SingleResultCallback<List<T>> callback;
        private final boolean tryNext;

        QueryResultSingleResultCallback(final AsyncConnection connection, final SingleResultCallback<List<T>> callback,
                                        final boolean tryNext) {
            this.connection = connection;
            this.callback = errorHandlingCallback(callback, LOGGER);
            this.tryNext = tryNext;
        }

        @Override
        public void onResult(final QueryResult<T> result, final Throwable t) {
            if (t != null) {
                connection.release();
                connectionSource.release();
                callback.onResult(null, t);
            } else {
                handleGetMoreQueryResult(connection, callback, result, tryNext);
            }
        }
    }

    private ServerCursor getCursorForNext() {
        ServerCursor localCursor;
        synchronized (lock) {
            localCursor = cursor.get();
            if (localCursor == null) {
                isClosed.getAndSet(true);
                connectionSource.release();
            } else {
                connectionSource.retain();
            }
        }
        return localCursor;
    }

    ServerCursor getServerCursor() {
        ServerCursor localCursor;
        synchronized (lock) {
            localCursor = cursor.get();
        }
        return localCursor;
    }

}
