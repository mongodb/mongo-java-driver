/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.connection.Connection;
import com.mongodb.connection.SingleResultCallback;
import com.mongodb.diagnostics.Loggers;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.protocol.GetMoreDiscardProtocol;
import com.mongodb.protocol.GetMoreProtocol;
import com.mongodb.protocol.GetMoreReceiveProtocol;
import com.mongodb.protocol.KillCursor;
import com.mongodb.protocol.KillCursorProtocol;
import com.mongodb.protocol.QueryResult;
import org.bson.codecs.Decoder;
import org.mongodb.Block;
import org.mongodb.MongoAsyncCursor;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.ServerCursor;


class MongoAsyncQueryCursor<T> implements MongoAsyncCursor<T> {

    private static final Logger LOGGER = Loggers.getLogger("operation.query.cursor");

    private final MongoNamespace namespace;
    private final QueryResult<T> firstBatch;
    private final int limit;
    private final int batchSize;
    private final Decoder<T> decoder;
    private final AsyncConnectionSource connectionSource;
    private final Connection exhaustConnection;
    private long numFetchedSoFar;
    private ServerCursor cursor;
    private boolean closed;

    // For normal queries
    MongoAsyncQueryCursor(final MongoNamespace namespace, final QueryResult<T> firstBatch, final int limit, final int batchSize,
                          final Decoder<T> decoder, final AsyncConnectionSource connectionSource) {
        this(namespace, firstBatch, limit, batchSize, decoder, connectionSource, null);
    }

    // For exhaust queries
    MongoAsyncQueryCursor(final MongoNamespace namespace, final QueryResult<T> firstBatch, final int limit, final int batchSize,
                          final Decoder<T> decoder, final Connection exhaustConnection) {
        this(namespace, firstBatch, limit, batchSize, decoder, null, exhaustConnection);
    }

    private MongoAsyncQueryCursor(final MongoNamespace namespace, final QueryResult<T> firstBatch, final int limit, final int batchSize,
                                  final Decoder<T> decoder, final AsyncConnectionSource connectionSource,
                                  final Connection exhaustConnection) {
        this.namespace = namespace;
        this.firstBatch = firstBatch;
        this.limit = limit;
        this.batchSize = batchSize;
        this.decoder = decoder;
        this.connectionSource = connectionSource;
        if (this.connectionSource != null) {
            this.connectionSource.retain();
        }
        this.exhaustConnection = exhaustConnection;
        if (this.exhaustConnection != null) {
            this.exhaustConnection.retain();
        }
    }

    @Override
    public MongoFuture<Void> forEach(final Block<? super T> block) {
        SingleResultFuture<Void> retVal = new SingleResultFuture<Void>();
        new QueryResultSingleResultCallback(block, retVal).onResult(firstBatch, null);
        return retVal;
    }

    private void close(final int responseTo, final SingleResultFuture<Void> future, final MongoException e) {
        if (isExhaust()) {
            handleExhaustCleanup(responseTo, future, e);
        } else {
            killCursorAndCompleteFuture(future, e);
        }
    }

    private boolean isExhaust() {
        return exhaustConnection != null;
    }

    private void handleExhaustCleanup(final int responseTo, final SingleResultFuture<Void> future, final MongoException e) {
        new GetMoreDiscardProtocol(cursor != null ? cursor.getId() : 0, responseTo)
        .executeAsync(exhaustConnection)
        .register(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final MongoException exhaustException) {
                exhaustConnection.release();
                releaseConnectionSource();
                closed = true;
                future.init(null, e == null ? exhaustException : e);
            }
        });
    }

    private void killCursorAndCompleteFuture(final SingleResultFuture<Void> future, final MongoException e) {
        if (cursor != null) {
            connectionSource.getConnection().register(new SingleResultCallback<Connection>() {
                @Override
                public void onResult(final Connection connection, final MongoException connectionException) {
                    new KillCursorProtocol(new KillCursor(cursor))
                        .executeAsync(connection)
                        .register(new SingleResultCallback<Void>() {
                            @Override
                            public void onResult(final Void result, final MongoException killException) {
                                connection.release();
                                releaseConnectionSource();
                                closed = true;

                                MongoException exception = e;
                                if (exception == null) {
                                    exception = connectionException;
                                }
                                if (exception == null) {
                                    exception = killException;
                                }

                                LOGGER.trace("Initializing forEach future " + (exception == null ? "" : " with  exception " + exception));
                                future.init(null, exception);
                            }
                        });
                }
            });
        } else {
            closed = true;
            LOGGER.trace("Initializing forEach future " + (e == null ? "" : " with exception " + e));
            future.init(null, e);
        }
    }

    private void releaseConnectionSource() {
        if (connectionSource != null) {
            connectionSource.release();
        }
    }

    private class QueryResultSingleResultCallback implements SingleResultCallback<QueryResult<T>> {
        private final SingleResultFuture<Void> future;
        private final Connection connection;
        private final Block<? super T> block;

        public QueryResultSingleResultCallback(final Block<? super T> block, final SingleResultFuture<Void> future,
                                               final Connection connection) {
            this.block = block;
            this.future = future;
            this.connection = connection;
        }

        public QueryResultSingleResultCallback(final Block<? super T> block, final SingleResultFuture<Void> future) {
            this(block, future, null);
        }

        @Override
        public void onResult(final QueryResult<T> result, final MongoException e) {
            if (closed) {
                throw new IllegalStateException("Cursor has been closed");
            }
            if (!isExhaust() & connection != null) {
                connection.release();
            }
            if (e != null) {
                close(0, future, e);
                return;
            }

            cursor = result.getCursor();

            boolean breakEarly = false;
            MongoException exceptionFromApply = null;
            try {
                for (final T cur : result.getResults()) {
                    numFetchedSoFar++;
                    if (limit > 0 && numFetchedSoFar > limit) {
                        breakEarly = true;
                        break;
                    }
                    LOGGER.trace("Applying block to " + cur);
                    block.apply(cur);

                }
            } catch (Throwable e1) {
                LOGGER.trace("Applied block threw exception: " + e1);
                breakEarly = true;
                exceptionFromApply = new MongoInternalException("Exception thrown by client while iterating over cursor", e1);
            }

            if (result.getCursor() == null || breakEarly) {
                close(result.getRequestId(), future, exceptionFromApply);
            } else {
                // get more results
                if (isExhaust()) {
                    new GetMoreReceiveProtocol<T>(decoder, result.getRequestId())
                    .executeAsync(exhaustConnection)
                    .register(this);
                } else {
                    connectionSource.getConnection().register(new SingleResultCallback<Connection>() {
                        @Override
                        public void onResult(final Connection connection, final MongoException e) {
                            if (e != null) {
                                close(0, future, e);
                            } else {
                                new GetMoreProtocol<T>(namespace, new GetMore(result.getCursor(), limit, batchSize, numFetchedSoFar),
                                                       decoder)
                                .executeAsync(connection)
                                .register(new QueryResultSingleResultCallback(block, future, connection));
                            }
                        }
                    });
                }
            }
        }
    }
}
