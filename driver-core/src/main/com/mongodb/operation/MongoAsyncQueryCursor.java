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

import com.mongodb.Block;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoNamespace;
import com.mongodb.ServerCursor;
import com.mongodb.async.MongoAsyncCursor;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.connection.Connection;
import com.mongodb.connection.QueryResult;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import org.bson.codecs.Decoder;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.CursorHelper.getNumberToReturn;
import static java.util.Arrays.asList;


class MongoAsyncQueryCursor<T> implements MongoAsyncCursor<T> {

    private static final Logger LOGGER = Loggers.getLogger("operation.query.cursor");

    private final MongoNamespace namespace;
    private final QueryResult<T> firstBatch;
    private final int limit;
    private final int batchSize;
    private final Decoder<T> decoder;
    private final AsyncConnectionSource connectionSource;
    private int numFetchedSoFar;
    private ServerCursor cursor;
    private boolean closed;

    // For normal queries
    MongoAsyncQueryCursor(final MongoNamespace namespace, final QueryResult<T> firstBatch, final int limit, final int batchSize,
                          final Decoder<T> decoder, final AsyncConnectionSource connectionSource) {
        this.namespace = namespace;
        this.firstBatch = firstBatch;
        this.limit = limit;
        this.batchSize = batchSize;
        this.decoder = decoder;
        this.connectionSource = notNull("connectionSource", connectionSource).retain();
    }

    @Override
    public MongoFuture<Void> forEach(final Block<? super T> block) {
        SingleResultFuture<Void> retVal = new SingleResultFuture<Void>();
        new QueryResultSingleResultCallback(block, retVal).onResult(firstBatch, null);
        return retVal;
    }

    private void close(final SingleResultFuture<Void> future, final MongoException e) {
        killCursorAndCompleteFuture(future, e);
    }

    private void killCursorAndCompleteFuture(final SingleResultFuture<Void> future, final MongoException e) {
        if (cursor != null) {
            connectionSource.getConnection().register(new SingleResultCallback<Connection>() {
                @Override
                public void onResult(final Connection connection, final MongoException connectionException) {
                    connection.killCursorAsync(asList(cursor.getId()))
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
            if (connection != null) {
                connection.release();
            }
            if (e != null) {
                close(future, e);
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
                close(future, exceptionFromApply);
            } else {
                // get more results
                connectionSource.getConnection().register(new SingleResultCallback<Connection>() {
                    @Override
                    public void onResult(final Connection connection, final MongoException e) {
                        if (e != null) {
                            close(future, e);
                        } else {
                            connection.getMoreAsync(namespace, result.getCursor().getId(),
                                                    getNumberToReturn(limit, batchSize, numFetchedSoFar),
                                                    decoder)
                                      .register(new QueryResultSingleResultCallback(block, future, connection));
                        }
                    }
                });
            }
        }
    }
}
