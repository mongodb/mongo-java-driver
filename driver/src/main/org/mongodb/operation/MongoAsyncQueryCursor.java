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

package org.mongodb.operation;

import org.mongodb.Block;
import org.mongodb.CancellationToken;
import org.mongodb.Decoder;
import org.mongodb.MongoAsyncCursor;
import org.mongodb.MongoException;
import org.mongodb.MongoInternalException;
import org.mongodb.MongoNamespace;
import org.mongodb.ServerCursor;
import org.mongodb.binding.AsyncConnectionSource;
import org.mongodb.connection.Connection;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.diagnostics.Loggers;
import org.mongodb.diagnostics.logging.Logger;
import org.mongodb.protocol.GetMoreDiscardProtocol;
import org.mongodb.protocol.GetMoreProtocol;
import org.mongodb.protocol.GetMoreReceiveProtocol;
import org.mongodb.protocol.QueryResult;

// TODO: kill cursor on early breakout
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
    public SingleResultFuture<Void> forEach(final Block<? super T> block) {
        return forEach(block, CancellationToken.notCancellable());
    }

    @Override
    public SingleResultFuture<Void> forEach(final Block<? super T> block, final CancellationToken cancellationToken) {
        SingleResultFuture<Void> future = new SingleResultFuture<Void>();
        new QueryResultSingleResultCallback(block, future, cancellationToken).onResult(firstBatch, null);
        return future;
    }

    private void close(final int responseTo, final SingleResultFuture<Void> future, final MongoException e) {
        if (isExhaust()) {
            handleExhaustCleanup(responseTo, future, e);
        } else {
            releaseConnectionSource();
            LOGGER.trace("Initializing forEach future " + (e == null ? "" : " with exception " + e));
            future.init(null, e);
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
                future.init(null, e);
            }
        });
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
        private final CancellationToken cancellationToken;
        private volatile Boolean limitReached;

        public QueryResultSingleResultCallback(final Block<? super T> block, final SingleResultFuture<Void> future,
                                               final Connection connection, final CancellationToken cancellationToken) {
            this.block = block;
            this.future = future;
            this.cancellationToken = cancellationToken;
            this.connection = connection;
            this.limitReached = false;
        }

        public QueryResultSingleResultCallback(final Block<? super T> block, final SingleResultFuture<Void> future,
                                               final CancellationToken cancellationToken) {
            this(block, future, null, cancellationToken);
        }

        @Override
        public void onResult(final QueryResult<T> result, final MongoException e) {
            if (!isExhaust() & connection != null) {
                connection.release();
            }
            if (e != null) {
                close(0, future, e);
                return;
            }

            cursor = result.getCursor();

            limitReached = false;
            MongoException exceptionFromApply = null;
            try {
                for (final T cur : result.getResults()) {
                    numFetchedSoFar++;
                    limitReached = limit > 0 && numFetchedSoFar > limit;
                    if (limitReached || isCancelled()) {
                        break;
                    } else {
                        LOGGER.trace("Applying block to " + cur);
                        block.apply(cur);
                    }
                }
            } catch (Throwable e1) {
                LOGGER.trace("Applied block threw exception: " + e1);
                exceptionFromApply = new MongoInternalException("Exception thrown by client while iterating over cursor", e1);
            }

            if (cursor == null || limitReached || isCancelled() || exceptionFromApply != null) {
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
                                .register(new QueryResultSingleResultCallback(block, future, connection, cancellationToken));
                            }
                        }
                    });
                }
            }
        }

        /**
         * Checks if the cancellationToken has been cancelled or if the future has been cancelled.
         *
         * If either has been cancelled then don't fetch or apply any more results.
         *
         * @return if either has been cancelled.
         */
        private boolean isCancelled() {
            return future.isCancelled() || cancellationToken.cancellationRequested();
        }

    }
}
