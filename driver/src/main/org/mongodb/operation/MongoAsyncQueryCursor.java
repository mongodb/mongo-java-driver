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

import org.mongodb.AsyncBlock;
import org.mongodb.Decoder;
import org.mongodb.MongoAsyncCursor;
import org.mongodb.MongoException;
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
// TODO: Report errors in callback
class MongoAsyncQueryCursor<T> implements MongoAsyncCursor<T> {
    private static final Logger LOGGER = Loggers.getLogger("operation");

    private final MongoNamespace namespace;
    private final QueryResult<T> firstBatch;
    private final int limit;
    private final int batchSize;
    private final Decoder<T> decoder;
    private AsyncConnectionSource connectionSource;
    private Connection exhaustConnection;
    private long numFetchedSoFar;
    private ServerCursor cursor;
    private AsyncBlock<? super T> block;

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
    public void start(final AsyncBlock<? super T> block) {
        this.block = block;
        new QueryResultSingleResultCallback(null).onResult(firstBatch, null);
    }

    private void close(final int responseTo) {
        if (isExhaust()) {
            handleExhaustCleanup(responseTo);
        } else {
            try {
                block.done();
            } finally {
                releaseConnectionSource();
            }
        }
    }

    private boolean isExhaust() {
        return exhaustConnection != null;
    }

    private void handleExhaustCleanup(final int responseTo) {
        new GetMoreDiscardProtocol(cursor != null ? cursor.getId() : 0, responseTo)
        .executeAsync(exhaustConnection)
        .register(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final MongoException e) {
                try {
                    block.done();
                } finally {
                    exhaustConnection.release();
                    releaseConnectionSource();
                }
            }
        });
    }

    private void releaseConnectionSource() {
        if (connectionSource != null) {
            connectionSource.release();
        }
    }

    private class QueryResultSingleResultCallback implements SingleResultCallback<QueryResult<T>> {
        private final Connection connection;

        public QueryResultSingleResultCallback(final Connection connection) {
            this.connection = connection;
        }

        @Override
        public void onResult(final QueryResult<T> result, final MongoException e) {
            if (!isExhaust() & connection != null) {
                connection.release();
            }
            if (e != null) {
                close(0);
                return;
            }

            cursor = result.getCursor();

            boolean breakEarly = false;

            try {
                for (final T cur : result.getResults()) {
                    numFetchedSoFar++;
                    if (limit > 0 && numFetchedSoFar > limit) {
                        breakEarly = true;
                        break;
                    }
                    block.apply(cur);

                }
            } catch (Exception e1) {
                breakEarly = true;
                LOGGER.error(e1.getMessage(), e1);
            }

            if (result.getCursor() == null || breakEarly) {
                close(result.getRequestId());
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
                                close(0);
                            } else {
                                new GetMoreProtocol<T>(namespace, new GetMore(result.getCursor(), limit, batchSize, numFetchedSoFar),
                                                       decoder)
                                .executeAsync(connection)
                                .register(new QueryResultSingleResultCallback(connection));
                            }
                        }
                    });
                }
            }
        }
    }
}
