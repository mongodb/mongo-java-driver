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
import org.mongodb.connection.Connection;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.diagnostics.Loggers;
import org.mongodb.diagnostics.logging.Logger;
import org.mongodb.protocol.GetMoreDiscardProtocol;
import org.mongodb.protocol.GetMoreProtocol;
import org.mongodb.protocol.GetMoreReceiveProtocol;
import org.mongodb.protocol.QueryResult;
import org.mongodb.session.ServerConnectionProvider;

import java.util.EnumSet;

// TODO: kill cursor on early breakout
// TODO: Report errors in callback
class MongoAsyncQueryCursor<T> implements MongoAsyncCursor<T> {
    private static final Logger LOGGER = Loggers.getLogger("operation");

    private final MongoNamespace namespace;
    private final QueryResult<T> firstBatch;
    private final EnumSet<QueryFlag> queryFlags;
    private final int limit;
    private final int batchSize;
    private final Decoder<T> decoder;
    private ServerConnectionProvider serverConnectionProvider;
    private Connection exhaustConnection;
    private ServerDescription serverDescription;
    private long numFetchedSoFar;
    private ServerCursor cursor;
    private AsyncBlock<? super T> block;

    // For normal queries
    MongoAsyncQueryCursor(final MongoNamespace namespace, final QueryResult<T> firstBatch, final EnumSet<QueryFlag> queryFlags,
                          final int limit, final int batchSize, final Decoder<T> decoder, final ServerConnectionProvider provider) {
        this(namespace, firstBatch, queryFlags, limit, batchSize, decoder, provider, null, null);
    }

    // For exhaust queries
    MongoAsyncQueryCursor(final MongoNamespace namespace, final QueryResult<T> firstBatch, final EnumSet<QueryFlag> queryFlags,
                          final int limit, final int batchSize, final Decoder<T> decoder, final Connection exhaustConnection,
                          final ServerDescription serverDescription) {
        this(namespace, firstBatch, queryFlags, limit, batchSize, decoder, null, exhaustConnection, serverDescription);
    }

    private MongoAsyncQueryCursor(final MongoNamespace namespace, final QueryResult<T> firstBatch, final EnumSet<QueryFlag> queryFlags,
                                  final int limit, final int batchSize, final Decoder<T> decoder, final ServerConnectionProvider provider,
                                  final Connection exhaustConnection, final ServerDescription serverDescription) {
        this.namespace = namespace;
        this.firstBatch = firstBatch;
        this.queryFlags = queryFlags;
        this.limit = limit;
        this.batchSize = batchSize;
        this.decoder = decoder;
        this.serverConnectionProvider = provider;
        this.exhaustConnection = exhaustConnection;
        this.serverDescription = serverDescription != null ? serverDescription : serverConnectionProvider.getServerDescription();
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
            block.done();
        }
    }

    private boolean isExhaust() {
        return queryFlags.contains(QueryFlag.Exhaust);
    }

    private void handleExhaustCleanup(final int responseTo) {
        new GetMoreDiscardProtocol(cursor != null ? cursor.getId() : 0, responseTo)
        .executeAsync(exhaustConnection, serverDescription)
        .register(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final MongoException e) {
                try {
                    block.done();
                } finally {
                    exhaustConnection.close();
                }
            }
        });
    }

    private class QueryResultSingleResultCallback implements SingleResultCallback<QueryResult<T>> {
        private final Connection connection;

        public QueryResultSingleResultCallback(final Connection connection) {
            this.connection = connection;
        }

        @Override
        public void onResult(final QueryResult<T> result, final MongoException e) {
            if (!isExhaust() & connection != null) {
                connection.close();
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
                if (queryFlags.contains(QueryFlag.Exhaust)) {
                    new GetMoreReceiveProtocol<T>(decoder, result.getRequestId())
                    .executeAsync(exhaustConnection, serverDescription)
                    .register(this);
                } else {
                    serverConnectionProvider.getConnectionAsync().register(new SingleResultCallback<Connection>() {
                        @Override
                        public void onResult(final Connection connection, final MongoException e) {
                            if (e != null) {
                                close(0);
                            } else {
                                new GetMoreProtocol<T>(namespace, new GetMore(result.getCursor(), limit, batchSize, numFetchedSoFar),
                                                       decoder)
                                .executeAsync(connection, serverDescription)
                                .register(new QueryResultSingleResultCallback(connection));
                            }
                        }
                    });
                }
            }
        }
    }
}
