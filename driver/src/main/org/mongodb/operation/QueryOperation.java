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

import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoAsyncCursor;
import org.mongodb.MongoCursor;
import org.mongodb.MongoException;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.ReadPreference;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.protocol.QueryProtocol;
import org.mongodb.protocol.QueryResult;
import org.mongodb.session.ServerConnectionProvider;
import org.mongodb.session.Session;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.connection.ServerType.SHARD_ROUTER;
import static org.mongodb.operation.OperationHelper.getConnectionProvider;
import static org.mongodb.operation.OperationHelper.getConnectionProviderAsync;

/**
 * An operation that queries a collection using the provided criteria.
 *
 * @param <T> the document type
 *
 * @since 3.0
 */
public class QueryOperation<T> implements AsyncOperation<MongoAsyncCursor<T>>, Operation<MongoCursor<T>> {
    private final Find find;
    private final Encoder<Document> queryEncoder;
    private final Decoder<T> resultDecoder;
    private final MongoNamespace namespace;

    public QueryOperation(final MongoNamespace namespace, final Find find, final Encoder<Document> queryEncoder,
                          final Decoder<T> resultDecoder) {
        this.namespace = notNull("namespace", namespace);
        this.find = notNull("find", find);
        this.queryEncoder = notNull("queryEncoder", queryEncoder);
        this.resultDecoder = notNull("resultDecoder", resultDecoder);
    }

    @Override
    public MongoCursor<T> execute(final Session session) {
        ServerConnectionProvider connectionProvider = getConnectionProvider(find.getReadPreference(), session);
        Connection connection = connectionProvider.getConnection();
        try {
            QueryResult<T> queryResult = asQueryProtocol(connectionProvider.getServerDescription()).execute(connection,
                                                                                                            connectionProvider
                                                                                                            .getServerDescription());
            if (isExhaustCursor()) {
                return new MongoQueryCursor<T>(namespace, queryResult, find.getLimit(), find.getBatchSize(),
                                               resultDecoder, connection, connectionProvider.getServerDescription());
            } else {
                return new MongoQueryCursor<T>(namespace, queryResult, find.getLimit(), find.getBatchSize(),
                                               resultDecoder, connectionProvider);
            }
        } finally {
            if (!isExhaustCursor()) {
                connection.close();
            }
        }
    }

    public MongoFuture<MongoAsyncCursor<T>> executeAsync(final Session session) {
        final SingleResultFuture<MongoAsyncCursor<T>> future = new SingleResultFuture<MongoAsyncCursor<T>>();
        getConnectionProviderAsync(find.getReadPreference(), session)
        .register(new SingleResultCallback<ServerConnectionProvider>() {
            @Override
            public void onResult(final ServerConnectionProvider connectionProvider, final MongoException e) {
                if (e != null) {
                    future.init(null, e);
                } else {
                    connectionProvider.getConnectionAsync().register(new SingleResultCallback<Connection>() {
                        @Override
                        public void onResult(final Connection connection, final MongoException e) {
                            asQueryProtocol(connectionProvider.getServerDescription())
                            .executeAsync(connection, connectionProvider.getServerDescription())
                            .register(new SingleResultCallback<QueryResult<T>>() {
                                @Override
                                public void onResult(final QueryResult<T> queryResult, final MongoException e) {
                                    try {
                                        if (e != null) {
                                            future.init(null, e);
                                        } else {
                                            MongoAsyncQueryCursor<T> cursor = new MongoAsyncQueryCursor<T>(namespace,
                                                                                      queryResult,
                                                                                      find.getLimit(),
                                                                                      find.getBatchSize(),
                                                                                      resultDecoder,
                                                                                      connectionProvider);
                                            future.init(cursor, null);
                                        }
                                    } finally {
                                            connection.close();
                                    }
                                }
                            });
                        }
                    });
                }
            }
        });
        return future;
    }

    private QueryProtocol<T> asQueryProtocol(final ServerDescription serverDescription) {
        return new QueryProtocol<T>(namespace, find.getFlags(), find.getSkip(),
                                    find.getNumberToReturn(), asDocument(serverDescription),
                                    find.getFields(), queryEncoder, resultDecoder);
    }

    private Document asDocument(final ServerDescription serverDescription) {
        Document document = new Document();
        document.put("$query", find.getFilter());
        if (find.getOrder() != null && !find.getOrder().isEmpty()) {
            document.put("$orderby", find.getOrder());
        }
        if (find.isSnapshotMode()) {
            document.put("$snapshot", true);
        }
        if (find.isExplain()) {
            document.put("$explain", true);
        }
        if (serverDescription.getType() == SHARD_ROUTER
            && find.getReadPreference() != null && !find.getReadPreference().equals(ReadPreference.primary())) {
            document.put("$readPreference", find.getReadPreference().toDocument());
        }

        if (find.getHint() != null) {
            document.put("$hint", find.getHint().getValue());
        }

        if (find.getOptions().getComment() != null) {
            document.put("$comment", find.getOptions().getComment());
        }

        if (find.getOptions().getMax() != null) {
            document.put("$max", find.getOptions().getMax());
        }

        if (find.getOptions().getMin() != null) {
            document.put("$min", find.getOptions().getMin());
        }

        if (find.getOptions().isReturnKey()) {
            document.put("$returnKey", true);
        }

        if (find.getOptions().isShowDiskLoc()) {
            document.put("$showDiskLoc", true);
        }

        if (find.getOptions().isSnapshot()) {
            document.put("$snapshot", true);
        }

        long maxTime = find.getOptions().getMaxTime(MILLISECONDS);
        if (maxTime != 0) {
            document.put("$maxTimeMS", maxTime);
        }

        int maxScan = find.getOptions().getMaxScan();
        if (maxScan > 0) {
            document.put("$maxScan", maxScan);
        }

        // TODO: special
        return document;
    }

    private boolean isExhaustCursor() {
        return find.getFlags().contains(QueryFlag.Exhaust);
    }
}