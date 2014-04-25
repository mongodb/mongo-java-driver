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
import org.mongodb.binding.AsyncConnectionSource;
import org.mongodb.binding.AsyncReadBinding;
import org.mongodb.binding.ConnectionSource;
import org.mongodb.binding.ReadBinding;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.protocol.QueryProtocol;
import org.mongodb.protocol.QueryResult;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mongodb.ReadPreference.primary;
import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.connection.ServerType.SHARD_ROUTER;

/**
 * An operation that queries a collection using the provided criteria.
 *
 * @param <T> the document type
 * @since 3.0
 */
public class QueryOperation<T> implements AsyncReadOperation<MongoAsyncCursor<T>>, ReadOperation<MongoCursor<T>> {
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
    public MongoCursor<T> execute(final ReadBinding binding) {
        ConnectionSource source = binding.getReadConnectionSource();
        try {
            Connection connection = source.getConnection();
            try {
                QueryResult<T> queryResult = asQueryProtocol(connection.getServerDescription(), binding.getReadPreference())
                                             .execute(connection);
                if (isExhaustCursor()) {
                    return new MongoQueryCursor<T>(namespace, queryResult, find.getLimit(), find.getBatchSize(),
                                                   resultDecoder, connection);
                } else {
                    return new MongoQueryCursor<T>(namespace, queryResult, find.getLimit(), find.getBatchSize(),
                                                   resultDecoder, source);
                }
            } finally {
                if (!isExhaustCursor()) {
                    connection.close();
                }
            }
        } finally {
            source.release();
        }
    }

    public MongoFuture<MongoAsyncCursor<T>> executeAsync(final AsyncReadBinding binding) {
        final SingleResultFuture<MongoAsyncCursor<T>> future = new SingleResultFuture<MongoAsyncCursor<T>>();
        binding.getReadConnectionSource()
               .register(new SingleResultCallback<AsyncConnectionSource>() {
                   @Override
                   public void onResult(final AsyncConnectionSource source, final MongoException e) {
                       if (e != null) {
                           future.init(null, e);
                       } else {
                           source.getConnection().register(new SingleResultCallback<Connection>() {
                               @Override
                               public void onResult(final Connection connection, final MongoException e) {
                                   if (e != null) {
                                       future.init(null, e);
                                   } else {
                                       asQueryProtocol(connection.getServerDescription(), binding.getReadPreference())
                                       .executeAsync(connection)
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
                                                                                                                      source);
                                                       future.init(cursor, null);
                                                   }
                                               } finally {
                                                   connection.close();
                                               }
                                           }
                                       });
                                   }
                               }
                           });
                       }
                   }
               });
        return future;
    }

    private QueryProtocol<T> asQueryProtocol(final ServerDescription serverDescription, final ReadPreference readPreference) {
        return new QueryProtocol<T>(namespace, find.getFlags(readPreference), find.getSkip(),
                                    find.getNumberToReturn(), asDocument(serverDescription, readPreference),
                                    find.getFields(), queryEncoder, resultDecoder);
    }

    private Document asDocument(final ServerDescription serverDescription, final ReadPreference readPreference) {
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
        if (serverDescription.getType() == SHARD_ROUTER && !readPreference.equals(primary())) {
            document.put("$readPreference", readPreference.toDocument());
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
        return find.getFlags(primary()).contains(QueryFlag.Exhaust);
    }
}