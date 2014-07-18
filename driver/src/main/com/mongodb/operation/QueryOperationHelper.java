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
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.connection.Connection;
import com.mongodb.connection.SingleResultCallback;
import com.mongodb.protocol.QueryProtocol;
import com.mongodb.protocol.QueryResult;
import org.bson.BsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;
import org.mongodb.Block;
import org.mongodb.Function;
import org.mongodb.MongoCursor;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.operation.OperationHelper.AsyncCallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.IdentityTransformer;
import static com.mongodb.operation.OperationHelper.executeProtocol;
import static com.mongodb.operation.OperationHelper.withConnection;

final class QueryOperationHelper {
    static <T> List<T> queryResultToList(final MongoNamespace namespace, final QueryProtocol<T> queryProtocol, final Decoder<T> decoder,
                                         final ReadBinding binding) {
        return queryResultToList(namespace, queryProtocol, decoder, binding, new Function<T, T>() {
            @Override
            public T apply(final T t) {
                return t;
            }
        });
    }

    static <V> List<V> queryResultToList(final MongoNamespace namespace, final QueryProtocol<BsonDocument> queryProtocol,
                                         final ReadBinding binding, final Function<BsonDocument, V> block) {
        return queryResultToList(namespace, queryProtocol, new BsonDocumentCodec(), binding, block);
    }

    static <T, V> List<V> queryResultToList(final MongoNamespace namespace, final QueryProtocol<T> queryProtocol, final Decoder<T> decoder,
                                            final ReadBinding binding, final Function<T, V> block) {
        ConnectionSource source = binding.getReadConnectionSource();
        try {
            return queryResultToList(namespace, executeProtocol(queryProtocol, source), decoder, source, block);
        } finally {
            source.release();
        }
    }

    static <T> List<T> queryResultToList(final MongoNamespace namespace, final QueryResult<T> queryResult, final Decoder<T> decoder,
                                         final ConnectionSource source) {
        return queryResultToList(namespace, queryResult, decoder, source, new IdentityTransformer<T>());
    }

    static <T, V> List<V> queryResultToList(final MongoNamespace namespace, final QueryResult<T> queryResult, final Decoder<T> decoder,
                                            final ConnectionSource source,
                                            final Function<T, V> block) {
        MongoCursor<T> cursor = new MongoQueryCursor<T>(namespace, queryResult, 0, 0, decoder, source);
        try {
            List<V> retVal = new ArrayList<V>();
            while (cursor.hasNext()) {
                V value = block.apply(cursor.next());
                if (value != null) {
                    retVal.add(value);
                }
            }
            return retVal;
        } finally {
            cursor.close();
        }
    }

    static MongoFuture<List<BsonDocument>> queryResultToListAsync(final MongoNamespace namespace,
                                                                  final QueryProtocol<BsonDocument> queryProtocol,
                                                                  final AsyncReadBinding binding) {
        return queryResultToListAsync(namespace, queryProtocol, binding, new IdentityTransformer<BsonDocument>());
    }

    static <T> MongoFuture<List<T>> queryResultToListAsync(final MongoNamespace namespace, final QueryProtocol<BsonDocument> queryProtocol,
                                                           final AsyncReadBinding binding, final Function<BsonDocument, T> transformer) {
        return queryResultToListAsync(namespace, queryProtocol, new BsonDocumentCodec(), binding, transformer);
    }

    static <T> MongoFuture<List<T>> queryResultToListAsync(final MongoNamespace namespace, final QueryProtocol<T> queryProtocol,
                                                           final Decoder<T> decoder, final AsyncReadBinding binding) {
        return queryResultToListAsync(namespace, queryProtocol, decoder, binding, new IdentityTransformer<T>());
    }

    static <T, V> MongoFuture<List<V>> queryResultToListAsync(final MongoNamespace namespace, final QueryProtocol<T> queryProtocol,
                                                              final Decoder<T> decoder, final AsyncReadBinding binding,
                                                              final Function<T, V> transformer) {
        return withConnection(binding, new AsyncCallableWithConnectionAndSource<List<V>>() {
            @Override
            public MongoFuture<List<V>> call(final AsyncConnectionSource source, final Connection connection) {
                final SingleResultFuture<List<V>> future = new SingleResultFuture<List<V>>();
                queryProtocol.executeAsync(connection)
                             .register(new QueryResultToListCallback<T, V>(future, namespace, decoder, source, transformer));
                return future;
            }
        });
    }

    private static class QueryResultToListCallback<T, V> implements SingleResultCallback<QueryResult<T>> {

        private SingleResultFuture<List<V>> future;
        private MongoNamespace namespace;
        private Decoder<T> decoder;
        private AsyncConnectionSource connectionSource;
        private Function<T, V> block;

        public QueryResultToListCallback(final SingleResultFuture<List<V>> future,
                                         final MongoNamespace namespace,
                                         final Decoder<T> decoder,
                                         final AsyncConnectionSource connectionSource,
                                         final Function<T, V> block) {
            this.future = future;
            this.namespace = namespace;
            this.decoder = decoder;
            this.connectionSource = connectionSource;
            this.block = block;
        }

        @Override
        public void onResult(final QueryResult<T> result, final MongoException e) {
            if (e != null) {
                future.init(null, e);
            } else {
                final List<V> results = new ArrayList<V>();
                new MongoAsyncQueryCursor<T>(namespace, result, 0, 0, decoder, connectionSource).forEach(new Block<T>() {
                    public void apply(final T v) {
                        V value = block.apply(v);
                        if (value != null) {
                            results.add(value);
                        }
                    }
                }).register(new SingleResultCallback<Void>() {
                    @Override
                    public void onResult(final Void result, final MongoException e) {
                        if (e != null) {
                            future.init(null, e);
                        } else {
                            future.init(results, null);
                        }
                    }
                });
            }
        }
    }

    private QueryOperationHelper() {
    }
}
