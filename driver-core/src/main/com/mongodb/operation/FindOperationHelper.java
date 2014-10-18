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
import com.mongodb.Function;
import com.mongodb.MongoCursor;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.protocol.QueryResult;
import org.bson.codecs.Decoder;

import java.util.ArrayList;
import java.util.List;

final class FindOperationHelper {

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

    static <T, V> MongoFuture<List<V>> queryResultToListAsync(final MongoNamespace namespace,
                                                              final MongoFuture<QueryResult<T>> queryResultFuture,
                                                              final Decoder<T> decoder, final AsyncConnectionSource source,
                                                              final Function<T, V> transformer) {
        SingleResultFuture<List<V>> future = new SingleResultFuture<List<V>>();
        queryResultFuture.register(new QueryResultToListCallback<T, V>(future, namespace, decoder, source, transformer));
        return future;
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

    private FindOperationHelper() {
    }
}
