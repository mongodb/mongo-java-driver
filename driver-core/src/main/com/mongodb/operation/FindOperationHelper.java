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

import com.mongodb.Function;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.connection.QueryResult;
import org.bson.codecs.Decoder;

import java.util.ArrayList;
import java.util.List;

final class FindOperationHelper {

    static <T, V> List<V> queryResultToList(final MongoNamespace namespace, final QueryResult<T> queryResult, final Decoder<T> decoder,
                                            final ConnectionSource source,
                                            final Function<T, V> block) {
        BatchCursor<T> cursor = new QueryBatchCursor<T>(namespace, queryResult, 0, 0, decoder, source);
        try {
            List<V> retVal = new ArrayList<V>();
            while (cursor.hasNext()) {
                for (T cur : cursor.next()) {
                    V value = block.apply(cur);
                    if (value != null) {
                        retVal.add(value);
                    }
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
        private Function<T, V> mapper;

        public QueryResultToListCallback(final SingleResultFuture<List<V>> future,
                                         final MongoNamespace namespace,
                                         final Decoder<T> decoder,
                                         final AsyncConnectionSource connectionSource,
                                         final Function<T, V> mapper) {
            this.future = future;
            this.namespace = namespace;
            this.decoder = decoder;
            this.connectionSource = connectionSource;
            this.mapper = mapper;
        }

        @Override
        public void onResult(final QueryResult<T> results, final MongoException e) {
            if (e != null) {
                future.init(null, e);
            } else {
                loopCursor(new AsyncQueryBatchCursor<T>(namespace, results, 0, 0, decoder, connectionSource), new ArrayList<V>());
            }
        }

        private void loopCursor(final AsyncBatchCursor<T> batchCursor, final List<V> mappedResults) {
            batchCursor.next(new SingleResultCallback<List<T>>() {
                @Override
                public void onResult(final List<T> results, final MongoException e) {
                    if (e != null) {
                        future.init(null, e);
                    } else if (results == null) {
                        future.init(mappedResults, null);
                    } else {
                        for (T result : results) {
                            mappedResults.add(mapper.apply(result));
                        }
                        loopCursor(batchCursor, mappedResults);
                    }
                }
            });
        }
    }

    private FindOperationHelper() {
    }
}
