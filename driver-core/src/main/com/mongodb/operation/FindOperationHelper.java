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
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.connection.QueryResult;
import org.bson.codecs.Decoder;

import java.util.ArrayList;
import java.util.List;

final class FindOperationHelper {

    static <T, V> List<V> queryResultToList(final QueryResult<T> queryResult, final Decoder<T> decoder,
                                            final ConnectionSource source,
                                            final Function<T, V> block) {
        BatchCursor<T> cursor = new QueryBatchCursor<T>(queryResult, 0, 0, decoder, source);
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

    static <T, V> void queryResultToListAsync(final QueryResult<T> queryResult,
                                              final Decoder<T> decoder, final AsyncConnectionSource source,
                                              final Function<T, V> transformer,
                                              final SingleResultCallback<List<V>> callback) {
        new QueryResultToListCallback<T, V>(decoder, source, transformer, callback).onResult(queryResult, null);
    }

    private static class QueryResultToListCallback<T, V> implements SingleResultCallback<QueryResult<T>> {
        private final Decoder<T> decoder;
        private final AsyncConnectionSource connectionSource;
        private final Function<T, V> mapper;
        private final SingleResultCallback<List<V>> callback;

        public QueryResultToListCallback(final Decoder<T> decoder,
                                         final AsyncConnectionSource connectionSource,
                                         final Function<T, V> mapper,
                                         final SingleResultCallback<List<V>> callback) {
            this.decoder = decoder;
            this.connectionSource = connectionSource;
            this.mapper = mapper;
            this.callback = callback;
        }

        @Override
        public void onResult(final QueryResult<T> results, final Throwable t) {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                loopCursor(new AsyncQueryBatchCursor<T>(results, 0, 0, decoder, connectionSource), new ArrayList<V>());
            }
        }

        private void loopCursor(final AsyncBatchCursor<T> batchCursor, final List<V> mappedResults) {
            batchCursor.next(new SingleResultCallback<List<T>>() {
                @Override
                public void onResult(final List<T> results, final Throwable t) {
                    if (t != null || results == null) {
                        batchCursor.close();
                        callback.onResult(mappedResults, t);
                    } else {
                        try {
                            for (T result : results) {
                                V mappedResult = mapper.apply(result);
                                if (mappedResult != null) {
                                    mappedResults.add(mappedResult);
                                }
                            }
                            loopCursor(batchCursor, mappedResults);
                        } catch (Throwable tr) {
                            batchCursor.close();
                            callback.onResult(null, tr);
                        }
                    }
                }
            });
        }
    }

    private FindOperationHelper() {
    }
}
