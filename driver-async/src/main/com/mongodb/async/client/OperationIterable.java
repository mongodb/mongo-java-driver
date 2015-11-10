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

package com.mongodb.async.client;

import com.mongodb.Block;
import com.mongodb.Function;
import com.mongodb.ReadPreference;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.AsyncReadOperation;

import java.util.Collection;
import java.util.List;

import static org.bson.assertions.Assertions.notNull;

class OperationIterable<T> implements MongoIterable<T> {
    private final AsyncReadOperation<? extends AsyncBatchCursor<T>> operation;
    private final ReadPreference readPreference;
    private final AsyncOperationExecutor executor;

    OperationIterable(final AsyncReadOperation<? extends AsyncBatchCursor<T>> operation, final ReadPreference readPreference,
                      final AsyncOperationExecutor executor) {
        this.operation = notNull("operation", operation);
        this.readPreference = notNull("readPreference", readPreference);
        this.executor = notNull("executor", executor);
    }

    @Override
    public void forEach(final Block<? super T> block, final SingleResultCallback<Void> callback) {
        notNull("block", block);
        notNull("callback", callback);
        batchCursor(new SingleResultCallback<AsyncBatchCursor<T>>() {
            @Override
            public void onResult(final AsyncBatchCursor<T> batchCursor, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    loopCursor(batchCursor, block, callback);
                }
            }
        });
    }

    @Override
    public <A extends Collection<? super T>> void into(final A target, final SingleResultCallback<A> callback) {
        notNull("target", target);
        notNull("callback", callback);
        batchCursor(new SingleResultCallback<AsyncBatchCursor<T>>() {
            @Override
            public void onResult(final AsyncBatchCursor<T> batchCursor, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    loopCursor(batchCursor, new Block<T>() {
                        @Override
                        public void apply(final T t) {
                            target.add(t);
                        }
                    }, new SingleResultCallback<Void>() {
                        @Override
                        public void onResult(final Void result, final Throwable t) {
                            if (t != null) {
                                callback.onResult(null, t);
                            } else {
                                callback.onResult(target, null);
                            }
                        }
                    });
                }
            }
        });
    }

    @Override
    public void first(final SingleResultCallback<T> callback) {
        notNull("callback", callback);
        batchCursor(new SingleResultCallback<AsyncBatchCursor<T>>() {
            @Override
            public void onResult(final AsyncBatchCursor<T> batchCursor, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    batchCursor.setBatchSize(1);
                    batchCursor.next(new SingleResultCallback<List<T>>() {
                        @Override
                        public void onResult(final List<T> results, final Throwable t) {
                            batchCursor.close();
                            if (t != null) {
                                callback.onResult(null, t);
                            } else if (results == null) {
                                callback.onResult(null, null);
                            } else {
                                callback.onResult(results.get(0), null);
                            }
                        }
                    });
                }
            }
        });
    }

    @Override
    public <U> MongoIterable<U> map(final Function<T, U> mapper) {
        return new MappingIterable<T, U>(this, mapper);
    }

    @Override
    public OperationIterable<T> batchSize(final int batchSize) {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void batchCursor(final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        notNull("callback", callback);
        executor.execute((AsyncReadOperation<AsyncBatchCursor<T>>) operation, readPreference, callback);
    }

    private void loopCursor(final AsyncBatchCursor<T> batchCursor, final Block<? super T> block,
                            final SingleResultCallback<Void> callback) {
        batchCursor.next(new SingleResultCallback<List<T>>() {
            @Override
            public void onResult(final List<T> results, final Throwable t) {
                if (t != null || results == null) {
                    batchCursor.close();
                    callback.onResult(null, t);
                } else {
                    try {
                        for (T result : results) {
                            block.apply(result);
                        }
                        loopCursor(batchCursor, block, callback);
                    } catch (Throwable tr) {
                        batchCursor.close();
                        callback.onResult(null, tr);
                    }
                }
            }
        });
    }

}
