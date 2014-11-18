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
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.ReadPreference;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.operation.AsyncBatchCursor;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.AsyncReadOperation;

import java.util.Collection;
import java.util.List;

class OperationIterable<T> implements MongoIterable<T> {
    private final AsyncReadOperation<? extends AsyncBatchCursor<T>> operation;
    private final ReadPreference readPreference;
    private final AsyncOperationExecutor executor;

    OperationIterable(final AsyncReadOperation<? extends AsyncBatchCursor<T>> operation, final ReadPreference readPreference,
                      final AsyncOperationExecutor executor) {
        this.operation = operation;
        this.readPreference = readPreference;
        this.executor = executor;
    }

    @Override
    public MongoFuture<T> first() {
        final SingleResultFuture<T> future = new SingleResultFuture<T>();
        execute().register(new SingleResultCallback<AsyncBatchCursor<T>>() {
            @Override
            public void onResult(final AsyncBatchCursor<T> batchCursor, final MongoException e) {
                if (e != null) {
                    future.init(null, e);
                } else {
                    batchCursor.setBatchSize(1);
                    batchCursor.next(new SingleResultCallback<List<T>>() {
                        @Override
                        public void onResult(final List<T> results, final MongoException e) {
                            if (e != null) {
                                future.init(null, e);
                            } else if (results == null) {
                                future.init(null, null);
                            } else {
                                future.init(results.get(0), null);
                            }
                            batchCursor.close();
                        }
                    });
                }
            }
        });
        return future;
    }

    @Override
    public MongoFuture<Void> forEach(final Block<? super T> block) {
        final SingleResultFuture<Void> future = new SingleResultFuture<Void>();
        execute().register(new SingleResultCallback<AsyncBatchCursor<T>>() {
            @Override
            public void onResult(final AsyncBatchCursor<T> batchCursor, final MongoException e) {
                if (e != null) {
                    future.init(null, e);
                } else {
                    loopCursor(future, batchCursor, block);
                }
            }
        });
        return future;
    }

    @Override
    public <A extends Collection<? super T>> MongoFuture<A> into(final A target) {
        final SingleResultFuture<A> future = new SingleResultFuture<A>();
        forEach(new Block<T>() {
            @Override
            public void apply(final T t) {
                target.add(t);
            }
        }).register(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final MongoException e) {
                if (e != null) {
                    future.init(null, e);
                } else {
                    future.init(target, null);
                }
            }
        });
        return future;
    }

    @Override
    public <U> MongoIterable<U> map(final Function<T, U> mapper) {
        return new MappingIterable<T, U>(this, mapper);
    }

    @SuppressWarnings("unchecked")
    private MongoFuture<AsyncBatchCursor<T>> execute() {
        return (MongoFuture<AsyncBatchCursor<T>>) executor.execute(operation, readPreference);
    }

    private void loopCursor(final SingleResultFuture<Void> future, final AsyncBatchCursor<T> batchCursor, final Block<? super T> block) {
        batchCursor.next(new SingleResultCallback<List<T>>() {
            @Override
            public void onResult(final List<T> results, final MongoException e) {
                if (e != null) {
                    future.init(null, e);
                } else if (results == null) {
                    future.init(null, null);
                } else {
                    for (T result: results) {
                        try {
                            block.apply(result);
                        } catch (MongoException err) {
                            future.init(null, err);
                            break;
                        } catch (Throwable t) {
                            future.init(null, new MongoInternalException(t.getMessage(), t));
                            break;
                        }
                    }
                    if (!future.isDone()) {
                        loopCursor(future, batchCursor, block);
                    }
                }
            }
        });
    }
}
