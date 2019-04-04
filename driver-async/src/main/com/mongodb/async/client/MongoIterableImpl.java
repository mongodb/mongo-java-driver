/*
 * Copyright 2008-present MongoDB, Inc.
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
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.lang.Nullable;
import com.mongodb.operation.AsyncReadOperation;

import java.util.Collection;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;


abstract class MongoIterableImpl<TResult> implements MongoIterable<TResult> {
    private final ClientSession clientSession;
    private final ReadConcern readConcern;
    private final OperationExecutor executor;
    private final ReadPreference readPreference;
    private final boolean retryReads;
    private Integer batchSize;

    MongoIterableImpl(@Nullable final ClientSession clientSession, final OperationExecutor executor, final ReadConcern readConcern,
                      final ReadPreference readPreference, final boolean retryReads) {
        this.clientSession = clientSession;
        this.executor = notNull("executor", executor);
        this.readConcern = notNull("readConcern", readConcern);
        this.readPreference = notNull("readPreference", readPreference);
        this.retryReads = notNull("retryReads", retryReads);
    }

    abstract AsyncReadOperation<AsyncBatchCursor<TResult>> asAsyncReadOperation();

    public ClientSession getClientSession() {
        return clientSession;
    }

    OperationExecutor getExecutor() {
        return executor;
    }

    ReadPreference getReadPreference() {
        return readPreference;
    }

    ReadConcern getReadConcern() {
        return readConcern;
    }

    boolean getRetryReads() {
        return retryReads;
    }

    @Override
    @Nullable
    public Integer getBatchSize() {
        return batchSize;
    }

    @Override
    public void forEach(final Block<? super TResult> block, final SingleResultCallback<Void> callback) {
        notNull("block", block);
        notNull("callback", callback);
        batchCursor(new SingleResultCallback<AsyncBatchCursor<TResult>>() {
            @Override
            public void onResult(final AsyncBatchCursor<TResult> batchCursor, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    loopCursor(batchCursor, block, callback);
                }
            }
        });
    }

    @Override
    public <A extends Collection<? super TResult>> void into(final A target, final SingleResultCallback<A> callback) {
        notNull("target", target);
        notNull("callback", callback);
        batchCursor(new SingleResultCallback<AsyncBatchCursor<TResult>>() {
            @Override
            public void onResult(final AsyncBatchCursor<TResult> batchCursor, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    loopCursor(batchCursor, new Block<TResult>() {
                        @Override
                        public void apply(final TResult t) {
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
    public void first(final SingleResultCallback<TResult> callback) {
        notNull("callback", callback);
        batchCursor(new SingleResultCallback<AsyncBatchCursor<TResult>>() {
            @Override
            public void onResult(final AsyncBatchCursor<TResult> batchCursor, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    batchCursor.setBatchSize(1);
                    batchCursor.next(new SingleResultCallback<List<TResult>>() {
                        @Override
                        public void onResult(final List<TResult> results, final Throwable t) {
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
    public <U> MongoIterable<U> map(final Function<TResult, U> mapper) {
        return new MappingIterable<TResult, U>(this, mapper);
    }

    @Override
    public MongoIterable<TResult> batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    @Override
    public void batchCursor(final SingleResultCallback<AsyncBatchCursor<TResult>> callback) {
        notNull("callback", callback);
        executor.execute(asAsyncReadOperation(), readPreference, readConcern, clientSession, callback);
    }

    private void loopCursor(final AsyncBatchCursor<TResult> batchCursor, final Block<? super TResult> block,
                            final SingleResultCallback<Void> callback) {
        batchCursor.next(new SingleResultCallback<List<TResult>>() {
            @Override
            public void onResult(final List<TResult> results, final Throwable t) {
                if (t != null || results == null) {
                    batchCursor.close();
                    callback.onResult(null, t);
                } else {
                    try {
                        for (TResult result : results) {
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
