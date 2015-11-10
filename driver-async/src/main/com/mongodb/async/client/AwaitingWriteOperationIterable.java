/*
 * Copyright 2014-2015 MongoDB, Inc.
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
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.AsyncWriteOperation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

class AwaitingWriteOperationIterable<T, W> implements MongoIterable<T> {
    private final MongoIterable<T> delegated;
    private final List<SingleResultCallback<Void>> callbacks = new ArrayList<SingleResultCallback<Void>>();
    private boolean writeCompleted;
    private Throwable thrown;

    AwaitingWriteOperationIterable(final AsyncWriteOperation<W> writeOperation, final AsyncOperationExecutor executor,
                                   final MongoIterable<T> delegated) {
        this.delegated = delegated;
        executor.execute(writeOperation, new SingleResultCallback<W>() {
            @Override
            public void onResult(final W result, final Throwable t) {
                synchronized (AwaitingWriteOperationIterable.this) {
                    writeCompleted = true;
                    thrown = t;
                    for (SingleResultCallback<Void> cur : callbacks) {
                        cur.onResult(null, t);
                    }
                }
            }
        });
    }

    @Override
    public void first(final SingleResultCallback<T> callback) {
        notNull("callback", callback);
        boolean localWriteCompleted;

        synchronized (this) {
            localWriteCompleted = writeCompleted;
            if (!localWriteCompleted) {
                callbacks.add(new SingleResultCallback<Void>() {
                    @Override
                    public void onResult(final Void result, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else {
                            delegated.first(callback);
                        }
                    }
                });
            }
        }
        if (localWriteCompleted) {
            if (thrown != null) {
                callback.onResult(null, thrown);
            } else {
                delegated.first(callback);
            }
        }
    }

    @Override
    public void forEach(final Block<? super T> block, final SingleResultCallback<Void> callback) {
        notNull("block", block);
        notNull("callback", callback);
        boolean localWriteCompleted;

        synchronized (this) {
            localWriteCompleted = writeCompleted;
            if (!localWriteCompleted) {
                callbacks.add(new SingleResultCallback<Void>() {
                    @Override
                    public void onResult(final Void result, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else {
                            delegated.forEach(block, callback);
                        }
                    }
                });
            }
        }
        if (localWriteCompleted) {
            if (thrown != null) {
                callback.onResult(null, thrown);
            } else {
                delegated.forEach(block, callback);
            }
        }

    }

    @Override
    public <A extends Collection<? super T>> void into(final A target, final SingleResultCallback<A> callback) {
        notNull("target", target);
        notNull("callback", callback);
        boolean localWriteCompleted;

        synchronized (this) {
            localWriteCompleted = writeCompleted;
            if (!localWriteCompleted) {
                callbacks.add(new SingleResultCallback<Void>() {
                    @Override
                    public void onResult(final Void result, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else {
                            delegated.into(target, callback);
                        }
                    }
                });
            }
        }
        if (localWriteCompleted) {
            if (thrown != null) {
                callback.onResult(null, thrown);
            } else {
                delegated.into(target, callback);
            }
        }
    }

    @Override
    public <U> MongoIterable<U> map(final Function<T, U> mapper) {
        return new MappingIterable<T, U>(this, mapper);
    }

    @Override
    public AwaitingWriteOperationIterable<T, W> batchSize(final int batchSize) {
        delegated.batchSize(batchSize);
        return this;
    }

    @Override
    public void batchCursor(final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        notNull("callback", callback);
        boolean localWriteCompleted;

        synchronized (this) {
            localWriteCompleted = writeCompleted;
            if (!localWriteCompleted) {
                callbacks.add(new SingleResultCallback<Void>() {
                    @Override
                    public void onResult(final Void result, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else {
                            delegated.batchCursor(callback);
                        }
                    }
                });
            }
        }
        if (localWriteCompleted) {
            if (thrown != null) {
                callback.onResult(null, thrown);
            } else {
                delegated.batchCursor(callback);
            }
        }
    }
}
