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
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.AsyncBatchCursor;

import java.util.Collection;

import static org.bson.assertions.Assertions.notNull;

class MappingIterable<T, U> implements MongoIterable<U> {
    private final MongoIterable<T> iterable;
    private final Function<T, U> mapper;

    MappingIterable(final MongoIterable<T> iterable, final Function<T, U> mapper) {
        this.iterable = notNull("iterable", iterable);
        this.mapper = notNull("mapper", mapper);
    }

    @Override
    public void first(final SingleResultCallback<U> callback) {
        notNull("callback", callback);
        iterable.first(new SingleResultCallback<T>() {
            @Override
            public void onResult(final T result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    callback.onResult(mapper.apply(result), null);
                }
            }
        });
    }

    @Override
    public void forEach(final Block<? super U> block, final SingleResultCallback<Void> callback) {
        notNull("block", block);
        notNull("callback", callback);
        iterable.forEach(new Block<T>() {
            @Override
            public void apply(final T t) {
                block.apply(mapper.apply(t));
            }
        }, new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    callback.onResult(null, null);
                }
            }
        });
    }

    @Override
    public <A extends Collection<? super U>> void into(final A target, final SingleResultCallback<A> callback) {
        notNull("target", target);
        notNull("callback", callback);
        iterable.forEach(new Block<T>() {
            @Override
            public void apply(final T t) {
                target.add(mapper.apply(t));
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

    @Override
    public <V> MongoIterable<V> map(final Function<U, V> mapper) {
        return new MappingIterable<U, V>(this, mapper);
    }

    @Override
    public MongoIterable<U> batchSize(final int batchSize) {
        iterable.batchSize(batchSize);
        return this;
    }

    @Override
    public void batchCursor(final SingleResultCallback<AsyncBatchCursor<U>> callback) {
        notNull("callback", callback);
        iterable.batchCursor(new SingleResultCallback<AsyncBatchCursor<T>>() {
            @Override
            public void onResult(final AsyncBatchCursor<T> batchCursor, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    callback.onResult(new MappingAsyncBatchCursor<T, U>(batchCursor, mapper), null);
                }
            }
        });
    }
}
