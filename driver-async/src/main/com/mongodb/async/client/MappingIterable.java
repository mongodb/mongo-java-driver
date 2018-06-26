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
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;

import java.util.Collection;

import static com.mongodb.assertions.Assertions.notNull;

class MappingIterable<U, V> implements MongoIterable<V> {
    private final MongoIterable<U> iterable;
    private final Function<U, V> mapper;

    MappingIterable(final MongoIterable<U> iterable, final Function<U, V> mapper) {
        this.iterable = notNull("iterable", iterable);
        this.mapper = notNull("mapper", mapper);
    }

    @Override
    public void first(final SingleResultCallback<V> callback) {
        notNull("callback", callback);
        iterable.first(new SingleResultCallback<U>() {
            @Override
            public void onResult(final U result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else if (result == null) {
                    callback.onResult(null, null);
                } else {
                    callback.onResult(mapper.apply(result), null);
                }
            }
        });
    }

    @Override
    public void forEach(final Block<? super V> block, final SingleResultCallback<Void> callback) {
        notNull("block", block);
        notNull("callback", callback);
        iterable.forEach(new Block<U>() {
            @Override
            public void apply(final U t) {
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
    public <A extends Collection<? super V>> void into(final A target, final SingleResultCallback<A> callback) {
        notNull("target", target);
        notNull("callback", callback);
        iterable.forEach(new Block<U>() {
            @Override
            public void apply(final U t) {
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
    public <W> MongoIterable<W> map(final Function<V, W> mapper) {
        return new MappingIterable<V, W>(this, mapper);
    }

    @Override
    public MongoIterable<V> batchSize(final int batchSize) {
        iterable.batchSize(batchSize);
        return this;
    }

    @Override
    public Integer getBatchSize() {
        return iterable.getBatchSize();
    }

    @Override
    public void batchCursor(final SingleResultCallback<AsyncBatchCursor<V>> callback) {
        notNull("callback", callback);
        iterable.batchCursor(new SingleResultCallback<AsyncBatchCursor<U>>() {
            @Override
            public void onResult(final AsyncBatchCursor<U> batchCursor, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    callback.onResult(new MappingAsyncBatchCursor<U, V>(batchCursor, mapper), null);
                }
            }
        });
    }

    MongoIterable<U> getMapped() {
        return iterable;
    }
}
