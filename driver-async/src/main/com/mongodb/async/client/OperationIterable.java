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
import com.mongodb.ReadPreference;
import com.mongodb.async.MongoAsyncCursor;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.AsyncReadOperation;

import java.util.ArrayList;
import java.util.Collection;

class OperationIterable<T> implements MongoIterable<T> {
    private final AsyncReadOperation<? extends MongoAsyncCursor<T>> operation;
    private final ReadPreference readPreference;
    private final AsyncOperationExecutor executor;

    OperationIterable(final AsyncReadOperation<? extends MongoAsyncCursor<T>> operation, final ReadPreference readPreference,
                      final AsyncOperationExecutor executor) {
        this.operation = operation;
        this.readPreference = readPreference;
        this.executor = executor;
    }

    @Override
    public MongoFuture<T> first() {
        final SingleResultFuture<T> future = new SingleResultFuture<T>();
        into(new ArrayList<T>(1)).register(new SingleResultCallback<ArrayList<T>>() {
            @Override
            public void onResult(final ArrayList<T> result, final MongoException e) {
                if (e != null) {
                    future.init(null, e);
                } else if (result.size() == 0) {
                    future.init(null, null);
                } else {
                    future.init(result.get(0), null);
                }
            }
        });
        return future;
    }

    @Override
    public MongoFuture<Void> forEach(final Block<? super T> block) {
        final SingleResultFuture<Void> future = new SingleResultFuture<Void>();
        execute().register(new
                           SingleResultCallback<MongoAsyncCursor<T>>() {
                               @Override
                               public void onResult(final MongoAsyncCursor<T> cursor, final MongoException e) {
                                   if (e != null) {
                                       future.init(null, e);
                                   } else {
                                       cursor.forEach(new Block<T>() {
                                           @Override
                                           public void apply(final T t) {
                                               block.apply(t);
                                           }
                                       }).register(new SingleResultCallback<Void>() {
                                           @Override
                                           public void onResult(final Void result, final MongoException e) {
                                               if (e != null) {
                                                   future.init(null, e);
                                               } else {
                                                   future.init(null, null);
                                               }
                                           }
                                       });
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
    private MongoFuture<MongoAsyncCursor<T>> execute() {
        return (MongoFuture<MongoAsyncCursor<T>>) executor.execute(operation, readPreference);
    }

}
