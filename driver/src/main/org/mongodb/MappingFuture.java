/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb;

import org.mongodb.connection.SingleResultCallback;
import org.mongodb.operation.MongoFuture;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A future that can map from another Future of type U to a Future of type V.
 *
 * @param <U> the type mapped from
 * @param <V> the type mapped to
 */
// TODO: take this private?
public class MappingFuture<U, V> implements MongoFuture<V> {
    private final MongoFuture<U> mappedFuture;
    private Function<U, V> function;

    public MappingFuture(final MongoFuture<U> mappedFuture, final Function<U, V> function) {
        this.mappedFuture = mappedFuture;
        this.function = function;
    }

    @Override
    public V get() {
        return function.apply(mappedFuture.get());
    }

    @Override
    public V get(final long timeout, final TimeUnit unit) throws TimeoutException {
        return function.apply(mappedFuture.get(timeout, unit));
    }

    @Override
    public void register(final SingleResultCallback<V> newCallback) {
        mappedFuture.register(new SingleResultCallback<U>() {
            @Override
            public void onResult(final U result, final MongoException e) {
                if (e != null) {
                    newCallback.onResult(null, e);
                }
                else {
                    newCallback.onResult(function.apply(result), null);
                }
            }
        });
    }

    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
        return mappedFuture.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return mappedFuture.isCancelled();
    }

    @Override
    public boolean isDone() {
        return mappedFuture.isDone();
    }
}
