/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client.syncadapter;

import com.mongodb.Function;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import org.reactivestreams.Publisher;

import java.util.Collection;
import java.util.function.Consumer;

public abstract class SyncMongoIterable<T> implements MongoIterable<T> {
    private final Publisher<T> wrapped;

    SyncMongoIterable(final Publisher<T> wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public MongoCursor<T> iterator() {
        return cursor();
    }

    @Override
    public MongoCursor<T> cursor() {
        return new SyncMongoCursor<>(wrapped);
    }

    @Override
    public T first() {
        SyncSubscriber<T> syncSubscriber = new SyncSubscriber<>();
        wrapped.subscribe(syncSubscriber);
        return syncSubscriber.first();
    }

    @Override
    public <U> MongoIterable<U> map(final Function<T, U> mapper) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forEach(final Consumer<? super T> action) {
        SyncSubscriber<T> syncSubscriber = new SyncSubscriber<>();
        wrapped.subscribe(syncSubscriber);
        for (T cur : syncSubscriber.all()) {
            action.accept(cur);
        }
    }

    @Override
    public <A extends Collection<? super T>> A into(final A target) {
        SyncSubscriber<T> syncSubscriber = new SyncSubscriber<>();
        wrapped.subscribe(syncSubscriber);
        target.addAll(syncSubscriber.all());
        return target;
    }
}
