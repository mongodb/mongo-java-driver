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

package com.mongodb.client.internal;

import com.mongodb.Function;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.lang.Nullable;

import java.util.Collection;
import java.util.function.Consumer;

class MappingIterable<U, V> implements MongoIterable<V> {

    private final MongoIterable<U> iterable;
    private final Function<U, V> mapper;

    MappingIterable(final MongoIterable<U> iterable, final Function<U, V> mapper) {
        this.iterable = iterable;
        this.mapper = mapper;
    }

    @Override
    public MongoCursor<V> iterator() {
        return new MongoMappingCursor<>(iterable.iterator(), mapper);
    }

    @Override
    public MongoCursor<V> cursor() {
        return iterator();
    }

    @Nullable
    @Override
    public V first() {
        U first = iterable.first();
        if (first == null) {
            return null;
        }
        return mapper.apply(first);
    }

    @Override
    public void forEach(final Consumer<? super V> block) {
        iterable.forEach(new Consumer<U>() {
            @Override
            public void accept(final U document) {
                block.accept(mapper.apply(document));
            }
        });
    }

    @Override
    public <A extends Collection<? super V>> A into(final A target) {
        forEach(new Consumer<V>() {
            @Override
            public void accept(final V v) {
                target.add(v);
            }
        });
        return target;
    }

    @Override
    public MappingIterable<U, V> batchSize(final int batchSize) {
        iterable.batchSize(batchSize);
        return this;
    }

    @Override
    public <W> MongoIterable<W> map(final Function<V, W> newMap) {
        return new MappingIterable<>(this, newMap);
    }

    MongoIterable<U> getMapped() {
        return iterable;
    }
}
