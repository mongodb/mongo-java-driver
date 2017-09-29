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

package com.mongodb;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;

import java.util.Collection;

class MappingIterable<U, V> implements MongoIterable<V> {

    private final MongoIterable<U> iterable;
    private final Function<U, V> mapper;

    MappingIterable(final MongoIterable<U> iterable, final Function<U, V> mapper) {
        this.iterable = iterable;
        this.mapper = mapper;
    }

    MongoIterable<U> getMapped() {
        return iterable;
    }

    @Override
    public MongoCursor<V> iterator() {
        return new MongoMappingCursor<U, V>(iterable.iterator(), mapper);
    }

    @Override
    public V first() {
        MongoCursor<V> iterator = iterator();
        if (!iterator.hasNext()) {
            return null;
        }
        return iterator.next();
    }

    @Override
    public void forEach(final Block<? super V> block) {
        iterable.forEach(new Block<U>() {
            @Override
            public void apply(final U document) {
                block.apply(mapper.apply(document));
            }
        });
    }

    @Override
    public <A extends Collection<? super V>> A into(final A target) {
        forEach(new Block<V>() {
            @Override
            public void apply(final V v) {
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
        return new MappingIterable<V, W>(this, newMap);
    }
}
