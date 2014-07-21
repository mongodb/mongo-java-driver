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

package com.mongodb.client;

import com.mongodb.Block;
import com.mongodb.operation.MapReduceWithInlineResultsOperation;
import org.mongodb.Function;
import org.mongodb.MongoCursor;

import java.util.Collection;

/**
 * MongoIterable implementation that aids iteration over the results of results of an inline map-reduce operation.
 *
 * @param <V> the return type of the map reduce
 * @since 3.0
 */
class MapReduceResultsIterable<T, V> implements MongoIterable<V> {
    private final MapReduceWithInlineResultsOperation<V> operation;
    private final MongoCollectionImpl<T> collection;

    MapReduceResultsIterable(final MapReduceWithInlineResultsOperation<V> operation, final MongoCollectionImpl<T> collection) {
        this.operation = operation;
        this.collection = collection;
    }

    @Override
    public MongoCursor<V> iterator() {
        return collection.execute(operation, collection.getOptions().getReadPreference());
    }

    @Override
    public void forEach(final Block<? super V> block) {
        MongoCursor<V> cursor = iterator();
        while (cursor.hasNext()) {
            block.apply(cursor.next());
        }
    }

    @Override
    public <A extends Collection<? super V>> A into(final A target) {
        forEach(new Block<V>() {
            @Override
            public void apply(final V t) {
                target.add(t);
            }
        });
        return target;
    }

    @Override
    public <U> MongoIterable<U> map(final Function<V, U> mapper) {
        return new MappingIterable<V, U>(this, mapper);
    }

}
