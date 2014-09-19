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

import com.mongodb.client.MongoIterable;
import com.mongodb.operation.MapReduceWithInlineResultsOperation;
import com.mongodb.operation.OperationExecutor;

import java.util.Collection;

/**
 * MongoIterable implementation that aids iteration over the results of results of an inline map-reduce operation.
 *
 * @param <V> the return type of the map reduce
 * @since 3.0
 */
class MapReduceResultsIterable<V> implements MongoIterable<V> {
    private final MapReduceWithInlineResultsOperation<V> operation;
    private final ReadPreference readPreference;
    private final OperationExecutor operationExecutor;

    MapReduceResultsIterable(final MapReduceWithInlineResultsOperation<V> operation, final ReadPreference readPreference,
                             final OperationExecutor operationExecutor) {
        this.operation = operation;
        this.readPreference = readPreference;
        this.operationExecutor = operationExecutor;
    }

    @Override
    public MongoCursor<V> iterator() {
        return operationExecutor.execute(operation, readPreference);
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
