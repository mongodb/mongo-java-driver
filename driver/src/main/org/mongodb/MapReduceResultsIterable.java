/*
 * Copyright (c) 2008 - 2013 MongoDB, Inc. <http://10gen.com>
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

import org.mongodb.operation.MapReduceWithInlineResultsOperation;

import java.util.Collection;

/**
 * MongoIterable implementation that aids iteration over the results of results of an inline map-reduce operation.
 *
 * @param <T> the return type of the map reduce
 * @since 3.0
 */
class MapReduceResultsIterable<T> implements MongoIterable<T> {
    private final MapReduceWithInlineResultsOperation<T> operation;

    MapReduceResultsIterable(final MapReduceWithInlineResultsOperation<T> operation) {
        this.operation = operation;
    }

    @Override
    public MongoCursor<T> iterator() {
        return operation.execute();

    }

    @Override
    public void forEach(final Block<? super T> block) {
        MongoCursor<T> cursor = iterator();
        while (cursor.hasNext()) {
            if (!block.run(cursor.next())) {
                break;
            }
        }
    }

    @Override
    public <A extends Collection<? super T>> A into(final A target) {
        forEach(new Block<T>() {
            @Override
            public boolean run(final T t) {
                target.add(t);
                return true;
            }
        });
        return target;
    }

    @Override
    public <U> MongoIterable<U> map(final Function<T, U> mapper) {
        return new MappingIterable<T, U>(this, mapper);
    }

    @Override
    public void asyncForEach(final AsyncBlock<? super T> block) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A extends Collection<? super T>> MongoFuture<A> asyncInto(final A target) {
        throw new UnsupportedOperationException();
    }
}
