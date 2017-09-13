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
import com.mongodb.operation.BatchCursor;
import com.mongodb.operation.ReadOperation;

import java.util.Collection;

/**
 * MongoIterable implementation that aids iteration over the results of results of an inline map-reduce operation.
 *
 * @param <T> the return type of the map reduce
 * @since 3.0
 */
class OperationIterable<T> implements MongoIterable<T> {
    private final ReadOperation<? extends BatchCursor<T>> operation;
    private final ReadPreference readPreference;
    private final OperationExecutor executor;

    OperationIterable(final ReadOperation<? extends BatchCursor<T>> operation, final ReadPreference readPreference,
                      final OperationExecutor executor) {
        this.operation = operation;
        this.readPreference = readPreference;
        this.executor = executor;
    }

    @Override
    public MongoCursor<T> iterator() {
        return new MongoBatchCursorAdapter<T>(executor.execute(operation, readPreference));
    }

    @Override
    public T first() {
        MongoCursor<T> cursor = iterator();
        try {
            if (!cursor.hasNext())  {
                return null;
            }
            return cursor.next();
        } finally {
            cursor.close();
        }
    }

    @Override
    public <U> MongoIterable<U> map(final Function<T, U> mapper) {
        return new MappingIterable<T, U>(this, mapper);
    }

    @Override
    public void forEach(final Block<? super T> block) {
        MongoCursor<T> cursor = iterator();
        try {
            while (cursor.hasNext()) {
                block.apply(cursor.next());
            }
        } finally {
            cursor.close();
        }
    }

    @Override
    public <A extends Collection<? super T>> A into(final A target) {
        forEach(new Block<T>() {
            @Override
            public void apply(final T t) {
                target.add(t);
            }
        });
        return target;
    }

    @Override
    public MongoIterable<T> batchSize(final int batchSize) {
        throw new UnsupportedOperationException();
    }

}
