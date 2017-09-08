/*
 * Copyright 2017 MongoDB, Inc.
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

package com.mongodb;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.operation.BatchCursor;
import com.mongodb.operation.ReadOperation;

import java.util.Collection;

import static com.mongodb.assertions.Assertions.notNull;

abstract class MongoIterableImpl<TResult> implements MongoIterable<TResult> {
    private final ClientSession clientSession;
    private final ReadConcern readConcern;
    private OperationExecutor executor;
    private ReadPreference readPreference;
    private Integer batchSize;

    MongoIterableImpl(final OperationExecutor executor, final ReadConcern readConcern,
                      final ReadPreference readPreference) {
        this(null, executor, readConcern, readPreference);
    }

    MongoIterableImpl(final ClientSession clientSession, final OperationExecutor executor, final ReadConcern readConcern,
                      final ReadPreference readPreference) {
        this.clientSession = clientSession;
        this.executor = notNull("executor", executor);
        this.readConcern = notNull("readConcern", readConcern);
        this.readPreference = notNull("readPreference", readPreference);
    }

    abstract ReadOperation<BatchCursor<TResult>> asReadOperation();

    ClientSession getClientSession() {
        return clientSession;
    }

    OperationExecutor getExecutor() {
        return executor;
    }

    ReadPreference getReadPreference() {
        return readPreference;
    }

    ReadConcern getReadConcern() {
        return readConcern;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    @Override
    public MongoIterable<TResult> batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    @Override
    public MongoCursor<TResult> iterator() {
        return new MongoBatchCursorAdapter<TResult>(execute());
    }

    @Override
    public TResult first() {
        MongoCursor<TResult> cursor = iterator();
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
    public <U> MongoIterable<U> map(final Function<TResult, U> mapper) {
        return new MappingIterable<TResult, U>(this, mapper);
    }

    @Override
    public void forEach(final Block<? super TResult> block) {
        MongoCursor<TResult> cursor = iterator();
        try {
            while (cursor.hasNext()) {
                block.apply(cursor.next());
            }
        } finally {
            cursor.close();
        }
    }

    @Override
    public <A extends Collection<? super TResult>> A into(final A target) {
        forEach(new Block<TResult>() {
            @Override
            public void apply(final TResult t) {
                target.add(t);
            }
        });
        return target;
    }

    private BatchCursor<TResult> execute() {
        return executor.execute(asReadOperation(), readPreference, clientSession);
    }
}
