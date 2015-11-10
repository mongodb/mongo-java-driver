/*
 * Copyright 2015 MongoDB, Inc.
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
import com.mongodb.ReadPreference;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.ListDatabasesOperation;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class ListDatabasesIterableImpl<TResult> implements ListDatabasesIterable<TResult> {
    private final Class<TResult> resultClass;
    private final ReadPreference readPreference;
    private final CodecRegistry codecRegistry;
    private final AsyncOperationExecutor executor;

    private long maxTimeMS;

    ListDatabasesIterableImpl(final Class<TResult> resultClass, final CodecRegistry codecRegistry,
                              final ReadPreference readPreference, final AsyncOperationExecutor executor) {
        this.resultClass = notNull("resultClass", resultClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.executor = notNull("executor", executor);
    }

    @Override
    public ListDatabasesIterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public void first(final SingleResultCallback<TResult> callback) {
        notNull("callback", callback);
        execute(createListDatabasesOperation()).first(callback);
    }

    @Override
    public void forEach(final Block<? super TResult> block, final SingleResultCallback<Void> callback) {
        notNull("block", block);
        notNull("callback", callback);
        execute().forEach(block, callback);
    }

    @Override
    public <A extends Collection<? super TResult>> void into(final A target, final SingleResultCallback<A> callback) {
        notNull("target", target);
        notNull("callback", callback);
        execute().into(target, callback);
    }

    @Override
    public <U> MongoIterable<U> map(final Function<TResult, U> mapper) {
        return new MappingIterable<TResult, U>(this, mapper);
    }

    @Override
    public ListDatabasesIterableImpl<TResult> batchSize(final int batchSize) {
        // Noop - not supported by listDatabasesIterable
        return this;
    }

    @Override
    public void batchCursor(final SingleResultCallback<AsyncBatchCursor<TResult>> callback) {
        notNull("callback", callback);
        execute().batchCursor(callback);
    }

    private MongoIterable<TResult> execute() {
        return execute(createListDatabasesOperation());
    }

    private MongoIterable<TResult> execute(final ListDatabasesOperation<TResult> operation) {
        return new OperationIterable<TResult>(operation, readPreference, executor);
    }

    private ListDatabasesOperation<TResult> createListDatabasesOperation() {
        return new ListDatabasesOperation<TResult>(codecRegistry.get(resultClass)).maxTime(maxTimeMS, MILLISECONDS);
    }
}
