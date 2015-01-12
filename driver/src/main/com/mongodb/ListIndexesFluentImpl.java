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

import com.mongodb.client.ListIndexesFluent;
import com.mongodb.client.MongoCollectionOptions;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.ListIndexesOptions;
import com.mongodb.operation.ListIndexesOperation;
import com.mongodb.operation.OperationExecutor;
import org.bson.codecs.Codec;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class ListIndexesFluentImpl<T> implements ListIndexesFluent<T> {
    private final MongoNamespace namespace;
    private final MongoCollectionOptions options;
    private final OperationExecutor executor;
    private final ListIndexesOptions listIndexesOptions;
    private final Class<T> clazz;

    ListIndexesFluentImpl(final MongoNamespace namespace, final MongoCollectionOptions options, final OperationExecutor executor,
                          final ListIndexesOptions listIndexesOptions, final Class<T> clazz) {
        this.namespace = notNull("namespace", namespace);
        this.options = notNull("options", options);
        this.executor = notNull("executor", executor);
        this.listIndexesOptions = notNull("listIndexesOptions", listIndexesOptions);
        this.clazz = notNull("clazz", clazz);
    }

    @Override
    public ListIndexesFluent<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        listIndexesOptions.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public ListIndexesFluent<T> batchSize(final int batchSize) {
        listIndexesOptions.batchSize(batchSize);
        return this;
    }

    @Override
    public MongoCursor<T> iterator() {
        return execute().iterator();
    }

    @Override
    public T first() {
        return execute().first();
    }

    @Override
    public <U> MongoIterable<U> map(final Function<T, U> mapper) {
        return execute().map(mapper);
    }

    @Override
    public void forEach(final Block<? super T> block) {
        execute().forEach(block);
    }

    @Override
    public <A extends Collection<? super T>> A into(final A target) {
        return execute().into(target);
    }

    private MongoIterable<T> execute() {
        return new OperationIterable<T>(createListIndexesOperation(), this.options.getReadPreference(), executor);
    }

    private <C> Codec<C> getCodec(final Class<C> clazz) {
        return options.getCodecRegistry().get(clazz);
    }

    private ListIndexesOperation<T> createListIndexesOperation() {
        return new ListIndexesOperation<T>(namespace, getCodec(clazz))
                .batchSize(listIndexesOptions.getBatchSize())
                .maxTime(listIndexesOptions.getMaxTime(MILLISECONDS), MILLISECONDS);
    }

}
