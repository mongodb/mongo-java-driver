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

import com.mongodb.client.ListCollectionsFluent;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabaseOptions;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.ListCollectionsOptions;
import com.mongodb.operation.ListCollectionsOperation;
import com.mongodb.operation.OperationExecutor;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.codecs.Codec;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class ListCollectionsFluentImpl<T> implements ListCollectionsFluent<T> {
    private final String databaseName;
    private final MongoDatabaseOptions options;
    private final OperationExecutor executor;
    private final ListCollectionsOptions listCollectionsOptions;
    private final Class<T> clazz;

    ListCollectionsFluentImpl(final String databaseName, final MongoDatabaseOptions options, final OperationExecutor executor,
                              final ListCollectionsOptions listCollectionsOptions, final Class<T> clazz) {
        this.databaseName = notNull("databaseName", databaseName);
        this.options = notNull("options", options);
        this.executor = notNull("executor", executor);
        this.listCollectionsOptions = notNull("listCollectionsOptions", listCollectionsOptions);
        this.clazz = notNull("clazz", clazz);
    }

    @Override
    public ListCollectionsFluent<T> filter(final Object filter) {
        notNull("filter", filter);
        listCollectionsOptions.filter(filter);
        return this;
    }

    @Override
    public ListCollectionsFluent<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        listCollectionsOptions.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public ListCollectionsFluent<T> batchSize(final int batchSize) {
        listCollectionsOptions.batchSize(batchSize);
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
        return new OperationIterable<T>(createListCollectionsOperation(), this.options.getReadPreference(), executor);
    }

    private <C> Codec<C> getCodec(final Class<C> clazz) {
        return options.getCodecRegistry().get(clazz);
    }

    private ListCollectionsOperation<T> createListCollectionsOperation() {
        return new ListCollectionsOperation<T>(databaseName, getCodec(clazz))
                .filter(asBson(listCollectionsOptions.getFilter()))
                .batchSize(listCollectionsOptions.getBatchSize())
                .maxTime(listCollectionsOptions.getMaxTime(MILLISECONDS), MILLISECONDS);
    }

    private BsonDocument asBson(final Object document) {
        return BsonDocumentWrapper.asBsonDocument(document, options.getCodecRegistry());
    }

}
