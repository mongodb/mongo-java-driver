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
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.ListCollectionsOperation;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class ListCollectionsFluentImpl<T> implements ListCollectionsFluent<T> {
    private final String databaseName;
    private final Class<T> clazz;
    private final ReadPreference readPreference;
    private final CodecRegistry codecRegistry;
    private final AsyncOperationExecutor executor;

    private Object filter;
    private int batchSize;
    private long maxTimeMS;

    ListCollectionsFluentImpl(final String databaseName, final Class<T> clazz, final CodecRegistry codecRegistry,
                              final ReadPreference readPreference, final AsyncOperationExecutor executor) {
        this.databaseName = notNull("databaseName", databaseName);
        this.clazz = notNull("clazz", clazz);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.executor = notNull("executor", executor);
    }

    @Override
    public ListCollectionsFluent<T> filter(final Object filter) {
        notNull("filter", filter);
        this.filter = filter;
        return this;
    }

    @Override
    public ListCollectionsFluent<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public ListCollectionsFluent<T> batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    @Override
    public void first(final SingleResultCallback<T> callback) {
        execute(createListCollectionsOperation().batchSize(-1)).first(callback);
    }

    @Override
    public void forEach(final Block<? super T> block, final SingleResultCallback<Void> callback) {
        execute().forEach(block, callback);
    }

    @Override
    public <A extends Collection<? super T>> void into(final A target, final SingleResultCallback<A> callback) {
        execute().into(target, callback);
    }

    @Override
    public <U> MongoIterable<U> map(final Function<T, U> mapper) {
        return new MappingIterable<T, U>(this, mapper);
    }

    @Override
    public void batchCursor(final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        execute().batchCursor(callback);
    }

    private MongoIterable<T> execute() {
        return execute(createListCollectionsOperation());
    }

    private MongoIterable<T> execute(final ListCollectionsOperation<T> operation) {
        return new OperationIterable<T>(operation, readPreference, executor);
    }

    private <C> Codec<C> getCodec(final Class<C> clazz) {
        return codecRegistry.get(clazz);
    }

    private ListCollectionsOperation<T> createListCollectionsOperation() {
        return new ListCollectionsOperation<T>(databaseName, getCodec(clazz))
                .filter(asBson(filter))
                .batchSize(batchSize)
                .maxTime(maxTimeMS, MILLISECONDS);
    }

    private BsonDocument asBson(final Object document) {
        return BsonDocumentWrapper.asBsonDocument(document, codecRegistry);
    }

}
