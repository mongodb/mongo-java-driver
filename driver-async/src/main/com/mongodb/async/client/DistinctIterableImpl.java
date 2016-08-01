/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.client.model.Collation;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.DistinctOperation;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class DistinctIterableImpl<TDocument, TResult> implements DistinctIterable<TResult> {
    private final MongoNamespace namespace;
    private final Class<TDocument> documentclass;
    private final Class<TResult> resultClass;
    private final ReadPreference readPreference;
    private final ReadConcern readConcern;
    private final CodecRegistry codecRegistry;
    private final AsyncOperationExecutor executor;
    private final String fieldName;
    private final Collation collation;

    private Bson filter;
    private long maxTimeMS;

    DistinctIterableImpl(final MongoNamespace namespace, final Class<TDocument> documentClass, final Class<TResult> resultClass,
                         final CodecRegistry codecRegistry, final ReadPreference readPreference, final ReadConcern readConcern,
                         final AsyncOperationExecutor executor, final String fieldName, final Bson filter, final Collation collation) {
        this.namespace = notNull("namespace", namespace);
        this.documentclass = notNull("documentClass", documentClass);
        this.resultClass = notNull("resultClass", resultClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.readConcern = notNull("readConcern", readConcern);
        this.executor = notNull("executor", executor);
        this.fieldName = notNull("mapFunction", fieldName);
        this.filter = filter;
        this.collation = collation;
    }

    @Override
    public DistinctIterable<TResult> filter(final Bson filter) {
        this.filter = filter;
        return this;
    }

    @Override
    public DistinctIterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public DistinctIterable<TResult> batchSize(final int batchSize) {
        // Noop - not supported by DistinctIterable
        return this;
    }

    @Override
    public void first(final SingleResultCallback<TResult> callback) {
        notNull("callback", callback);
        execute().first(callback);
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
    public void batchCursor(final SingleResultCallback<AsyncBatchCursor<TResult>> callback) {
        notNull("callback", callback);
        execute().batchCursor(callback);
    }

    private MongoIterable<TResult> execute() {
        DistinctOperation<TResult> operation = new DistinctOperation<TResult>(namespace, fieldName, codecRegistry.get(resultClass))
                .filter(filter == null ? null : filter.toBsonDocument(documentclass, codecRegistry))
                .maxTime(maxTimeMS, MILLISECONDS)
                .readConcern(readConcern)
                .collation(collation);
        return new OperationIterable<TResult>(operation, readPreference, executor);
    }
}
