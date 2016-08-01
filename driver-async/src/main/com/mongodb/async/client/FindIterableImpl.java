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

package com.mongodb.async.client;

import com.mongodb.Block;
import com.mongodb.CursorType;
import com.mongodb.Function;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.FindOptions;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.FindOperation;
import org.bson.BsonDocument;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class FindIterableImpl<TDocument, TResult> implements FindIterable<TResult> {
    private final MongoNamespace namespace;
    private final Class<TDocument> documentClass;
    private final Class<TResult> resultClass;
    private final ReadPreference readPreference;
    private final ReadConcern readConcern;
    private final CodecRegistry codecRegistry;
    private final AsyncOperationExecutor executor;
    private final FindOptions findOptions;
    private final Collation collation;
    private Bson filter;

    FindIterableImpl(final MongoNamespace namespace, final Class<TDocument> documentClass, final Class<TResult> resultClass,
                     final CodecRegistry codecRegistry, final ReadPreference readPreference, final ReadConcern readConcern,
                     final AsyncOperationExecutor executor, final Bson filter, final FindOptions findOptions, final Collation collation) {
        this.namespace = notNull("namespace", namespace);
        this.documentClass = notNull("documentClass", documentClass);
        this.resultClass = notNull("resultClass", resultClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.readConcern = notNull("readConcern", readConcern);
        this.executor = notNull("executor", executor);
        this.filter = notNull("filter", filter);
        this.findOptions = notNull("findOptions", findOptions);
        this.collation = collation;
    }

    @Override
    public FindIterable<TResult> filter(final Bson filter) {
        this.filter = filter;
        return this;
    }

    @Override
    public FindIterable<TResult> limit(final int limit) {
        findOptions.limit(limit);
        return this;
    }

    @Override
    public FindIterable<TResult> skip(final int skip) {
        findOptions.skip(skip);
        return this;
    }

    @Override
    public FindIterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        findOptions.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public FindIterable<TResult> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        findOptions.maxAwaitTime(maxAwaitTime, timeUnit);
        return this;
    }

    @Override
    public FindIterable<TResult> batchSize(final int batchSize) {
        findOptions.batchSize(batchSize);
        return this;
    }

    @Override
    public FindIterable<TResult> modifiers(final Bson modifiers) {
        findOptions.modifiers(modifiers);
        return this;
    }

    @Override
    public FindIterable<TResult> projection(final Bson projection) {
        findOptions.projection(projection);
        return this;
    }

    @Override
    public FindIterable<TResult> sort(final Bson sort) {
        findOptions.sort(sort);
        return this;
    }

    @Override
    public FindIterable<TResult> noCursorTimeout(final boolean noCursorTimeout) {
        findOptions.noCursorTimeout(noCursorTimeout);
        return this;
    }

    @Override
    public FindIterable<TResult> oplogReplay(final boolean oplogReplay) {
        findOptions.oplogReplay(oplogReplay);
        return this;
    }

    @Override
    public FindIterable<TResult> partial(final boolean partial) {
        findOptions.partial(partial);
        return this;
    }

    @Override
    public FindIterable<TResult> cursorType(final CursorType cursorType) {
        findOptions.cursorType(cursorType);
        return this;
    }

    @Override
    public void first(final SingleResultCallback<TResult> callback) {
        notNull("callback", callback);
        execute(createQueryOperation().batchSize(0).limit(-1)).first(callback);
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
        return execute(createQueryOperation());
    }

    private MongoIterable<TResult> execute(final FindOperation<TResult> operation) {
        return new OperationIterable<TResult>(operation, readPreference, executor);
    }

    private FindOperation<TResult> createQueryOperation() {
        return new FindOperation<TResult>(namespace, codecRegistry.get(resultClass))
               .filter(toBsonDocument(filter))
               .batchSize(findOptions.getBatchSize())
               .skip(findOptions.getSkip())
               .limit(findOptions.getLimit())
               .maxTime(findOptions.getMaxTime(MILLISECONDS), MILLISECONDS)
               .maxAwaitTime(findOptions.getMaxAwaitTime(MILLISECONDS), MILLISECONDS)
               .modifiers(toBsonDocument(findOptions.getModifiers()))
               .projection(toBsonDocument(findOptions.getProjection()))
               .sort(toBsonDocument(findOptions.getSort()))
               .cursorType(findOptions.getCursorType())
               .noCursorTimeout(findOptions.isNoCursorTimeout())
               .oplogReplay(findOptions.isOplogReplay())
               .partial(findOptions.isPartial())
               .slaveOk(readPreference.isSlaveOk())
               .readConcern(readConcern)
                .collation(collation);
    }

    private BsonDocument toBsonDocument(final Bson document) {
        return document == null ? null : document.toBsonDocument(documentClass, codecRegistry);
    }

}
