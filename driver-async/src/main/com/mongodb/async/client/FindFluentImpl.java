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
import com.mongodb.ReadPreference;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.client.model.FindOptions;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.FindOperation;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class FindFluentImpl<T> implements FindFluent<T> {
    private final MongoNamespace namespace;
    private final Class<T> clazz;
    private final ReadPreference readPreference;
    private final CodecRegistry codecRegistry;
    private final AsyncOperationExecutor executor;
    private final FindOptions findOptions;
    private Object filter;

    FindFluentImpl(final MongoNamespace namespace, final Class<T> clazz, final CodecRegistry codecRegistry,
                   final ReadPreference readPreference, final AsyncOperationExecutor executor,
                   final Object filter, final FindOptions findOptions) {
        this.namespace = notNull("namespace", namespace);
        this.clazz = notNull("clazz", clazz);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.executor = notNull("executor", executor);
        this.filter = notNull("filter", filter);
        this.findOptions = notNull("findOptions", findOptions);
    }

    @Override
    public FindFluent<T> filter(final Object filter) {
        this.filter = filter;
        return this;
    }

    @Override
    public FindFluent<T> limit(final int limit) {
        findOptions.limit(limit);
        return this;
    }

    @Override
    public FindFluent<T> skip(final int skip) {
        findOptions.skip(skip);
        return this;
    }

    @Override
    public FindFluent<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        findOptions.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public FindFluent<T> batchSize(final int batchSize) {
        findOptions.batchSize(batchSize);
        return this;
    }

    @Override
    public FindFluent<T> modifiers(final Object modifiers) {
        findOptions.modifiers(modifiers);
        return this;
    }

    @Override
    public FindFluent<T> projection(final Object projection) {
        findOptions.projection(projection);
        return this;
    }

    @Override
    public FindFluent<T> sort(final Object sort) {
        findOptions.sort(sort);
        return this;
    }

    @Override
    public FindFluent<T> noCursorTimeout(final boolean noCursorTimeout) {
        findOptions.noCursorTimeout(noCursorTimeout);
        return this;
    }

    @Override
    public FindFluent<T> oplogReplay(final boolean oplogReplay) {
        findOptions.oplogReplay(oplogReplay);
        return this;
    }

    @Override
    public FindFluent<T> partial(final boolean partial) {
        findOptions.partial(partial);
        return this;
    }

    @Override
    public FindFluent<T> cursorType(final CursorType cursorType) {
        findOptions.cursorType(cursorType);
        return this;
    }

    @Override
    public void first(final SingleResultCallback<T> callback) {
        execute(createQueryOperation().batchSize(0).limit(-1)).first(callback);
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
        return execute().map(mapper);
    }

    private MongoIterable<T> execute() {
        return execute(createQueryOperation());
    }

    private MongoIterable<T> execute(final FindOperation<T> operation) {
        return new OperationIterable<T>(operation, readPreference, executor);
    }

    private <C> Codec<C> getCodec(final Class<C> clazz) {
        return codecRegistry.get(clazz);
    }

    private FindOperation<T> createQueryOperation() {
        return new FindOperation<T>(namespace, getCodec(clazz))
               .filter(asBson(filter))
               .batchSize(findOptions.getBatchSize())
               .skip(findOptions.getSkip())
               .limit(findOptions.getLimit())
               .maxTime(findOptions.getMaxTime(MILLISECONDS), MILLISECONDS)
               .modifiers(asBson(findOptions.getModifiers()))
               .projection(asBson(findOptions.getProjection()))
               .sort(asBson(findOptions.getSort()))
               .cursorType(findOptions.getCursorType())
               .noCursorTimeout(findOptions.isNoCursorTimeout())
               .oplogReplay(findOptions.isOplogReplay())
               .partial(findOptions.isPartial())
               .slaveOk(readPreference.isSlaveOk());
    }

    private BsonDocument asBson(final Object document) {
        return BsonDocumentWrapper.asBsonDocument(document, codecRegistry);
    }

}
