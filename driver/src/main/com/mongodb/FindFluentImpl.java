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

import com.mongodb.client.FindFluent;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.FindOptions;
import com.mongodb.client.options.OperationOptions;
import com.mongodb.operation.BatchCursor;
import com.mongodb.operation.FindOperation;
import com.mongodb.operation.OperationExecutor;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.codecs.Codec;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class FindFluentImpl<T> implements FindFluent<T> {
    private final MongoNamespace namespace;
    private final OperationOptions options;
    private final OperationExecutor executor;
    private final FindOptions findOptions;
    private Object filter;
    private final Class<T> clazz;

    FindFluentImpl(final MongoNamespace namespace, final OperationOptions options, final OperationExecutor executor,
                   final Object filter, final FindOptions findOptions, final Class<T> clazz) {
        this.namespace = notNull("namespace", namespace);
        this.options = notNull("options", options);
        this.executor = notNull("executor", executor);
        this.filter = notNull("filter", filter);
        this.findOptions = notNull("findOptions", findOptions);
        this.clazz = notNull("clazz", clazz);
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
        return new FindOperationIterable(createQueryOperation(), this.options.getReadPreference(), executor);
    }

    private <C> Codec<C> getCodec(final Class<C> clazz) {
        return options.getCodecRegistry().get(clazz);
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
                   .slaveOk(options.getReadPreference().isSlaveOk());
    }

    private BsonDocument asBson(final Object document) {
        return BsonDocumentWrapper.asBsonDocument(document, options.getCodecRegistry());
    }

    private final class FindOperationIterable extends OperationIterable<T> {
        private final ReadPreference readPreference;
        private final OperationExecutor executor;

        FindOperationIterable(final FindOperation<T> operation, final ReadPreference readPreference,
                              final OperationExecutor executor) {
            super(operation, readPreference, executor);
            this.readPreference = readPreference;
            this.executor = executor;
        }

        @Override
        public T first() {
            FindOperation<T> findFirstOperation = createQueryOperation().batchSize(0).limit(-1);
            BatchCursor<T> batchCursor = executor.execute(findFirstOperation, readPreference);
            return batchCursor.hasNext() ? batchCursor.next().iterator().next() : null;
        }
    }
}
