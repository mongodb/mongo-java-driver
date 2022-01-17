/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.client.internal;

import com.mongodb.Function;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.ReadOperation;
import com.mongodb.internal.operation.SyncOperations;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class ListCollectionsIterableImpl<TResult> extends MongoIterableImpl<TResult> implements ListCollectionsIterable<TResult> {
    private final SyncOperations<BsonDocument> operations;
    private final String databaseName;
    private final Class<TResult> resultClass;

    private Bson filter;
    private final boolean collectionNamesOnly;
    private boolean authorizedCollections;
    private long maxTimeMS;

    ListCollectionsIterableImpl(@Nullable final ClientSession clientSession, final String databaseName, final boolean collectionNamesOnly,
                                final Class<TResult> resultClass, final CodecRegistry codecRegistry, final ReadPreference readPreference,
                                final OperationExecutor executor, final boolean retryReads) {
        super(clientSession, executor, ReadConcern.DEFAULT, readPreference, retryReads); // TODO: read concern?
        this.collectionNamesOnly = collectionNamesOnly;
        this.operations = new SyncOperations<BsonDocument>(BsonDocument.class, readPreference, codecRegistry, retryReads);
        this.databaseName = notNull("databaseName", databaseName);
        this.resultClass = notNull("resultClass", resultClass);
    }

    @Override
    public ListCollectionsIterable<TResult> filter(@Nullable final Bson filter) {
        this.filter = filter;
        return this;
    }

    @Override
    public ListCollectionsIterableImpl<TResult> authorizedCollections(final boolean authorizedCollections) {
        this.authorizedCollections = authorizedCollections;
        return this;
    }

    @Override
    public ListCollectionsIterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public ListCollectionsIterable<TResult> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public ReadOperation<BatchCursor<TResult>> asReadOperation() {
        return operations.listCollections(databaseName, resultClass, filter, collectionNamesOnly, authorizedCollections,
                getBatchSize(), maxTimeMS);
    }

    @Override
    public <U> ListCollectionsIterable<U> map(final Function<TResult, U> mapper) {
        return new Mapping<>(this, mapper);
    }

    private static final class Mapping<T, U> implements ListCollectionsIterable<U> {
        private final ListCollectionsIterable<T> wrapped;
        private final Function<T, U> mapper;

        Mapping(final ListCollectionsIterable<T> iterable, final Function<T, U> mapper) {
            this.wrapped = iterable;
            this.mapper = mapper;
        }

        @Override
        public Mapping<T, U> filter(@Nullable final Bson filter) {
            wrapped.filter(filter);
            return this;
        }

        @Override
        public ListCollectionsIterable<U> authorizedCollections(final boolean authorizedCollections) {
            wrapped.authorizedCollections(authorizedCollections);
            return this;
        }

        @Override
        public ListCollectionsIterable<U> maxTime(final long maxTime, final TimeUnit timeUnit) {
            wrapped.maxTime(maxTime, timeUnit);
            return this;
        }

        @Override
        public ListCollectionsIterable<U> batchSize(final int batchSize) {
            wrapped.batchSize(batchSize);
            return this;
        }

        @Override
        public MongoCursor<U> iterator() {
            return new MongoMappingCursor<>(wrapped.iterator(), mapper);
        }

        @Override
        public MongoCursor<U> cursor() {
            return iterator();
        }

        @Nullable
        @Override
        public U first() {
            T first = wrapped.first();
            return first == null ? null : mapper.apply(first);
        }

        @Override
        public <W> Mapping<U, W> map(final Function<U, W> mapper) {
            return new Mapping<>(this, mapper);
        }

        @Override
        public <A extends Collection<? super U>> A into(final A target) {
            forEach(target::add);
            return target;
        }

        /**
         * This method is used in tests written in Groovy.
         */
        @VisibleForTesting(otherwise = PRIVATE)
        ListCollectionsIterable<T> getMapped() {
            return wrapped;
        }
    }
}
