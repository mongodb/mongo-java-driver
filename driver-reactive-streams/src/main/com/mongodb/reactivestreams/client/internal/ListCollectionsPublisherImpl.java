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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.ReadConcern;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.ListCollectionsPublisher;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class ListCollectionsPublisherImpl<T> extends BatchCursorPublisher<T> implements ListCollectionsPublisher<T> {

    private final boolean collectionNamesOnly;
    private boolean authorizedCollections;
    private Bson filter;
    private long maxTimeMS;
    private BsonValue comment;

    ListCollectionsPublisherImpl(
            @Nullable final ClientSession clientSession,
            final MongoOperationPublisher<T> mongoOperationPublisher,
            final boolean collectionNamesOnly) {
        super(clientSession, mongoOperationPublisher.withReadConcern(ReadConcern.DEFAULT));
        this.collectionNamesOnly = collectionNamesOnly;
    }

    public ListCollectionsPublisher<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    public ListCollectionsPublisher<T> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    public ListCollectionsPublisher<T> filter(@Nullable final Bson filter) {
        this.filter = filter;
        return this;
    }

    @Override
    public ListCollectionsPublisherImpl<T> authorizedCollections(final boolean authorizedCollections) {
        this.authorizedCollections = authorizedCollections;
        return this;
    }

    @Override
    public ListCollectionsPublisher<T> comment(@Nullable final String comment) {
        this.comment = comment != null ? new BsonString(comment) : null;
        return this;
    }

    @Override
    public ListCollectionsPublisher<T> comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    AsyncReadOperation<AsyncBatchCursor<T>> asAsyncReadOperation(final int initialBatchSize) {
        return getOperations().listCollections(getNamespace().getDatabaseName(), getDocumentClass(), filter, collectionNamesOnly,
                authorizedCollections, initialBatchSize, maxTimeMS, comment);
    }

    <U> ListCollectionsPublisher<U> map(final Function<T, U> mapper) {
        return new Mapping<>(this, mapper);
    }

    private static final class Mapping<T, U> implements ListCollectionsPublisher<U> {
        private final ListCollectionsPublisher<T> mapped;
        private final Flux<U> mappingPublisher;
        private final Function<T, U> mapper;

        Mapping(final ListCollectionsPublisher<T> publisher, final Function<T, U> mapper) {
            this.mapped = publisher;
            mappingPublisher = Flux.from(publisher).map(mapper);
            this.mapper = mapper;
        }

        @Override
        public Mapping<T, U> filter(@Nullable final Bson filter) {
            mapped.filter(filter);
            return this;
        }

        @Override
        public Mapping<T, U> authorizedCollections(final boolean authorizedCollections) {
            mapped.authorizedCollections(authorizedCollections);
            return this;
        }

        @Override
        public Mapping<T, U> maxTime(final long maxTime, final TimeUnit timeUnit) {
            mapped.maxTime(maxTime, timeUnit);
            return this;
        }

        @Override
        public Mapping<T, U> batchSize(final int batchSize) {
            mapped.batchSize(batchSize);
            return this;
        }

        @Override
        public Mapping<T, U> comment(@Nullable final String comment) {
            mapped.comment(comment);
            return this;
        }

        @Override
        public Mapping<T, U> comment(@Nullable final BsonValue comment) {
            mapped.comment(comment);
            return this;
        }

        @Override
        public Publisher<U> first() {
            return Mono.from(mapped.first()).map(mapper);
        }

        @Override
        public void subscribe(final Subscriber<? super U> s) {
            mappingPublisher.subscribe(s);
        }

        /**
         * This method is used in tests via the reflection API. See
         * {@code com.mongodb.reactivestreams.client.internal.TestHelper.assertPublisherIsTheSameAs}
         */
        @VisibleForTesting(otherwise = PRIVATE)
        ListCollectionsPublisher<T> getMapped() {
            return mapped;
        }
    }
}
