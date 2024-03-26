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

import com.mongodb.internal.VisibleForTesting;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ListCollectionNamesPublisher;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;

public final class ListCollectionNamesPublisherImpl implements ListCollectionNamesPublisher {
    private final ListCollectionsPublisherImpl<Document> wrapped;
    private final Flux<String> wrappedWithMapping;

    ListCollectionNamesPublisherImpl(final ListCollectionsPublisherImpl<Document> wrapped) {
        this.wrapped = wrapped;
        wrappedWithMapping = Flux.from(wrapped).map(ListCollectionNamesPublisherImpl::name);
    }

    @Override
    @Deprecated
    public ListCollectionNamesPublisher maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        wrapped.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public ListCollectionNamesPublisher batchSize(final int batchSize) {
        wrapped.batchSize(batchSize);
        return this;
    }

    @Override
    public ListCollectionNamesPublisher filter(@Nullable final Bson filter) {
        wrapped.filter(filter);
        return this;
    }

    @Override
    public ListCollectionNamesPublisher comment(@Nullable final String comment) {
        wrapped.comment(comment);
        return this;
    }

    @Override
    public ListCollectionNamesPublisher comment(@Nullable final BsonValue comment) {
        wrapped.comment(comment);
        return this;
    }

    @Override
    public ListCollectionNamesPublisher authorizedCollections(final boolean authorizedCollections) {
        wrapped.authorizedCollections(authorizedCollections);
        return this;
    }

    @Override
    public Publisher<String> first() {
        return Mono.fromDirect(wrapped.first()).map(ListCollectionNamesPublisherImpl::name);
    }

    @Override
    public void subscribe(final Subscriber<? super String> subscriber) {
        wrappedWithMapping.subscribe(subscriber);
    }

    @VisibleForTesting(otherwise = PRIVATE)
    public BatchCursorPublisher<Document> getWrapped() {
        return wrapped;
    }

    private static String name(final Document collectionDoc) {
        return collectionDoc.getString("name");
    }
}
