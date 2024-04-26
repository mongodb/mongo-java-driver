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
import com.mongodb.client.ListCollectionNamesIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;

final class ListCollectionNamesIterableImpl implements ListCollectionNamesIterable {
    private final ListCollectionsIterableImpl<BsonDocument> wrapped;
    private final MongoIterable<String> wrappedWithMapping;

    ListCollectionNamesIterableImpl(final ListCollectionsIterableImpl<BsonDocument> wrapped) {
        this.wrapped = wrapped;
        wrappedWithMapping = wrapped.map(collectionDoc -> collectionDoc.getString("name").getValue());
    }

    @Override
    public ListCollectionNamesIterable filter(@Nullable final Bson filter) {
        wrapped.filter(filter);
        return this;
    }

    @Override
    public ListCollectionNamesIterable maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        wrapped.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public ListCollectionNamesIterable batchSize(final int batchSize) {
        wrapped.batchSize(batchSize);
        return this;
    }

    @Override
    public ListCollectionNamesIterable comment(@Nullable final String comment) {
        wrapped.comment(comment);
        return this;
    }

    @Override
    public ListCollectionNamesIterable comment(@Nullable final BsonValue comment) {
        wrapped.comment(comment);
        return this;
    }

    @Override
    public ListCollectionNamesIterable authorizedCollections(final boolean authorizedCollections) {
        wrapped.authorizedCollections(authorizedCollections);
        return this;
    }

    @Override
    public MongoCursor<String> iterator() {
        return wrappedWithMapping.iterator();
    }

    @Override
    public MongoCursor<String> cursor() {
        return wrappedWithMapping.cursor();
    }

    @Nullable
    @Override
    public String first() {
        return wrappedWithMapping.first();
    }

    @Override
    public <U> MongoIterable<U> map(final Function<String, U> mapper) {
        return wrappedWithMapping.map(mapper);
    }

    @Override
    public <A extends Collection<? super String>> A into(final A target) {
        return wrappedWithMapping.into(target);
    }

    /**
     * This method is used from Groovy code in {@code com.mongodb.client.internal.MongoDatabaseSpecification}.
     */
    @VisibleForTesting(otherwise = PRIVATE)
    ListCollectionsIterableImpl<BsonDocument> getWrapped() {
        return wrapped;
    }
}
