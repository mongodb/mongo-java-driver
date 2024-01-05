/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client.syncadapter;

import com.mongodb.client.ListCollectionNamesIterable;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ListCollectionNamesPublisher;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

final class SyncListCollectionNamesIterable extends SyncMongoIterable<String> implements ListCollectionNamesIterable {
    private final ListCollectionNamesPublisher wrapped;

    SyncListCollectionNamesIterable(final ListCollectionNamesPublisher wrapped) {
        super(wrapped);
        this.wrapped = wrapped;
    }

    @Override
    public ListCollectionNamesIterable filter(@Nullable final Bson filter) {
        wrapped.filter(filter);
        return this;
    }

    @Override
    @Deprecated
    public ListCollectionNamesIterable maxTime(final long maxTime, final TimeUnit timeUnit) {
        wrapped.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public ListCollectionNamesIterable batchSize(final int batchSize) {
        wrapped.batchSize(batchSize);
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public ListCollectionNamesIterable comment(final String comment) {
        wrapped.comment(comment);
        return this;
    }

    @Override
    public ListCollectionNamesIterable comment(final BsonValue comment) {
        wrapped.comment(comment);
        return this;
    }

    @Override
    public ListCollectionNamesIterable authorizedCollections(final boolean authorizedCollections) {
        wrapped.authorizedCollections(authorizedCollections);
        return this;
    }
}
