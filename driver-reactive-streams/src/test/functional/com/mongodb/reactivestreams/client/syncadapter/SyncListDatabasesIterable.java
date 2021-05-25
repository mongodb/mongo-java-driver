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

import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ListDatabasesPublisher;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

class SyncListDatabasesIterable<T> extends SyncMongoIterable<T> implements ListDatabasesIterable<T> {
    private final ListDatabasesPublisher<T> wrapped;

    SyncListDatabasesIterable(final ListDatabasesPublisher<T> wrapped) {
        super(wrapped);
        this.wrapped = wrapped;
    }

    @Deprecated
    @Override
    public ListDatabasesIterable<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        wrapped.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public ListDatabasesIterable<T> batchSize(final int batchSize) {
        wrapped.batchSize(batchSize);
        return this;
    }

    @Override
    public ListDatabasesIterable<T> filter(@Nullable final Bson filter) {
        wrapped.filter(filter);
        return this;
    }

    @Override
    public ListDatabasesIterable<T> nameOnly(@Nullable final Boolean nameOnly) {
        wrapped.nameOnly(nameOnly);
        return this;
    }

    @Override
    public ListDatabasesIterable<T> authorizedDatabasesOnly(@Nullable final Boolean authorizedDatabasesOnly) {
        wrapped.authorizedDatabasesOnly(authorizedDatabasesOnly);
        return this;
    }
}
