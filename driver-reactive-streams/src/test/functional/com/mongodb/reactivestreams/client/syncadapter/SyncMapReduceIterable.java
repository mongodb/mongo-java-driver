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

import com.mongodb.client.model.Collation;
import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;

@SuppressWarnings("deprecation")
class SyncMapReduceIterable<T> extends SyncMongoIterable<T> implements com.mongodb.client.MapReduceIterable<T> {
    private final com.mongodb.reactivestreams.client.MapReducePublisher<T> wrapped;

    SyncMapReduceIterable(final com.mongodb.reactivestreams.client.MapReducePublisher<T> wrapped) {
        super(wrapped);
        this.wrapped = wrapped;
    }

    @Override
    public void toCollection() {
        Mono.from(wrapped.toCollection()).block(TIMEOUT_DURATION);
    }

    @Override
    public com.mongodb.client.MapReduceIterable<T> collectionName(final String collectionName) {
        wrapped.collectionName(collectionName);
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<T> finalizeFunction(@Nullable final String finalizeFunction) {
        wrapped.finalizeFunction(finalizeFunction);
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<T> scope(@Nullable final Bson scope) {
        wrapped.scope(scope);
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<T> sort(@Nullable final Bson sort) {
        wrapped.sort(sort);
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<T> filter(@Nullable final Bson filter) {
        wrapped.filter(filter);
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<T> limit(final int limit) {
        wrapped.limit(limit);
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<T> jsMode(final boolean jsMode) {
        wrapped.jsMode(jsMode);
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<T> verbose(final boolean verbose) {
        wrapped.verbose(verbose);
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        wrapped.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<T> action(final com.mongodb.client.model.MapReduceAction action) {
        wrapped.action(action);
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<T> databaseName(@Nullable final String databaseName) {
        wrapped.databaseName(databaseName);
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<T> sharded(final boolean sharded) {
        wrapped.sharded(sharded);
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<T> nonAtomic(final boolean nonAtomic) {
        wrapped.nonAtomic(nonAtomic);
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<T> batchSize(final int batchSize) {
        wrapped.batchSize(batchSize);
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<T> bypassDocumentValidation(@Nullable final Boolean bypassDocumentValidation) {
        wrapped.bypassDocumentValidation(bypassDocumentValidation);
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<T> collation(@Nullable final Collation collation) {
        wrapped.collation(collation);
        return this;
    }
}
