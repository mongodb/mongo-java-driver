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

import com.mongodb.ExplainVerbosity;
import com.mongodb.client.ListSearchIndexesIterable;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.Collation;
import com.mongodb.reactivestreams.client.ListSearchIndexesPublisher;
import org.bson.BsonValue;
import org.bson.Document;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static com.mongodb.reactivestreams.client.syncadapter.ContextHelper.CONTEXT;
import static java.util.Objects.requireNonNull;

final class SyncListSearchIndexesIterable<T> extends SyncMongoIterable<T> implements ListSearchIndexesIterable<T> {
    private final ListSearchIndexesPublisher<T> wrapped;

    SyncListSearchIndexesIterable(final ListSearchIndexesPublisher<T> wrapped) {
        super(wrapped);
        this.wrapped = wrapped;
    }

    @Override
    public ListSearchIndexesIterable<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        wrapped.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public ListSearchIndexesIterable<T> collation(final Collation collation) {
        wrapped.collation(collation);
        return this;
    }

    @Override
    public ListSearchIndexesIterable<T> name(final String indexName) {
        wrapped.name(indexName);
        return this;
    }

    @Override
    public ListSearchIndexesIterable<T> allowDiskUse(final Boolean allowDiskUse) {
        wrapped.allowDiskUse(allowDiskUse);
        return this;
    }

    @Override
    public ListSearchIndexesIterable<T> batchSize(final int batchSize) {
        wrapped.batchSize(batchSize);
        return this;
    }

    @Override
    public ListSearchIndexesIterable<T> comment(final String comment) {
        wrapped.comment(comment);
        return this;
    }

    @Override
    public ListSearchIndexesIterable<T> comment(final BsonValue comment) {
        wrapped.comment(comment);
        return this;
    }

    @Override
    public ListSearchIndexesIterable<T> timeoutMode(final TimeoutMode timeoutMode) {
        wrapped.timeoutMode(timeoutMode);
        return this;
    }

    @Override
    public Document explain() {
        return requireNonNull(Mono.from(wrapped.explain()).contextWrite(CONTEXT).block(TIMEOUT_DURATION));
    }

    @Override
    public Document explain(final ExplainVerbosity verbosity) {
        return requireNonNull(Mono.from(wrapped.explain(verbosity)).contextWrite(CONTEXT).block(TIMEOUT_DURATION));
    }

    @Override
    public <E> E explain(final Class<E> explainResultClass) {
        return requireNonNull(Mono.from(wrapped.explain(explainResultClass)).contextWrite(CONTEXT).block(TIMEOUT_DURATION));
    }

    @Override
    public <E> E explain(final Class<E> explainResultClass, final ExplainVerbosity verbosity) {
        return requireNonNull(Mono.from(wrapped.explain(explainResultClass, verbosity)).contextWrite(CONTEXT).block(TIMEOUT_DURATION));
    }
}
