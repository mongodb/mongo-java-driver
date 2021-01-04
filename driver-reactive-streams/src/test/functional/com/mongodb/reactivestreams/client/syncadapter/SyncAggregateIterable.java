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
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.model.Collation;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import org.bson.Document;
import org.bson.conversions.Bson;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static java.util.Objects.requireNonNull;

class SyncAggregateIterable<T> extends SyncMongoIterable<T> implements AggregateIterable<T> {
    private final AggregatePublisher<T> wrapped;

    SyncAggregateIterable(final AggregatePublisher<T> wrapped) {
        super(wrapped);
        this.wrapped = wrapped;
    }

    @Override
    public void toCollection() {
        Mono.from(wrapped.toCollection()).block(TIMEOUT_DURATION);
    }

    @Override
    public AggregateIterable<T> allowDiskUse(@Nullable final Boolean allowDiskUse) {
        wrapped.allowDiskUse(allowDiskUse);
        return this;
    }

    @Override
    public AggregateIterable<T> batchSize(final int batchSize) {
        wrapped.batchSize(batchSize);
        return this;
    }

    @Override
    public AggregateIterable<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        wrapped.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public AggregateIterable<T> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        wrapped.maxAwaitTime(maxAwaitTime, timeUnit);
        return this;
    }

    @Override
    public AggregateIterable<T> bypassDocumentValidation(@Nullable final Boolean bypassDocumentValidation) {
        wrapped.bypassDocumentValidation(bypassDocumentValidation);
        return this;
    }

    @Override
    public AggregateIterable<T> collation(@Nullable final Collation collation) {
        wrapped.collation(collation);
        return this;
    }

    @Override
    public AggregateIterable<T> comment(@Nullable final String comment) {
        wrapped.comment(comment);
        return this;
    }

    @Override
    public AggregateIterable<T> hint(@Nullable final Bson hint) {
        wrapped.hint(hint);
        return this;
    }

    @Override
    public Document explain() {
        return requireNonNull(Mono.from(wrapped.explain()).block(TIMEOUT_DURATION));
    }

    @Override
    public Document explain(final ExplainVerbosity verbosity) {
        return requireNonNull(Mono.from(wrapped.explain(verbosity)).block(TIMEOUT_DURATION));
    }

    @Override
    public <E> E explain(final Class<E> explainResultClass) {
        return requireNonNull(Mono.from(wrapped.explain(explainResultClass)).block(TIMEOUT_DURATION));
    }

    @Override
    public <E> E explain(final Class<E> explainResultClass, final ExplainVerbosity verbosity) {
        return requireNonNull(Mono.from(wrapped.explain(explainResultClass, verbosity)).block(TIMEOUT_DURATION));
    }
}
