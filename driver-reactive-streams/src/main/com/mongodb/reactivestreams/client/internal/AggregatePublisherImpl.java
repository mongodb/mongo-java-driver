/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.client.model.Collation;
import com.mongodb.internal.async.client.AsyncAggregateIterable;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;


final class AggregatePublisherImpl<TResult> implements AggregatePublisher<TResult> {

    private final AsyncAggregateIterable<TResult> wrapped;

    AggregatePublisherImpl(final AsyncAggregateIterable<TResult> wrapped) {
        this.wrapped = notNull("wrapped", wrapped);
    }


    @Override
    public AggregatePublisher<TResult> allowDiskUse(final Boolean allowDiskUse) {
        wrapped.allowDiskUse(allowDiskUse);
        return this;
    }

    @Override
    public AggregatePublisher<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        wrapped.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public AggregatePublisher<TResult> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        wrapped.maxAwaitTime(maxAwaitTime, timeUnit);
        return this;
    }

    @Override
    public AggregatePublisher<TResult> bypassDocumentValidation(final Boolean bypassDocumentValidation) {
        wrapped.bypassDocumentValidation(bypassDocumentValidation);
        return this;
    }

    @Override
    public Publisher<Void> toCollection() {
        return Publishers.publish(wrapped::toCollection);
    }

    @Override
    public AggregatePublisher<TResult> collation(final Collation collation) {
        wrapped.collation(collation);
        return this;
    }

    @Override
    public AggregatePublisher<TResult> comment(final String comment) {
        wrapped.comment(comment);
        return this;
    }

    @Override
    public AggregatePublisher<TResult> hint(final Bson hint) {
        wrapped.hint(hint);
        return this;
    }

    @Override
    public AggregatePublisher<TResult> batchSize(final int batchSize) {
        wrapped.batchSize(batchSize);
        return this;
    }

    @Override
    public Publisher<TResult> first() {
        return Publishers.publish(wrapped::first);
    }

    @Override
    public void subscribe(final Subscriber<? super TResult> s) {
        Publishers.publish(wrapped).subscribe(s);
    }
}
