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

import com.mongodb.Block;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.async.client.DistinctIterable;
import com.mongodb.internal.async.client.Observables;
import com.mongodb.reactivestreams.client.DistinctPublisher;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;


final class DistinctPublisherImpl<TResult> implements DistinctPublisher<TResult> {

    private final DistinctIterable<TResult> wrapped;

    DistinctPublisherImpl(final DistinctIterable<TResult> wrapped) {
        this.wrapped = notNull("wrapped", wrapped);
    }

    @Override
    public DistinctPublisher<TResult> filter(final Bson filter) {
        wrapped.filter(filter);
        return this;
    }

    @Override
    public DistinctPublisher<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        wrapped.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public DistinctPublisher<TResult> collation(final Collation collation) {
        wrapped.collation(collation);
        return this;
    }

    @Override
    public DistinctPublisher<TResult> batchSize(final int batchSize) {
        wrapped.batchSize(batchSize);
        return this;
    }

    @Override
    public Publisher<TResult> first() {
        return new SingleResultObservableToPublisher<TResult>(
                new Block<com.mongodb.async.SingleResultCallback<TResult>>() {
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<TResult> callback) {
                        wrapped.first(callback);
                    }
                });
    }

    @Override
    public void subscribe(final Subscriber<? super TResult> s) {
        new ObservableToPublisher<TResult>(Observables.observe(wrapped)).subscribe(s);
    }
}
