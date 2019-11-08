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
import com.mongodb.client.model.MapReduceAction;
import com.mongodb.internal.async.client.AsyncMapReduceIterable;
import com.mongodb.internal.async.client.Observables;
import com.mongodb.reactivestreams.client.MapReducePublisher;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;


final class MapReducePublisherImpl<TResult> implements MapReducePublisher<TResult> {
    private final AsyncMapReduceIterable<TResult> wrapped;

    MapReducePublisherImpl(final AsyncMapReduceIterable<TResult> wrapped) {
        this.wrapped = notNull("wrapped", wrapped);
    }


    @Override
    public MapReducePublisher<TResult> collectionName(final String collectionName) {
        wrapped.collectionName(collectionName);
        return this;
    }

    @Override
    public MapReducePublisher<TResult> finalizeFunction(final String finalizeFunction) {
        wrapped.finalizeFunction(finalizeFunction);
        return this;
    }

    @Override
    public MapReducePublisher<TResult> scope(final Bson scope) {
        wrapped.scope(scope);
        return this;
    }

    @Override
    public MapReducePublisher<TResult> sort(final Bson sort) {
        wrapped.sort(sort);
        return this;
    }

    @Override
    public MapReducePublisher<TResult> filter(final Bson filter) {
        wrapped.filter(filter);
        return this;
    }

    @Override
    public MapReducePublisher<TResult> limit(final int limit) {
        wrapped.limit(limit);
        return this;
    }

    @Override
    public MapReducePublisher<TResult> jsMode(final boolean jsMode) {
        wrapped.jsMode(jsMode);
        return this;
    }

    @Override
    public MapReducePublisher<TResult> verbose(final boolean verbose) {
        wrapped.verbose(verbose);
        return this;
    }

    @Override
    public MapReducePublisher<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        wrapped.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public MapReducePublisher<TResult> action(final MapReduceAction action) {
        wrapped.action(action);
        return this;
    }

    @Override
    public MapReducePublisher<TResult> databaseName(final String databaseName) {
        wrapped.databaseName(databaseName);
        return this;
    }

    @Override
    public MapReducePublisher<TResult> sharded(final boolean sharded) {
        wrapped.sharded(sharded);
        return this;
    }

    @Override
    public MapReducePublisher<TResult> nonAtomic(final boolean nonAtomic) {
        wrapped.nonAtomic(nonAtomic);
        return this;
    }

    @Override
    public MapReducePublisher<TResult> bypassDocumentValidation(final Boolean bypassDocumentValidation) {
        wrapped.bypassDocumentValidation(bypassDocumentValidation);
        return this;
    }

    @Override
    public Publisher<Void> toCollection() {
        return new SingleResultObservableToPublisher<>(wrapped::toCollection);
    }

    @Override
    public MapReducePublisher<TResult> collation(final Collation collation) {
        wrapped.collation(collation);
        return this;
    }

    @Override
    public MapReducePublisher<TResult> batchSize(final int batchSize) {
        wrapped.batchSize(batchSize);
        return this;
    }

    @Override
    public Publisher<TResult> first() {
        return new SingleResultObservableToPublisher<>(wrapped::first);
    }

    @Override
    public void subscribe(final Subscriber<? super TResult> s) {
        new ObservableToPublisher<>(Observables.observe(wrapped)).subscribe(s);
    }
}
