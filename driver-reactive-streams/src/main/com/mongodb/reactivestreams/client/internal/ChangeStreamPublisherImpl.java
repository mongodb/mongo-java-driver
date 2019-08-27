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
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.internal.async.client.ChangeStreamIterable;
import com.mongodb.internal.async.client.Observables;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;


final class ChangeStreamPublisherImpl<TResult> implements ChangeStreamPublisher<TResult> {

    private final ChangeStreamIterable<TResult> wrapped;

    ChangeStreamPublisherImpl(final ChangeStreamIterable<TResult> wrapped) {
        this.wrapped = notNull("wrapped", wrapped);
    }

    @Override
    public ChangeStreamPublisher<TResult> fullDocument(final FullDocument fullDocument) {
        wrapped.fullDocument(fullDocument);
        return this;
    }

    @Override
    public ChangeStreamPublisher<TResult> resumeAfter(final BsonDocument resumeToken) {
        wrapped.resumeAfter(resumeToken);
        return this;
    }

    @Override
    public ChangeStreamPublisher<TResult> startAtOperationTime(final BsonTimestamp startAtOperationTime) {
        wrapped.startAtOperationTime(startAtOperationTime);
        return this;
    }

    @Override
    public ChangeStreamPublisher<TResult> startAfter(final BsonDocument startAfter) {
        wrapped.startAfter(startAfter);
        return this;
    }

    @Override
    public ChangeStreamPublisher<TResult> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        wrapped.maxAwaitTime(maxAwaitTime, timeUnit);
        return this;
    }

    @Override
    public ChangeStreamPublisher<TResult> collation(final Collation collation) {
        wrapped.collation(collation);
        return this;
    }

    @Override
    public <TDocument> Publisher<TDocument> withDocumentClass(final Class<TDocument> clazz) {
        return new ObservableToPublisher<TDocument>(Observables.observe(wrapped.withDocumentClass(clazz)));
    }

    @Override
    public ChangeStreamPublisher<TResult> batchSize(final int batchSize) {
        wrapped.batchSize(batchSize);
        return this;
    }

    @Override
    public Publisher<ChangeStreamDocument<TResult>> first() {
        return new SingleResultObservableToPublisher<ChangeStreamDocument<TResult>>(
                new Block<com.mongodb.async.SingleResultCallback<ChangeStreamDocument<TResult>>>() {
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<ChangeStreamDocument<TResult>> callback) {
                        wrapped.first(callback);
                    }
                });
    }

    @Override
    public void subscribe(final Subscriber<? super ChangeStreamDocument<TResult>> s) {
        new ObservableToPublisher<ChangeStreamDocument<TResult>>(Observables.observe(wrapped)).subscribe(s);
    }
}
