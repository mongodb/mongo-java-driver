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
import com.mongodb.CursorType;
import com.mongodb.client.model.Collation;
import com.mongodb.reactivestreams.client.FindPublisher;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;


@SuppressWarnings("deprecation")
final class FindPublisherImpl<TResult> implements FindPublisher<TResult> {

    private final com.mongodb.async.client.FindIterable<TResult> wrapped;

    FindPublisherImpl(final com.mongodb.async.client.FindIterable<TResult> wrapped) {
        this.wrapped = notNull("wrapped", wrapped);
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
    public FindPublisher<TResult> filter(final Bson filter) {
        wrapped.filter(filter);
        return this;
    }

    @Override
    public FindPublisher<TResult> limit(final int limit) {
        wrapped.limit(limit);
        return this;
    }

    @Override
    public FindPublisher<TResult> skip(final int skip) {
        wrapped.skip(skip);
        return this;
    }

    @Override
    public FindPublisher<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        wrapped.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public FindPublisher<TResult> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        wrapped.maxAwaitTime(maxAwaitTime, timeUnit);
        return this;
    }

    @Override
    public FindPublisher<TResult> projection(final Bson projection) {
        wrapped.projection(projection);
        return this;
    }

    @Override
    public FindPublisher<TResult> sort(final Bson sort) {
        wrapped.sort(sort);
        return this;
    }

    @Override
    public FindPublisher<TResult> noCursorTimeout(final boolean noCursorTimeout) {
        wrapped.noCursorTimeout(noCursorTimeout);
        return this;
    }

    @Override
    public FindPublisher<TResult> oplogReplay(final boolean oplogReplay) {
        wrapped.oplogReplay(oplogReplay);
        return this;
    }

    @Override
    public FindPublisher<TResult> partial(final boolean partial) {
        wrapped.partial(partial);
        return this;
    }

    @Override
    public FindPublisher<TResult> cursorType(final CursorType cursorType) {
        wrapped.cursorType(cursorType);
        return this;
    }

    @Override
    public FindPublisher<TResult> collation(final Collation collation) {
        wrapped.collation(collation);
        return this;
    }

    @Override
    public FindPublisher<TResult> comment(final String comment) {
        wrapped.comment(comment);
        return this;
    }

    @Override
    public FindPublisher<TResult> hint(final Bson hint) {
        wrapped.hint(hint);
        return this;
    }

    @Override
    public FindPublisher<TResult> max(final Bson max) {
        wrapped.max(max);
        return this;
    }

    @Override
    public FindPublisher<TResult> min(final Bson min) {
        wrapped.min(min);
        return this;
    }

    @Override
    public FindPublisher<TResult> returnKey(final boolean returnKey) {
        wrapped.returnKey(returnKey);
        return this;
    }

    @Override
    public FindPublisher<TResult> showRecordId(final boolean showRecordId) {
        wrapped.showRecordId(showRecordId);
        return this;
    }

    @Override
    public FindPublisher<TResult> batchSize(final int batchSize) {
        wrapped.batchSize(batchSize);
        return this;
    }

    @Override
    public void subscribe(final Subscriber<? super TResult> s) {
        new ObservableToPublisher<TResult>(com.mongodb.async.client.Observables.observe(wrapped)).subscribe(s);
    }
}
