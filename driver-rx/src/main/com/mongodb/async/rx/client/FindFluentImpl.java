/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.async.rx.client;

import rx.Observable;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

class FindFluentImpl<T> implements FindFluent<T> {
    private final com.mongodb.async.client.FindFluent<T> wrapped;

    public FindFluentImpl(final com.mongodb.async.client.FindFluent<T> wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public FindFluent<T> filter(final Object filter) {
        wrapped.filter(filter);
        return this;
    }

    @Override
    public FindFluent<T> limit(final int limit) {
        wrapped.limit(limit);
        return this;
    }

    @Override
    public FindFluent<T> skip(final int skip) {
        wrapped.skip(skip);
        return this;
    }

    @Override
    public FindFluent<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        wrapped.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public FindFluent<T> batchSize(final int batchSize) {
        wrapped.batchSize(batchSize);
        return this;
    }

    @Override
    public FindFluent<T> modifiers(final Object modifiers) {
        wrapped.modifiers(modifiers);
        return this;
    }

    @Override
    public FindFluent<T> projection(final Object projection) {
        wrapped.projection(projection);
        return this;
    }

    @Override
    public FindFluent<T> sort(final Object sort) {
        wrapped.sort(sort);
        return this;
    }

    @Override
    public FindFluent<T> awaitData(final boolean awaitData) {
        wrapped.awaitData(awaitData);
        return this;
    }

    @Override
    public FindFluent<T> noCursorTimeout(final boolean noCursorTimeout) {
        wrapped.noCursorTimeout(noCursorTimeout);
        return this;
    }

    @Override
    public FindFluent<T> oplogReplay(final boolean oplogReplay) {
        wrapped.oplogReplay(oplogReplay);
        return this;
    }

    @Override
    public FindFluent<T> partial(final boolean partial) {
        wrapped.partial(partial);
        return this;
    }

    @Override
    public FindFluent<T> tailable(final boolean tailable) {
        wrapped.tailable(tailable);
        return this;
    }

    @Override
    public Observable<T> first() {
        return new OperationIterable<T>(wrapped).first();
    }

    @Override
    public Observable<T> toObservable() {
        return new OperationIterable<T>(wrapped).toObservable();
    }
}
