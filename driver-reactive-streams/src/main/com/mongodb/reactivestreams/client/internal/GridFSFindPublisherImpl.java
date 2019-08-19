/*
 * Copyright 2016 MongoDB, Inc.
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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.Block;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.model.Collation;
import com.mongodb.reactivestreams.client.gridfs.GridFSFindPublisher;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;


@SuppressWarnings("deprecation")
final class GridFSFindPublisherImpl implements GridFSFindPublisher {
    private final com.mongodb.async.client.gridfs.GridFSFindIterable wrapped;

    GridFSFindPublisherImpl(final com.mongodb.async.client.gridfs.GridFSFindIterable wrapped) {
        this.wrapped = notNull("GridFSFindIterable", wrapped);
    }

    @Override
    public Publisher<GridFSFile> first() {
        return new SingleResultObservableToPublisher<GridFSFile>(
                new Block<com.mongodb.async.SingleResultCallback<GridFSFile>>() {
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<GridFSFile> callback) {
                        wrapped.first(callback);
                    }
                });
    }

    @Override
    public GridFSFindPublisher sort(final Bson sort) {
        wrapped.sort(sort);
        return this;
    }

    @Override
    public GridFSFindPublisher skip(final int skip) {
        wrapped.skip(skip);
        return this;
    }

    @Override
    public GridFSFindPublisher limit(final int limit) {
        wrapped.limit(limit);
        return this;
    }

    @Override
    public GridFSFindPublisher filter(final Bson filter) {
        wrapped.filter(filter);
        return this;
    }

    @Override
    public GridFSFindPublisher maxTime(final long maxTime, final TimeUnit timeUnit) {
        wrapped.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public GridFSFindPublisher noCursorTimeout(final boolean noCursorTimeout) {
        wrapped.noCursorTimeout(noCursorTimeout);
        return this;
    }

    @Override
    public GridFSFindPublisher collation(final Collation collation) {
        wrapped.collation(collation);
        return this;
    }

    @Override
    public GridFSFindPublisher batchSize(final int batchSize) {
        wrapped.batchSize(batchSize);
        return this;
    }

    @Override
    public void subscribe(final Subscriber<? super GridFSFile> s) {
        new ObservableToPublisher<GridFSFile>(com.mongodb.async.client.Observables.observe(wrapped)).subscribe(s);
    }
}
