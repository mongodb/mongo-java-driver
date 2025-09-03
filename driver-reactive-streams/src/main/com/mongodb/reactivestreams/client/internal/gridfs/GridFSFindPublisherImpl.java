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

package com.mongodb.reactivestreams.client.internal.gridfs;

import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.model.Collation;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.gridfs.GridFSFindPublisher;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class GridFSFindPublisherImpl implements GridFSFindPublisher {
    private final FindPublisher<GridFSFile> wrapped;

    GridFSFindPublisherImpl(final FindPublisher<GridFSFile> wrapped) {
        this.wrapped = notNull("GridFSFindIterable", wrapped);
    }

    @Override
    public Publisher<GridFSFile> first() {
        return wrapped.first();
    }

    @Override
    public GridFSFindPublisher sort(@Nullable final Bson sort) {
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
    public GridFSFindPublisher filter(@Nullable final Bson filter) {
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
    public GridFSFindPublisher collation(@Nullable final Collation collation) {
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
        wrapped.subscribe(s);
    }
}
