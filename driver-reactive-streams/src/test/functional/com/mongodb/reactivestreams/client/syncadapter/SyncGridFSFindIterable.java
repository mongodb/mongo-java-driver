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

import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.model.Collation;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.gridfs.GridFSFindPublisher;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

class SyncGridFSFindIterable extends SyncMongoIterable<GridFSFile> implements GridFSFindIterable {
    private final GridFSFindPublisher wrapped;

    SyncGridFSFindIterable(final GridFSFindPublisher wrapped) {
        super(wrapped);
        this.wrapped = wrapped;
    }

    @Override
    public GridFSFindIterable filter(@Nullable final Bson filter) {
        wrapped.filter(filter);
        return this;
    }

    @Override
    public GridFSFindIterable limit(final int limit) {
        wrapped.limit(limit);
        return this;
    }

    @Override
    public GridFSFindIterable skip(final int skip) {
        wrapped.skip(skip);
        return this;
    }

    @Override
    public GridFSFindIterable maxTime(final long maxTime, final TimeUnit timeUnit) {
        wrapped.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public GridFSFindIterable sort(@Nullable final Bson sort) {
        wrapped.sort(sort);
        return this;
    }

    @Override
    public GridFSFindIterable noCursorTimeout(final boolean noCursorTimeout) {
        wrapped.noCursorTimeout(noCursorTimeout);
        return this;
    }

    @Override
    public GridFSFindIterable batchSize(final int batchSize) {
        wrapped.batchSize(batchSize);
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public GridFSFindIterable collation(@Nullable final Collation collation) {
        wrapped.collation(collation);
        return this;
    }
}
