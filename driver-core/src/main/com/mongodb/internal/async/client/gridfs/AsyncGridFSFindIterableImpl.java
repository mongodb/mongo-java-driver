/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.internal.async.client.gridfs;

import com.mongodb.Block;
import com.mongodb.Function;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.client.AsyncFindIterable;
import com.mongodb.internal.async.client.AsyncMongoIterable;
import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

final class AsyncGridFSFindIterableImpl implements AsyncGridFSFindIterable {
    private final AsyncFindIterable<GridFSFile> underlying;

    AsyncGridFSFindIterableImpl(final AsyncFindIterable<GridFSFile> underlying) {
        this.underlying = underlying;
    }

    @Override
    public AsyncGridFSFindIterable sort(@Nullable final Bson sort) {
        underlying.sort(sort);
        return this;
    }

    @Override
    public AsyncGridFSFindIterable skip(final int skip) {
        underlying.skip(skip);
        return this;
    }

    @Override
    public AsyncGridFSFindIterable limit(final int limit) {
        underlying.limit(limit);
        return this;
    }

    @Override
    public AsyncGridFSFindIterable filter(@Nullable final Bson filter) {
        underlying.filter(filter);
        return this;
    }

    @Override
    public AsyncGridFSFindIterable maxTime(final long maxTime, final TimeUnit timeUnit) {
        underlying.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public AsyncGridFSFindIterable noCursorTimeout(final boolean noCursorTimeout) {
        underlying.noCursorTimeout(noCursorTimeout);
        return this;
    }

    @Override
    public void first(final SingleResultCallback<GridFSFile> callback) {
        underlying.first(callback);
    }

    @Override
    public void forEach(final Block<? super GridFSFile> block, final SingleResultCallback<Void> callback) {
        underlying.forEach(block, callback);
    }

    @Override
    public <A extends Collection<? super GridFSFile>> void into(final A target, final SingleResultCallback<A> callback) {
        underlying.into(target, callback);
    }

    @Override
    public <U> AsyncMongoIterable<U> map(final Function<GridFSFile, U> mapper) {
        return underlying.map(mapper);
    }

    @Override
    public AsyncGridFSFindIterable batchSize(final int batchSize) {
        underlying.batchSize(batchSize);
        return this;
    }

    @Override
    public Integer getBatchSize() {
        return underlying.getBatchSize();
    }

    @Override
    public AsyncGridFSFindIterable collation(@Nullable final Collation collation) {
        underlying.collation(collation);
        return this;
    }

    @Override
    public void batchCursor(final SingleResultCallback<AsyncBatchCursor<GridFSFile>> callback) {
        underlying.batchCursor(callback);
    }

}
