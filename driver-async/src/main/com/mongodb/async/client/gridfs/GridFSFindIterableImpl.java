/*
 * Copyright 2015 MongoDB, Inc.
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

package com.mongodb.async.client.gridfs;

import com.mongodb.Block;
import com.mongodb.Function;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.FindIterable;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoIterable;
import com.mongodb.client.gridfs.model.GridFSFile;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.mongodb.internal.gridfs.GridFSFileHelper.documentToGridFSFileMapper;

final class GridFSFindIterableImpl implements GridFSFindIterable {
    private static final CodecRegistry DEFAULT_CODEC_REGISTRY = MongoClients.getDefaultCodecRegistry();
    private final FindIterable<Document> underlying;

    public GridFSFindIterableImpl(final FindIterable<Document> underlying) {
        this.underlying = underlying;
    }

    @Override
    public GridFSFindIterable sort(final Bson sort) {
        underlying.sort(sort);
        return this;
    }

    @Override
    public GridFSFindIterable skip(final int skip) {
        underlying.skip(skip);
        return this;
    }

    @Override
    public GridFSFindIterable limit(final int limit) {
        underlying.limit(limit);
        return this;
    }

    @Override
    public GridFSFindIterable filter(final Bson filter) {
        underlying.filter(filter);
        return this;
    }

    @Override
    public GridFSFindIterable maxTime(final long maxTime, final TimeUnit timeUnit) {
        underlying.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public GridFSFindIterable noCursorTimeout(final boolean noCursorTimeout) {
        underlying.noCursorTimeout(noCursorTimeout);
        return this;
    }

    @Override
    public void first(final SingleResultCallback<GridFSFile> callback) {
        toMongoIterable().first(callback);
    }

    @Override
    public void forEach(final Block<? super GridFSFile> block, final SingleResultCallback<Void> callback) {
        toMongoIterable().forEach(block, callback);
    }

    @Override
    public <A extends Collection<? super GridFSFile>> void into(final A target, final SingleResultCallback<A> callback) {
        toMongoIterable().into(target, callback);
    }

    @Override
    public <U> MongoIterable<U> map(final Function<GridFSFile, U> mapper) {
        return toMongoIterable().map(mapper);
    }

    @Override
    public GridFSFindIterable batchSize(final int batchSize) {
        underlying.batchSize(batchSize);
        return this;
    }

    @Override
    public void batchCursor(final SingleResultCallback<AsyncBatchCursor<GridFSFile>> callback) {
        toMongoIterable().batchCursor(callback);
    }

    private MongoIterable<GridFSFile> toMongoIterable() {
        return underlying.map(documentToGridFSFileMapper(DEFAULT_CODEC_REGISTRY));
    }
}
