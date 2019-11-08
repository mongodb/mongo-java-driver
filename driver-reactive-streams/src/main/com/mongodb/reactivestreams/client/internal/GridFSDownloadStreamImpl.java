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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.internal.async.client.gridfs.AsyncGridFSDownloadStream;
import com.mongodb.reactivestreams.client.gridfs.GridFSDownloadStream;
import org.reactivestreams.Publisher;

import java.nio.ByteBuffer;

import static com.mongodb.assertions.Assertions.notNull;


final class GridFSDownloadStreamImpl implements GridFSDownloadStream {
    private final AsyncGridFSDownloadStream wrapped;

    GridFSDownloadStreamImpl(final AsyncGridFSDownloadStream wrapped) {
        this.wrapped = notNull("GridFSDownloadStream", wrapped);
    }

    @Override
    public Publisher<GridFSFile> getGridFSFile() {
        return new SingleResultObservableToPublisher<>(wrapped::getGridFSFile);
    }

    @Override
    public GridFSDownloadStream batchSize(final int batchSize) {
        wrapped.batchSize(batchSize);
        return this;
    }

    @Override
    public Publisher<Integer> read(final ByteBuffer dst) {
        return new SingleResultObservableToPublisher<>(callback -> wrapped.read(dst, callback));
    }

    @Override
    public Publisher<Long> skip(final long bytesToSkip) {
        return new SingleResultObservableToPublisher<>(
                callback -> wrapped.skip(bytesToSkip, callback));
    }

    @Override
    public Publisher<Void> close() {
        return new SingleResultObservableToPublisher<>(wrapped::close);
    }
}
