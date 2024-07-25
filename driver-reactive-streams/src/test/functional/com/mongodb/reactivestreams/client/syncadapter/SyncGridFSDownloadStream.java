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

import com.mongodb.MongoGridFSException;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.reactivestreams.client.gridfs.GridFSDownloadPublisher;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static com.mongodb.reactivestreams.client.syncadapter.ContextHelper.CONTEXT;
import static java.util.Objects.requireNonNull;

public class SyncGridFSDownloadStream extends GridFSDownloadStream {
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private ByteBuffer byteBuffer;
    private final GridFSDownloadPublisher wrapped;

    public SyncGridFSDownloadStream(final GridFSDownloadPublisher publisher) {
       this.wrapped = publisher;
       this.byteBuffer = ByteBuffer.allocate(0);
    }

    @Override
    public GridFSFile getGridFSFile() {
        throw new UnsupportedOperationException();
    }

    @Override
    public GridFSDownloadStream batchSize(final int batchSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read() {
        checkClosed();
        readAll();

        return byteBuffer.get();
    }

    @Override
    public int read(final byte[] b) {
        checkClosed();
        readAll();
        int remaining = byteBuffer.remaining();
        byteBuffer.get(b);
        return remaining - byteBuffer.remaining();
    }

    @Override
    public int read(final byte[] b, final int off, final int len) {
        checkClosed();
        readAll();
        int remaining = byteBuffer.remaining();
        byteBuffer.get(b, off, len);
        return remaining - byteBuffer.remaining();
    }

    @Override
    public long skip(final long n) {
        checkClosed();
        readAll();
        int position = byteBuffer.position();
        long min = Math.min(position, n);
        byteBuffer.position((int) min);
        return min;
    }

    @Override
    public int available() {
        checkClosed();
        readAll();
        return byteBuffer.remaining();
    }

    @Override
    public void mark() {
        checkClosed();
        readAll();
        byteBuffer.mark();
    }

    @Override
    public void reset() {
        checkClosed();
        readAll();
        byteBuffer.reset();
    }

    @Override
    public void close() {
        closed.set(true);
    }

    private void readAll() {
        List<ByteBuffer> byteBuffers = requireNonNull(Flux
                .from(wrapped).contextWrite(CONTEXT).collectList().block((TIMEOUT_DURATION)));

       byteBuffer = byteBuffers.stream().reduce((byteBuffer1, byteBuffer2) -> {
            byteBuffer1.put(byteBuffer2);
            return byteBuffer1;
        }).orElseThrow(() -> new IllegalStateException("No data found"));
    }

    private void checkClosed() {
        if (closed.get()) {
            throw new MongoGridFSException("The DownloadStream has been closed");
        }
    }
}
