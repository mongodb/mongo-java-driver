/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.impl;

import org.bson.io.BasicInputBuffer;
import org.bson.io.InputBuffer;
import org.mongodb.MongoInterruptedException;
import org.mongodb.ServerAddress;
import org.mongodb.io.BufferPool;
import org.mongodb.io.ChannelAwareOutputBuffer;
import org.mongodb.io.MongoSocketReadException;
import org.mongodb.io.MongoSocketReadTimeoutException;
import org.mongodb.io.PooledInputBuffer;
import org.mongodb.io.ResponseBuffers;
import org.mongodb.protocol.MongoReplyHeader;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;

import static org.mongodb.protocol.MongoReplyHeader.REPLY_HEADER_LENGTH;

abstract class DefaultSyncConnection implements Connection {
    private final ServerAddress serverAddress;
    private final BufferPool<ByteBuffer> bufferPool;
    private volatile boolean isClosed;

    DefaultSyncConnection(final ServerAddress serverAddress, final BufferPool<ByteBuffer> bufferPool) {
        this.serverAddress = serverAddress;
        this.bufferPool = bufferPool;
    }

    @Override
    public void close() {
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    public void sendMessage(final ChannelAwareOutputBuffer buffer) {
        check();
        sendOneWayMessage(buffer);
    }

    public ResponseBuffers sendAndReceiveMessage(final ChannelAwareOutputBuffer buffer) {
        check();
        long start = System.nanoTime();
        sendOneWayMessage(buffer);
        return receiveMessage(start);
    }

    @Override
    public ResponseBuffers receiveMessage() {
        check();
        return receiveMessage(System.nanoTime());
    }

    protected abstract void ensureOpen();

    protected abstract void sendOneWayMessage(final ChannelAwareOutputBuffer buffer);

    protected abstract void fillAndFlipBuffer(final ByteBuffer buffer);

    protected void handleIOException(final IOException e) {
        close();
        if (e instanceof SocketTimeoutException) {
            throw new MongoSocketReadTimeoutException("Exception receiving message", serverAddress, (SocketTimeoutException) e);
        }
        else if (e instanceof InterruptedIOException || e instanceof ClosedByInterruptException) {
            throw new MongoInterruptedException("Exception receiving message", e);
        }
        else {
            throw new MongoSocketReadException("Exception receiving message", serverAddress, e);
        }
    }

    private ResponseBuffers receiveMessage(final long start) {
        ByteBuffer headerByteBuffer = bufferPool.get(REPLY_HEADER_LENGTH);

        final MongoReplyHeader replyHeader;
        try {
            fillAndFlipBuffer(headerByteBuffer);
            final InputBuffer headerInputBuffer = new BasicInputBuffer(headerByteBuffer);

            replyHeader = new MongoReplyHeader(headerInputBuffer);
        } finally {
            bufferPool.done(headerByteBuffer);
        }

        PooledInputBuffer bodyInputBuffer = null;

        if (replyHeader.getNumberReturned() > 0) {
            ByteBuffer bodyByteBuffer = bufferPool.get(replyHeader.getMessageLength() - REPLY_HEADER_LENGTH);
            fillAndFlipBuffer(bodyByteBuffer);

            bodyInputBuffer = new PooledInputBuffer(bodyByteBuffer, bufferPool);
        }

        return new ResponseBuffers(replyHeader, bodyInputBuffer, System.nanoTime() - start);
    }

    private void check() {
        ensureOpen();
    }
}
