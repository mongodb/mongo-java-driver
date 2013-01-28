/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

package org.mongodb.io;

import org.bson.io.ByteBufferInput;
import org.bson.io.InputBuffer;
import org.bson.types.Document;
import org.bson.util.BufferPool;
import org.mongodb.MongoCursorNotFoundException;
import org.mongodb.MongoQueryFailureException;
import org.mongodb.ServerAddress;
import org.mongodb.protocol.MongoGetMoreMessage;
import org.mongodb.protocol.MongoQueryMessage;
import org.mongodb.protocol.MongoReplyHeader;
import org.mongodb.protocol.MongoReplyMessage;
import org.mongodb.protocol.MongoRequestMessage;
import org.mongodb.result.ServerCursor;
import org.mongodb.serialization.Serializer;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static org.mongodb.protocol.MongoReplyHeader.REPLY_HEADER_LENGTH;

// TODO: migrate all the DBPort configuration
// TODO: authentication

public class MongoChannel {
    private final ServerAddress address;
    private final BufferPool<ByteBuffer> pool;
    private final Serializer<Document> errorSerializer;
    private volatile SocketChannel socketChannel;

    public MongoChannel(final ServerAddress address, final BufferPool<ByteBuffer> pool,
                        final Serializer<Document> errorSerializer) {
        this.address = address;
        this.pool = pool;
        this.errorSerializer = errorSerializer;
    }

    public ServerAddress getAddress() {
        return address;
    }

    public void sendMessage(final MongoRequestMessage message) {
        ensureOpen();
        sendOneWayMessage(message);
    }

    public <T> MongoReplyMessage<T> sendQueryMessage(final MongoQueryMessage message, final Serializer<T> serializer) {
        ensureOpen();
        long start = System.nanoTime();
        sendOneWayMessage(message);
        return receiveMessage(message, serializer, start);
    }

    public <T> MongoReplyMessage<T> sendGetMoreMessage(final MongoGetMoreMessage message,
                                                       final Serializer<T> serializer) {
        ensureOpen();
        long start = System.nanoTime();
        sendOneWayMessage(message);
        return receiveMessage(message, serializer, start);
    }

    private void sendOneWayMessage(final MongoRequestMessage message) {
        try {
            message.pipe(socketChannel);
        } catch (IOException e) {
            throw new MongoSocketWriteException("Exception sending message", address, e);
        } finally {
            message.close();
        }
    }

    private <T> MongoReplyMessage<T> receiveMessage(final MongoRequestMessage message, final Serializer<T> serializer, final long start) {
        ByteBuffer headerByteBuffer = null;
        ByteBuffer bodyByteBuffer = null;
        try {
            headerByteBuffer = pool.get(REPLY_HEADER_LENGTH);
            fillAndFlipBuffer(headerByteBuffer);
            final InputBuffer headerInputBuffer = new ByteBufferInput(headerByteBuffer);

            final MongoReplyHeader replyHeader = new MongoReplyHeader(headerInputBuffer);

            InputBuffer bodyInputBuffer = null;

            if (replyHeader.getNumberReturned() > 0) {
                bodyByteBuffer = pool.get(replyHeader.getMessageLength() - REPLY_HEADER_LENGTH);
                fillAndFlipBuffer(bodyByteBuffer);

                bodyInputBuffer = new ByteBufferInput(bodyByteBuffer);
            }

            if (replyHeader.isCursorNotFound()) {
                throw new MongoCursorNotFoundException(new ServerCursor(((MongoGetMoreMessage) message).getCursorId(),
                        address));
            } else if (replyHeader.isQueryFailure()) {
                final Document errorDocument = new MongoReplyMessage<Document>(replyHeader, bodyInputBuffer,
                        errorSerializer, System.nanoTime() - start).getDocuments().get(0);
                throw new MongoQueryFailureException(address, errorDocument);
            }
            return new MongoReplyMessage<T>(replyHeader, bodyInputBuffer, serializer, System.nanoTime() - start);
        } catch (SocketTimeoutException e) {
            throw new MongoSocketReadTimeoutException("Exception receiving message", address, e);
        } catch (InterruptedIOException e) {
            throw new MongoSocketInterruptedReadException("Exception receiving message", address, e);
        } catch (IOException e) {
            throw new MongoSocketReadException("Exception receiving message", address, e);
        } finally {
            if (headerByteBuffer != null) {
                pool.done(headerByteBuffer);
            }
            if (bodyByteBuffer != null) {
                pool.done(bodyByteBuffer);
            }
        }
    }

    private void fillAndFlipBuffer(final ByteBuffer buffer) throws IOException {
        int totalBytesRead = 0;
        while (totalBytesRead < buffer.limit()) {
            final int bytesRead = socketChannel.read(buffer);
            if (bytesRead == -1) {
                throw new MongoSocketReadException("Prematurely reached end of stream", address);
            }
            totalBytesRead += bytesRead;
        }
        buffer.flip();
    }

    private void ensureOpen() {
        try {
            if (socketChannel == null) {
                socketChannel = SocketChannel.open(address.getSocketAddress());
            }
        } catch (IOException e) {
            throw new MongoSocketOpenException("Exception opening socket", address, e);
        }
    }

    //CHECKSTYLE:OFF
    public void close() {
        try {
            if (socketChannel != null) {
                socketChannel.close();
                socketChannel = null;
            }
        } catch (IOException e) {
            // ignore
        }
    }
    //CHECKSTYLE:ON
}
