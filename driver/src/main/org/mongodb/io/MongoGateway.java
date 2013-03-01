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
 *
 */

package org.mongodb.io;

import org.bson.io.BasicInputBuffer;
import org.bson.io.InputBuffer;
import org.mongodb.Document;
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoCursorNotFoundException;
import org.mongodb.MongoQueryFailureException;
import org.mongodb.ServerAddress;
import org.mongodb.protocol.MongoGetMoreMessage;
import org.mongodb.protocol.MongoReplyHeader;
import org.mongodb.protocol.MongoReplyMessage;
import org.mongodb.protocol.MongoRequestMessage;
import org.mongodb.result.ServerCursor;
import org.mongodb.serialization.Serializer;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;

import static org.mongodb.protocol.MongoReplyHeader.REPLY_HEADER_LENGTH;


// TODO: authentication

/**
 * A gateway for the MongoDB wire protocol.
 * <p>
 * Note: This class is not part of the public API.  It may break binary compatibility even in minor releases.
 */
public abstract class MongoGateway {
    private final ServerAddress address;
    private final BufferPool<ByteBuffer> pool;
    private final Serializer<Document> errorSerializer;

    public static MongoGateway create(final ServerAddress address, final BufferPool<ByteBuffer> pool,
                                      final Serializer<Document> errorSerializer,
                                      final MongoClientOptions options) {
        if (options.isSSLEnabled()) {
            return new MongoSocketGateway(address, pool, errorSerializer, SSLSocketFactory.getDefault());
        }
        else if (System.getProperty("org.mongodb.useSocket", "false").equals("true")) {
            return new MongoSocketGateway(address, pool, errorSerializer, SocketFactory.getDefault());
        }
        else {
            return new MongoSocketChannelGateway(address, pool, errorSerializer);
        }
    }

    public ServerAddress getAddress() {
        return address;
    }

    public void sendMessage(final MongoRequestMessage message) {
        ensureOpen();
        sendOneWayMessage(message);
    }

    public <T> MongoReplyMessage<T> sendAndReceiveMessasge(final MongoRequestMessage message, final Serializer<T> serializer) {
        ensureOpen();
        long start = System.nanoTime();
        sendOneWayMessage(message);
        return receiveMessage(message, serializer, start);
    }

    public abstract void close();

    protected MongoGateway(final ServerAddress address, final BufferPool<ByteBuffer> pool, final Serializer<Document> errorSerializer) {
        this.address = address;
        this.pool = pool;
        this.errorSerializer = errorSerializer;
    }

    protected abstract void ensureOpen();

    protected abstract void sendOneWayMessage(final MongoRequestMessage message);

    protected abstract void fillAndFlipBuffer(final ByteBuffer buffer);

    protected void handleIOException(final IOException e) {
        if (e instanceof SocketTimeoutException) {
            throw new MongoSocketReadTimeoutException("Exception receiving message", address, (SocketTimeoutException) e);
        }
        else if (e instanceof InterruptedIOException) {
            throw new MongoSocketInterruptedReadException("Exception receiving message", address, (InterruptedIOException) e);
        }
        else {
            throw new MongoSocketReadException("Exception receiving message", address, e);
        }
    }

    private <T> MongoReplyMessage<T> receiveMessage(final MongoRequestMessage message, final Serializer<T> serializer, final long start) {
        ByteBuffer headerByteBuffer = null;
        ByteBuffer bodyByteBuffer = null;
        try {
            headerByteBuffer = pool.get(REPLY_HEADER_LENGTH);
            fillAndFlipBuffer(headerByteBuffer);
            final InputBuffer headerInputBuffer = new BasicInputBuffer(headerByteBuffer);

            final MongoReplyHeader replyHeader = new MongoReplyHeader(headerInputBuffer);

            InputBuffer bodyInputBuffer = null;

            if (replyHeader.getNumberReturned() > 0) {
                bodyByteBuffer = pool.get(replyHeader.getMessageLength() - REPLY_HEADER_LENGTH);
                fillAndFlipBuffer(bodyByteBuffer);

                bodyInputBuffer = new BasicInputBuffer(bodyByteBuffer);
            }

            if (replyHeader.isCursorNotFound()) {
                throw new MongoCursorNotFoundException(new ServerCursor(((MongoGetMoreMessage) message).getCursorId(),
                        address));
            }
            else if (replyHeader.isQueryFailure()) {
                final Document errorDocument = new MongoReplyMessage<Document>(replyHeader, bodyInputBuffer,
                        errorSerializer, System.nanoTime() - start).getDocuments().get(0);
                throw new MongoQueryFailureException(address, errorDocument);
            }
            return new MongoReplyMessage<T>(replyHeader, bodyInputBuffer, serializer, System.nanoTime() - start);
        } finally {
            if (headerByteBuffer != null) {
                pool.done(headerByteBuffer);
            }
            if (bodyByteBuffer != null) {
                pool.done(bodyByteBuffer);
            }
        }
    }
}
