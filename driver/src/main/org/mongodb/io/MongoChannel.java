/**
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
 *
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
import org.mongodb.serialization.Serializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

// TODO: migrate all the DBPort configuration
// TODO: authentication

/**
 *
 */
public class MongoChannel {
    private final ServerAddress address;
    private final BufferPool<ByteBuffer> pool;
    private final Serializer<Document> errorSerializer;
    private volatile SocketChannel socketChannel;

    public MongoChannel(final ServerAddress address, final BufferPool<ByteBuffer> pool,
                        Serializer<Document> errorSerializer) {
        this.address = address;
        this.pool = pool;
        this.errorSerializer = errorSerializer;
    }

    public ServerAddress getAddress() {
        return address;
    }

    public void sendOneWayMessage(final MongoRequestMessage message) {

        try {
            if (socketChannel == null) {
                open();
            }

            message.pipeAndClose(socketChannel);
        } catch (IOException e) {
            throw new MongoSocketException("Exception sending message", address, e);
        }
    }

    public <T> MongoReplyMessage<T> sendQueryMessage(final MongoQueryMessage message, Serializer<T> serializer) {
        sendOneWayMessage(message);
        return receiveMessage(message, serializer);
    }

    public <T> MongoReplyMessage<T> sendGetMoreMessage(final MongoGetMoreMessage message, Serializer<T> serializer) {
        sendOneWayMessage(message);
        return receiveMessage(message, serializer);
    }

    public <T> MongoReplyMessage<T> receiveMessage(final MongoRequestMessage message, final Serializer<T> serializer) {
        ByteBuffer headerByteBuffer = null;
        ByteBuffer bodyByteBuffer = null;
        try {
            headerByteBuffer = pool.get(36);
            fillAndFlipBuffer(headerByteBuffer);

            final InputBuffer headerInputBuffer = new ByteBufferInput(headerByteBuffer);
            final int length = headerInputBuffer.readInt32();
            headerInputBuffer.setPosition(0);

            InputBuffer bodyInputBuffer = null;

            if (length > 36) {
                bodyByteBuffer = pool.get(length - 36);
                fillAndFlipBuffer(bodyByteBuffer);

                bodyInputBuffer = new ByteBufferInput(bodyByteBuffer);
            }

            MongoReplyHeader replyHeader = new MongoReplyHeader(headerInputBuffer);
            if (replyHeader.isCursorNotFound()) {
                throw new MongoCursorNotFoundException(address, ((MongoGetMoreMessage) message).getCursorId());
            }
            else if (replyHeader.isQueryFailure()) {
                Document errorDocument = new MongoReplyMessage<Document>(replyHeader, bodyInputBuffer,
                                                                         errorSerializer).getDocuments().get(0);
                throw new MongoQueryFailureException(address, errorDocument);
            }
            return new MongoReplyMessage<T>(replyHeader, bodyInputBuffer, serializer);
        } catch (IOException e) {
          throw new MongoSocketException("Exception receiving message", address, e);
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
            int bytesRead = socketChannel.read(buffer);
            if (bytesRead == -1) {
                throw new MongoSocketException("Prematurely reached end of stream", address);
            }
            totalBytesRead += bytesRead;
        }
        buffer.flip();
    }

    private void open() {
        try {
            socketChannel = SocketChannel.open(address.getSocketAddress());
        } catch (IOException e) {
            throw new MongoSocketException("Exception opening socket", address, e);
        }
    }

    public void close() {
        try {
            if (socketChannel != null) {
                socketChannel.close();
            }
        } catch (IOException e) {
            // ignore
        }
    }
}
