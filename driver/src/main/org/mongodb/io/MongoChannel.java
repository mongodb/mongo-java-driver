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
import org.bson.io.OutputBuffer;
import org.bson.types.Document;
import org.bson.util.BufferPool;
import org.mongodb.MongoCursorNotFoundException;
import org.mongodb.MongoException;
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
    private SocketChannel socketChannel;
    private final BufferPool<ByteBuffer> pool;
    private final Serializer<Document> errorSerializer;

    public MongoChannel(final ServerAddress address, final BufferPool<ByteBuffer> pool, Serializer<Document> errorSerializer) {
        this.address = address;
        this.pool = pool;
        this.errorSerializer = errorSerializer;
    }

    public ServerAddress getAddress() {
        return address;
    }

    public void sendOneWayMessage(final MongoRequestMessage message) throws IOException {
        if (socketChannel == null) {
            open();
        }

        final OutputBuffer buffer = message.getBuffer();
        buffer.pipe(socketChannel);
        message.done();
    }

    public <T> MongoReplyMessage<T> sendQueryMessage(final MongoQueryMessage message, Serializer<T> serializer) throws IOException {
        sendOneWayMessage(message);
        return receiveMessage(message, serializer);
    }

    public <T> MongoReplyMessage<T> sendGetMoreMessage(final MongoGetMoreMessage message, Serializer<T> serializer) throws IOException {
        sendOneWayMessage(message);
        return receiveMessage(message, serializer);
    }

    public <T> MongoReplyMessage<T> receiveMessage(final MongoRequestMessage message, final Serializer<T> serializer) throws IOException {
        ByteBuffer headerByteBuffer = null;
        ByteBuffer bodyByteBuffer = null;
        try {
            headerByteBuffer = pool.get(36);
            int bytesRead = socketChannel.read(headerByteBuffer);
            if (bytesRead < headerByteBuffer.limit()) {
                throw new MongoException("Unable to read message header: " + bytesRead);
            }

            headerByteBuffer.flip();
            final InputBuffer headerInputBuffer = new ByteBufferInput(headerByteBuffer);
            final int length = headerInputBuffer.readInt32();
            headerInputBuffer.setPosition(0);

            InputBuffer bodyInputBuffer = null;

            if (length > 36) {
                bodyByteBuffer = pool.get(length - 36);
                bytesRead = 0;
                while (bytesRead < bodyByteBuffer.limit()) {
                    bytesRead += socketChannel.read(bodyByteBuffer);
                }

                bodyByteBuffer.flip();
                bodyInputBuffer = new ByteBufferInput(bodyByteBuffer);
            }

            MongoReplyHeader replyHeader = new MongoReplyHeader(headerInputBuffer);
            if (replyHeader.isCursorNotFound()) {
                throw new MongoCursorNotFoundException(address, ((MongoGetMoreMessage) message).getCursorId());
            }
            else if (replyHeader.isQueryFailure()) {
                Document errorDocument = new MongoReplyMessage<Document>(replyHeader, bodyInputBuffer, errorSerializer).getDocuments().get(0);
                throw new MongoQueryFailureException(address, errorDocument);
            }
            return new MongoReplyMessage<T>(replyHeader, bodyInputBuffer, serializer);
        } finally {
            if (headerByteBuffer != null) {
                pool.done(headerByteBuffer);
            }
            if (bodyByteBuffer != null) {
                pool.done(bodyByteBuffer);
            }
        }
    }

    private void open() throws IOException {
        socketChannel = SocketChannel.open(address.getSocketAddress());
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
