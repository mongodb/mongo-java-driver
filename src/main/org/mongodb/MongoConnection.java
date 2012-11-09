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

package org.mongodb;

import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import org.bson.io.ByteBufferInput;
import org.bson.io.InputBuffer;
import org.bson.io.OutputBuffer;
import org.bson.util.BufferPool;
import org.mongodb.protocol.MongoReplyMessage;
import org.mongodb.protocol.MongoRequestMessage;
import org.mongodb.serialization.Serializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

// TODO: port over all the DBPort configuration

/**
 *
 */
public class MongoConnection {
    private final ServerAddress address;
    private SocketChannel socketChannel;
    private final BufferPool<ByteBuffer> pool;

    public MongoConnection(final ServerAddress address, BufferPool<ByteBuffer> pool) {
        this.address = address;
        this.pool = pool;
    }

    // TODO: piggy back getLastError onto same buffer
    public void sendMessage(MongoRequestMessage message) throws IOException {
        if (socketChannel == null) {
            open();
        }

        OutputBuffer buffer = message.getBuffer();
        buffer.pipe(socketChannel);
        message.done();
    }

    public <T> MongoReplyMessage<T> receiveMessage(Serializer serializer, Class clazz) throws IOException {
        ByteBuffer headerByteBuffer = pool.get(36);
        int bytesRead = socketChannel.read(headerByteBuffer);
        if (bytesRead < headerByteBuffer.limit()) {
            throw new MongoException("Unable to read message header: " + bytesRead);
        }

        headerByteBuffer.flip();
        InputBuffer headerInputBuffer = new ByteBufferInput(headerByteBuffer);
        int length = headerInputBuffer.readInt32();
        headerInputBuffer.setPosition(0);

        ByteBuffer bodyByteBuffer = null;
        InputBuffer bodyInputBuffer = null;

        if (length > 36) {
            bodyByteBuffer = pool.get(length - 36);

            while (bytesRead < bodyByteBuffer.limit()) {
               bytesRead += socketChannel.read(bodyByteBuffer);
            }

            bodyByteBuffer.flip();
            bodyInputBuffer = new ByteBufferInput(bodyByteBuffer);
        }

        MongoReplyMessage<T> retVal = new MongoReplyMessage<T>(headerInputBuffer, bodyInputBuffer, serializer, clazz);

        pool.done(headerByteBuffer);
        if (bodyByteBuffer != null) {
            pool.done(bodyByteBuffer);
        }

        return retVal;
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
