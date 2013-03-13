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

package org.mongodb.io;

import org.mongodb.Document;
import org.mongodb.ServerAddress;
import org.mongodb.protocol.MongoRequestMessage;
import org.mongodb.serialization.Serializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

// TODO: migrate all the DBPort configuration
class MongoSocketChannelGateway extends MongoGateway {
    private volatile SocketChannel socketChannel;

    public MongoSocketChannelGateway(final ServerAddress address, final BufferPool<ByteBuffer> pool,
                                     final Serializer<Document> errorSerializer) {
        super(address, pool, errorSerializer);
    }

    protected void ensureOpen() {
        try {
            if (socketChannel == null) {
                socketChannel = SocketChannel.open(getAddress().getSocketAddress());
                socketChannel.socket().setTcpNoDelay(true);
            }
        } catch (IOException e) {
            throw new MongoSocketOpenException("Exception opening socket", getAddress(), e);
        }
    }

    protected void sendOneWayMessage(final MongoRequestMessage message) {
        try {
            message.pipeAndClose(socketChannel);
        } catch (IOException e) {
            throw new MongoSocketWriteException("Exception sending message", getAddress(), e);
        }
    }


    protected void fillAndFlipBuffer(final ByteBuffer buffer) {
        try {
            int totalBytesRead = 0;
            while (totalBytesRead < buffer.limit()) {
                final int bytesRead = socketChannel.read(buffer);
                if (bytesRead == -1) {
                    throw new MongoSocketReadException("Prematurely reached end of stream", getAddress());
                }
                totalBytesRead += bytesRead;
            }
            buffer.flip();
        } catch (IOException e) {
            handleIOException(e);
        }
    }

    //CHECKSTYLE:OFF
    public void close() {
        try {
            if (socketChannel != null) {
                socketChannel.close();
                socketChannel = null;
            }
        } catch (IOException e) {  //NOPMD
            // ignore
        }
    }
    //CHECKSTYLE:ON
}
