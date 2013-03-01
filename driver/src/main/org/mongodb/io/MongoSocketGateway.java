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

import org.mongodb.Document;
import org.mongodb.ServerAddress;
import org.mongodb.protocol.MongoRequestMessage;
import org.mongodb.serialization.Serializer;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

// TODO: migrate all the DBPort configuration
class MongoSocketGateway extends MongoGateway {
    private final SocketFactory socketFactory;
    private volatile Socket socket;

    public MongoSocketGateway(final ServerAddress address, final BufferPool<ByteBuffer> pool,
                              final Serializer<Document> errorSerializer, final SocketFactory socketFactory) {
        super(address, pool, errorSerializer);
        this.socketFactory = socketFactory;
    }

    protected void ensureOpen() {
        try {
            if (socket == null) {
                socket = socketFactory.createSocket(getAddress().getSocketAddress().getAddress(), getAddress().getPort());
            }
        } catch (IOException e) {
            throw new MongoSocketOpenException("Exception opening socket", getAddress(), e);
        }
    }

    protected void sendOneWayMessage(final MongoRequestMessage message) {
        try {
            message.pipeAndClose(socket);
        } catch (IOException e) {
            throw new MongoSocketWriteException("Exception sending message", getAddress(), e);
        }
    }

    protected void fillAndFlipBuffer(final ByteBuffer buffer)  {
        try {
            if (buffer.isDirect()) {
                throw new IllegalArgumentException("Only works with non-direct buffers, so something must be misconfigured");
            }
            int totalBytesRead = 0;
            byte[] bytes = buffer.array();
            while (totalBytesRead < buffer.limit()) {
                final int bytesRead = socket.getInputStream().read(bytes, totalBytesRead, buffer.limit() - totalBytesRead);
                if (bytesRead == -1) {
                    throw new MongoSocketReadException("Prematurely reached end of stream", getAddress());
                }
                totalBytesRead += bytesRead;
            }
        } catch (IOException e) {
            handleIOException(e);
        }
    }

    //CHECKSTYLE:OFF
    public void close() {
        try {
            if (socket != null) {
                socket.close();
                socket = null;
            }
        } catch (IOException e) {
            // ignore
        }
    }
}