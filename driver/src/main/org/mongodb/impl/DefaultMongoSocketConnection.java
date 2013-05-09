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

import org.mongodb.MongoCredential;
import org.mongodb.ServerAddress;
import org.mongodb.io.BufferPool;
import org.mongodb.io.ChannelAwareOutputBuffer;
import org.mongodb.io.MongoSocketOpenException;
import org.mongodb.io.MongoSocketReadException;
import org.mongodb.io.MongoSocketWriteException;
import org.mongodb.pool.SimplePool;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;

// TODO: migrate all the DBPort configuration
class DefaultMongoSocketConnection extends DefaultMongoSyncConnection {
    private final SocketFactory socketFactory;
    private volatile Socket socket;

    public DefaultMongoSocketConnection(final ServerAddress address, final SimplePool<MongoSyncConnection> connectionPool,
                                        final BufferPool<ByteBuffer> bufferPool, final List<MongoCredential> credentialList,
                                        final SocketFactory socketFactory) {
        super(address, connectionPool, bufferPool, credentialList);
        this.socketFactory = socketFactory;
    }

    protected void ensureOpen() {
        try {
            if (socket == null) {
                socket = socketFactory.createSocket(getServerAddress().getSocketAddress().getAddress(), getServerAddress().getPort());
                socket.setTcpNoDelay(true);
            }
        } catch (IOException e) {
            close();
            throw new MongoSocketOpenException("Exception opening socket", getServerAddress(), e);
        }
    }

    @Override
    protected void sendOneWayMessage(final ChannelAwareOutputBuffer buffer) {
        try {
            buffer.pipeAndClose(socket);
        } catch (IOException e) {
            close();
            throw new MongoSocketWriteException("Exception sending message", getServerAddress(), e);
        }
    }

    protected void fillAndFlipBuffer(final ByteBuffer buffer) {
        try {
            if (buffer.isDirect()) {
                throw new IllegalArgumentException("Only works with non-direct buffers, so something must be misconfigured");
            }
            int totalBytesRead = 0;
            byte[] bytes = buffer.array();
            while (totalBytesRead < buffer.limit()) {
                final int bytesRead = socket.getInputStream().read(bytes, totalBytesRead, buffer.limit() - totalBytesRead);
                if (bytesRead == -1) {
                    throw new MongoSocketReadException("Prematurely reached end of stream", getServerAddress());
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
                super.close();
            }
        } catch (IOException e) { // NOPMD
            // ignoreBSON
        }
    }
}