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

package org.mongodb.connection.impl;

import org.bson.ByteBuf;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.MongoSocketOpenException;
import org.mongodb.connection.MongoSocketReadException;
import org.mongodb.connection.ServerAddress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

// TODO: migrate all the DBPort configuration
class DefaultSocketChannelConnection extends DefaultConnection {
    private final SocketChannel socketChannel;

    public DefaultSocketChannelConnection(final ServerAddress address, final ConnectionSettings settings,
                                          final BufferProvider bufferProvider) {
        super(address, settings, bufferProvider);
        try {
            socketChannel = SocketChannel.open();
            initializeSocket(socketChannel.socket());
        } catch (IOException e) {
            close();
            throw new MongoSocketOpenException("Exception opening socket", getServerAddress(), e);
        }
    }

    @Override
    protected void write(final List<ByteBuf> buffers) throws IOException {
        int totalSize = 0;
        ByteBuffer[] byteBufferArray = new ByteBuffer[buffers.size()];
        for (int i = 0; i < buffers.size(); i++) {
            byteBufferArray[i] = buffers.get(i).asNIO();
            totalSize += byteBufferArray[i].limit();
        }

        long bytesRead = 0;
        while (bytesRead < totalSize) {
            bytesRead += socketChannel.write(byteBufferArray);
        }
    }

    protected void read(final ByteBuf buffer) throws IOException {
        int totalBytesRead = 0;
        while (totalBytesRead < buffer.limit()) {
            final int bytesRead = socketChannel.read(buffer.asNIO());
            if (bytesRead == -1) {
                throw new MongoSocketReadException("Prematurely reached end of stream", getServerAddress());
            }
            totalBytesRead += bytesRead;
        }
        buffer.flip();
    }

    public void close() {
        try {
            if (socketChannel != null) {
                socketChannel.close();
            }
            super.close();
        } catch (IOException e) {
            // ignore
        }
    }
}
