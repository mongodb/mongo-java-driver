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

package org.mongodb.connection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

// TODO: migrate all the DBPort configuration
class DefaultSocketChannelConnection extends DefaultConnection {
    private volatile SocketChannel socketChannel;

    public DefaultSocketChannelConnection(final ServerAddress address, final DefaultConnectionSettings settings,
                                          final BufferPool<ByteBuffer> bufferPool) {
        super(address, settings, bufferPool);
    }

    protected void ensureOpen() {
        try {
            if (socketChannel == null) {
                socketChannel = SocketChannel.open();
                initializeSocket(socketChannel.socket());
            }
        } catch (IOException e) {
            close();
            throw new MongoSocketOpenException("Exception opening socket", getServerAddress(), e);
        }
    }

    @Override
    protected void sendOneWayMessage(final ChannelAwareOutputBuffer buffer) throws IOException {
        buffer.pipeAndClose(socketChannel);
    }

    protected void fillAndFlipBuffer(final ByteBuffer buffer) throws IOException {
        int totalBytesRead = 0;
        while (totalBytesRead < buffer.limit()) {
            final int bytesRead = socketChannel.read(buffer);
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
                socketChannel = null;
                super.close();
            }
        } catch (IOException e) {  //NOPMD
            // ignore
        }
    }
}
