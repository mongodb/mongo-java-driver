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

import org.bson.ByteBuf;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.Socket;
import java.util.List;

class SocketStream implements Stream {
    private final Socket socket;
    private final ServerAddress address;
    private final SocketSettings settings;
    private volatile boolean isClosed;

    public SocketStream(final ServerAddress address, final SocketSettings settings, final SocketFactory socketFactory)  {
        this.address = address;
        this.settings = settings;
        try {
            socket = socketFactory.createSocket();
            SocketStreamHelper.initialize(socket, address, settings);
        } catch (IOException e) {
            close();
            throw new MongoSocketOpenException("Exception opening socket", getAddress(), e);
        }
    }

    @Override
    public void write(final List<ByteBuf> buffers) throws IOException {
        for (ByteBuf cur : buffers) {
            socket.getOutputStream().write(cur.array(), 0, cur.limit());
        }
    }

    public void read(final ByteBuf buffer) throws IOException {
        int totalBytesRead = 0;
        byte[] bytes = buffer.array();
        while (totalBytesRead < buffer.limit()) {
            final int bytesRead = socket.getInputStream().read(bytes, totalBytesRead, buffer.limit() - totalBytesRead);
            if (bytesRead == -1) {
                throw new MongoSocketReadException("Prematurely reached end of stream", getAddress());
            }
            totalBytesRead += bytesRead;
        }
    }

    @Override
    public void writeAsync(final List<ByteBuf> buffers, final AsyncCompletionHandler handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readAsync(final ByteBuf buffer, final AsyncCompletionHandler handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ServerAddress getAddress() {
        return address;
    }

    /**
     * Get the settings for this socket.
     *
     * @return the settings
     */
    SocketSettings getSettings() {
        return settings;
    }

    @Override
    public void close() {
        try {
            isClosed = true;
            if (socket != null) {
                socket.close();
            }
        } catch (IOException e) {
            // ignore
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }
}
