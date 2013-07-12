/**
 * Copyright [2012] [Gihan Munasinghe ayeshka@gmail.com ]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */


package org.mongodb.connection.impl;


import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class NIOSocketInputStream extends InputStream {

    private ByteBuffer streamBuffer;
    private ByteBuffer channelBuffer;

    private SocketClient readClient;
    private boolean stremClosed = false;
    private Boolean isReadWait = false;
    private Lock readLock = new ReentrantLock();

    public NIOSocketInputStream(final SocketClient client) {
        readClient = client;
        streamBuffer = ByteBuffer.allocate(1024);
        streamBuffer.flip();
        channelBuffer = ByteBuffer.allocate(1024);
    }

    @Override
    public int read() throws IOException {
        final byte[] b = new byte[1];
        final int count = read(b, 0, b.length);
        return count == -1 ? -1 : (0xFF & b[0]);

    }

    @Override
    public int read(final byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {

        try {
            readLock.lock();
            int readSize = 0;
            int offset = off;
            while (true) {

                if (stremClosed) {
                    throw new IOException("Read stream closed");
                }

                if (streamBuffer.remaining() > 0) {
                    final int toRead = Math.min(len, streamBuffer.remaining());
                    streamBuffer.get(b, offset, toRead);
                    readSize += toRead;
                    offset = readSize;
                    if (readSize == len) {
                        break;
                    }
                } else {
                    final int available = available();
                    if (available > 0) {
                        continue;
                    } else if (available == 0) {
                        // Block the caller
                        try {
                            if (readSize == 0) {
                                synchronized (isReadWait) {
                                    isReadWait = true;
                                }
                                synchronized (this) {
                                    while (isReadWait()) {
                                        wait();
                                    }
                                }
                            } else {
                                break;
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    } else {
                        return -1;
                    }
                }

            }
            return readSize;
        } finally {
            readLock.unlock();
        }


    }

    @Override
    public synchronized int available() throws IOException {
        while (true) {

            if (stremClosed) {
                throw new IOException("Read stream closed");
            }

            int available = streamBuffer.remaining();
            if (available == 0) {
                streamBuffer.rewind();
                streamBuffer.limit(streamBuffer.capacity());
                // Read it from the stream add it to the stream buffer
                channelBuffer.rewind();
                channelBuffer.limit(channelBuffer.capacity());
                if (readClient.readToBuffer(channelBuffer) < 0) {
                    close();
                    return -1;
                }
                channelBuffer.flip();
                streamBuffer.put(channelBuffer);
                streamBuffer.flip();
                available = streamBuffer.remaining();
            }

            return available;
        }
    }

    @Override
    public void close() throws IOException {
        if (!stremClosed) {
            stremClosed = true;
            notifyRead();
        }
    }

    protected boolean isReadWait() {
        return isReadWait;
    }

    protected void notifyRead() {
        synchronized (isReadWait) {
            isReadWait = false;
        }
        synchronized (this) {
            this.notifyAll();
        }

    }

    protected void lock() {
        readLock.lock();
    }

    protected void unlock() {
        readLock.unlock();
    }

    protected boolean tryLock() {
        return readLock.tryLock();
    }
}
