/**
 * Copyright 2013 10gen.com
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
 *  Contributions:
 *      Gihan Munasinghe    ayeshka@gmail.com
 */


package org.mongodb.connection.impl;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;

import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.MongoSocketOpenException;
import org.mongodb.connection.MongoSocketReadException;
import org.mongodb.connection.ServerAddress;


/**
 * Heavily modified version of https://github.com/buksy/java-nio-socket/blob/master/src/org/nio/socket/SocketClient.java
 */
public class SocketClient {
    private SocketChannel client = null;
    private Selector selector = null;
    private ServerAddress address = null;
    private BufferProvider bufferProvider;
    private final BlockingQueue<AsyncCompletionHandler> callbacks = new ArrayBlockingQueue<AsyncCompletionHandler>(1);

    private SSLHandler sslHandler = null;

    private boolean initConnDone = false;
    private boolean isClosed = false;

    private final NIOSocketOutputStream socketOutputStream;

    public SocketClient(final ServerAddress address, final BufferProvider bufferProvider) throws SSLException {
        this.address = address;
        this.bufferProvider = bufferProvider;
        socketOutputStream = new NIOSocketOutputStream(this);
    }

    protected void buildSSLHandler() {
        if (sslHandler == null) {
            sslHandler = new SSLHandler(this, bufferProvider, client);
        }

    }

    public void connect(final AsyncCompletionHandler handler) throws IOException {
        if (initConnDone) {
            throw new IOException("Socket Already connected");
        }

        client = SocketChannel.open();
        client.configureBlocking(false);
        client.connect(address.getSocketAddress());

        new Thread(new NIOListener(handler)).start();
    }

    protected void readChannel() {
        final AsyncCompletionHandler handler = callbacks.poll();
        if (handler != null) {
            handler.completed();
        }
    }


    protected void unblockWrite() {
        socketOutputStream.notifyWrite();
    }

    protected int doWrite() throws IOException {
        if (sslHandler != null) {
            return sslHandler.doWrite(socketOutputStream.getByteBuffer());
        } else {
            final ByteBuffer buff = socketOutputStream.getByteBuffer();
            final int out = buff.remaining();
            while (buff.hasRemaining()) {
                final int x = client.write(buff);
                if (x < 0) {
                    return x;
                }
            }
            return out;
        }
    }


    public boolean isConnected() {
        return (initConnDone && client != null && client.isConnected());
    }

    public void close() throws IOException {
        if (!isClosed) {
            isClosed = true;
            if (sslHandler != null) {
                sslHandler.stop();
            }
            client.close();
            socketOutputStream.close();
            initConnDone = false;
            if (selector != null) {
                selector.wakeup();
            }
        }
    }

    protected void triggerWrite() throws IOException {
        if (isConnected()) {
            try {
                client.register(selector, SelectionKey.OP_WRITE | SelectionKey.OP_READ, this);
                selector.wakeup();
            } catch (ClosedChannelException e) {
                throw new IOException("Connection Closed ");
            }
        }
    }


    protected int readToBuffer(final ByteBuffer buffer) throws IOException {
        final int out;
        if (sslHandler != null) {
            out = sslHandler.doRead(buffer);
        } else {
            out = client.read(buffer);
        }
        buffer.flip();
        if (out < 0) {
            close();
        } else {
            client.register(selector, SelectionKey.OP_READ, this);
        }
        return out;
    }

    public int write(final ByteBuffer byteBuffer) throws IOException {
        if (sslHandler != null) {
            return sslHandler.doWrite(byteBuffer);
        } else {
            final int out = byteBuffer.remaining();
            while (byteBuffer.hasRemaining()) {
                final int x = client.write(byteBuffer);
                if (x < 0) {
                    return x;
                }
            }
            return out;
        }

    }

    public void read(final ByteBuffer byteBuf, final CompletionHandler<Integer, Void> callback) {
        try {
            callbacks.offer(new AsyncCompletionHandler() {
                @Override
                public void completed() {
                    try {
                        final int read = readToBuffer(byteBuf);
                        callback.completed(read, null);
                    } catch (IOException e) {
                        throw new MongoSocketReadException(e.getMessage(), address, e);
                    }
                }

                @Override
                public void failed(final Throwable t) {
                    callback.failed(t, null);
                }
            }, 1, TimeUnit.SECONDS);
            client.register(selector, SelectionKey.OP_WRITE | SelectionKey.OP_READ, this);
            selector.wakeup();
        } catch (InterruptedException e) {
            throw new MongoSocketReadException(e.getMessage(), address, e);
        } catch (IOException e) {
            throw new MongoSocketReadException(e.getMessage(), address, e);
        }
    }

    public ServerAddress getServerAddress() {
        return address;
    }

    private class NIOListener implements Runnable {
        private final AsyncCompletionHandler handler;

        public NIOListener(final AsyncCompletionHandler handler) {
            this.handler = handler;
        }

        @Override
        public void run() {
            try {
                try {
                    selector = Selector.open();
                    client.register(selector, SelectionKey.OP_CONNECT);

                    while (!isClosed) {
                        if (selector.select() == 0) {
                            continue;
                        }
                        final Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                        while (it.hasNext()) {
                            final SelectionKey key = it.next();
                            it.remove();
                            if (!key.isValid()) {
                                continue;
                            }
                            if (key.isValid() && key.isConnectable()) {
                                if (client.finishConnect()) {
                                    buildSSLHandler();
                                    client.register(selector, SelectionKey.OP_READ);
                                    initConnDone = true;
                                    handler.completed();
                                }
                            }
                            if (key.isValid() && key.isReadable()) {
                                readChannel();
                                if (client.isOpen()) {
                                    client.register(selector, SelectionKey.OP_READ);
                                }
                            }
                            if (key.isValid() && key.isWritable()) {
                                unblockWrite();
                                if (client.isOpen()) {
                                    client.register(selector, SelectionKey.OP_READ);
                                }
                            }
                        }
                        readChannel();
                    }
                } finally {
                    selector.close();
                    client.close();
                    socketOutputStream.close();
                }
            } catch (ClosedChannelException e) {
                throw new MongoSocketOpenException(e.getMessage(), getServerAddress(), e);
            } catch (IOException e) {
                throw new MongoSocketOpenException(e.getMessage(), getServerAddress(), e);
            }

        }
    }
}
