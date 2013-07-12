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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.bson.ByteBuf;


public class SocketClient {
    private SocketChannel client = null;
    private Selector selector = null;
    private InetSocketAddress address = null;

    private SSLContext sslContext;
    private SSLHandler sslHandler = null;

    private boolean initConnDone = false;
    private boolean isClosed = false;

    private final NIOSocketInputStream socketInputStream;
    private final NIOSocketOutputStream socketOutputStream;

    public SocketClient(final InetSocketAddress address) {
        this.address = address;
        socketInputStream = new NIOSocketInputStream(this);
        socketOutputStream = new NIOSocketOutputStream(this);
    }

    public SocketClient(final SocketChannel client, final Selector key) {
        this.client = client;
        initConnDone = true;
        selector = key;
        socketInputStream = new NIOSocketInputStream(this);
        socketOutputStream = new NIOSocketOutputStream(this);
    }

    public void setSSLContext(final SSLContext context) {
        this.sslContext = context;
    }

    protected void buildSSLHandler(final SSLContext context, final boolean clientmode) throws IOException {
        if (context != null && client != null) {
            setSSLContext(context);
            final SSLEngine sslEngine = sslContext.createSSLEngine(client.socket().getInetAddress().getHostName(),
                client.socket().getPort());
            sslEngine.setUseClientMode(clientmode);
            sslHandler = new SSLHandler(sslEngine, client);
            //sslHandler.doHandshake();
            //System.out.println(((!clientmode)? "Server ": "Client ") + "Handshake done");
        }
    }

    public void connect(final AsyncCompletionHandler handler) throws IOException {

        if (initConnDone) {
            throw new IOException("Socket Already connected");
        }

        client = SocketChannel.open();
        client.configureBlocking(false);
        client.connect(address);

        new Thread() {
            public void run() {
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
                                    if (sslContext != null) {
                                        // Do the SSL handshake stuff ;
                                        buildSSLHandler(sslContext, true);
                                    }
                                    client.register(selector, SelectionKey.OP_READ);
                                    initConnDone = true;
                                    handler.completed();
                                }
                            }
                            if (key.isValid() && key.isReadable()) {
                                unblockRead();
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
                    }

                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } finally {
                    try {
                        selector.close();
                        client.close();
                        socketInputStream.close();
                        socketOutputStream.close();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }

                }
            }
        }.start();
    }

    protected void unblockRead() {
        socketInputStream.notifyRead();
    }


    protected void unblockWrite() {
        socketOutputStream.notifyWrite();
    }

    protected int doWrite() throws IOException {
        if (sslHandler != null) {
            return sslHandler.doWrite(socketOutputStream.getByteBuffer());
        } else {
            //Write the non SSL bit of the transfer
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
            socketInputStream.close();
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
            buffer.flip();
        }
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
            //Write the non SSL bit of the transfer
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

    public void read(final ByteBuf byteBuf, final CompletionHandler<Integer, Void> callback) {
        try {
            callback.completed(readToBuffer(byteBuf.asNIO()), null);
        } catch (IOException e) {
            callback.failed(e, null);
        }
    }
}
