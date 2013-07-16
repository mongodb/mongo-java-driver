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


import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import org.bson.ByteBuf;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.MongoSocketOpenException;
import org.mongodb.connection.MongoSocketReadException;
import org.mongodb.connection.MongoSocketWriteException;


public class SSLHandler {

    private SSLEngine sslEngine;
    private SocketChannel channel;
    private ByteBuffer netSendBuffer = null;

    private ByteBuffer netRecvBuffer = null;

    private int remaining = 0;

    private boolean handShakeDone = false;

    private final SocketClient socketClient;
    private final BufferProvider bufferProvider;
    private static final SSLContext SSL_CONTEXT;

    static {
        try {
            SSL_CONTEXT = SSLContext.getInstance("TLS");
            final KeyStore ks = KeyStore.getInstance("JKS");
            final KeyStore ts = KeyStore.getInstance("JKS");

            final char[] passphrase = System.getProperty("javax.net.ssl.trustStorePassword").toCharArray();

            final String keyStoreFile = System.getProperty("javax.net.ssl.trustStore");
            ks.load(new FileInputStream(keyStoreFile), passphrase);
            ts.load(new FileInputStream(keyStoreFile), passphrase);

            final KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
            kmf.init(ks, passphrase);

            final TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
            tmf.init(ts);

            SSL_CONTEXT.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        } catch (GeneralSecurityException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public SSLHandler(final SocketClient socketClient, final BufferProvider bufferProvider, final SocketChannel channel) {
        this.socketClient = socketClient;
        this.bufferProvider = bufferProvider;
        this.channel = channel;

        sslEngine = SSL_CONTEXT.createSSLEngine(socketClient.getServerAddress().getHost(), socketClient.getServerAddress().getPort());
        sslEngine.setUseClientMode(true);

        this.netSendBuffer = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());
        this.netRecvBuffer = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());

        try {
            sslEngine.beginHandshake();
        } catch (SSLException e) {
            throw new MongoSocketOpenException(e.getMessage(), socketClient.getServerAddress(), e);
        }
    }

    public void stop() throws IOException {
        sslEngine.closeOutbound();
    }

    protected int doWrite(final ByteBuffer buff) throws IOException {
        doHandshake();
        final int out = buff.remaining();
        while (buff.remaining() > 0) {
            if (wrapAndWrite(buff) < 0) {
                return -1;
            }
        }
        return out;
    }

    protected ByteBuf doRead(final ByteBuf buff) throws IOException {

        final ByteBuf byteBuf = readAndUnwrap(buff);
        if (byteBuf != null && byteBuf.position() >= 0) {
            doHandshake();
        } else {
            return null;
        }

        return byteBuf;
    }

    private int wrapAndWrite(final ByteBuffer buff) {
        try {
            Status status;
            netSendBuffer.clear();
            do {
                status = sslEngine.wrap(buff, netSendBuffer).getStatus();
                if (status == Status.BUFFER_OVERFLOW) {
                    // There data in the net buffer therefore need to send out the data
                    flush();
                }
            } while (status == Status.BUFFER_OVERFLOW);
            if (status == Status.CLOSED) {
                throw new MongoSocketWriteException("SSLEngine Closed", socketClient.getServerAddress());
            }
            return flush();
        } catch (IOException e) {
            throw new MongoSocketWriteException(e.getMessage(), socketClient.getServerAddress(), e);
        }
    }

    private int flush() throws IOException {
        netSendBuffer.flip();
        int count = 0;
        while (netSendBuffer.hasRemaining() && count != -1) {
            final int x = channel.write(netSendBuffer);
            if (x >= 0) {
                count += x;
            } else {
                count = x;
            }
        }
        netSendBuffer.compact();
        return count;
    }

    private ByteBuf readAndUnwrap(final ByteBuf provided) {
        ByteBuf buff = provided;
        Status status;
        if (!channel.isOpen()) {
            throw new MongoSocketReadException("Channel is closed", socketClient.getServerAddress());
        }
        boolean needRead;

        if (remaining > 0) {
            netRecvBuffer.compact();
            netRecvBuffer.flip();
            needRead = false;
        } else {
            netRecvBuffer.clear();
            needRead = true;
        }

        int x = 0;
        try {
            do {
                if (needRead) {
                    x = channel.read(netRecvBuffer);
                    if (x == -1) {
                        return buff;
                    }
                    netRecvBuffer.flip();
                }
                if (buff.limit() < netRecvBuffer.limit()) {
                    buff.close();
                    buff = bufferProvider.get(netRecvBuffer.limit());
                }
                status = sslEngine.unwrap(netRecvBuffer, buff.asNIO()).getStatus();
                if (x == 0 && netRecvBuffer.position() == 0) {
                    netRecvBuffer.clear();
                }
                if (x == 0 && handShakeDone) {
                    return buff;
                }
                if (status == Status.BUFFER_UNDERFLOW) {
                    needRead = true;
                } else if (status == Status.BUFFER_OVERFLOW) {
                    break;
                } else if (status == Status.CLOSED) {
                    buff.flip();
                    return null;
                }
            } while (status != Status.OK);
        } catch (IOException e) {
            throw new MongoSocketReadException(e.getMessage(), socketClient.getServerAddress(), e);
        }

        remaining = netRecvBuffer.remaining();
        return buff;
    }

    private void doHandshake() {

        handShakeDone = false;
        ByteBuf tmpBuff = bufferProvider.get(sslEngine.getSession().getApplicationBufferSize());
        HandshakeStatus status = sslEngine.getHandshakeStatus();
        while (status != HandshakeStatus.FINISHED && status != HandshakeStatus.NOT_HANDSHAKING) {
            switch (status) {
                case NEED_TASK:

                    final Executor exec = Executors.newSingleThreadExecutor();
                    Runnable task;

                    while ((task = sslEngine.getDelegatedTask()) != null) {
                        exec.execute(task);
                    }
                    break;
                case NEED_WRAP:
                    tmpBuff.clear();
                    tmpBuff.flip();
                    if (wrapAndWrite(tmpBuff.asNIO()) < 0) {
                        throw new MongoSocketOpenException("SSLHandshake failed", socketClient.getServerAddress());
                    }
                    break;

                case NEED_UNWRAP:
                    tmpBuff.clear();
                    tmpBuff = readAndUnwrap(tmpBuff);
                    if (tmpBuff == null || tmpBuff.position() != 0) {
                        throw new MongoSocketOpenException("SSLHandshake failed", socketClient.getServerAddress());
                    }
                    break;
                default:
            }
            status = sslEngine.getHandshakeStatus();
        }
        handShakeDone = true;
    }
}
