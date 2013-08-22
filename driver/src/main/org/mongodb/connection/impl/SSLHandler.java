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


import org.mongodb.connection.MongoSocketOpenException;
import org.mongodb.connection.MongoSocketReadException;
import org.mongodb.connection.MongoSocketWriteException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;


public class SSLHandler {

    private SSLEngine sslEngine;
    private SocketChannel channel;

    private ByteBuffer sendBuffer = null;
    private ByteBuffer receiveBuffer = null;
    private ByteBuffer unwrappedBuffer = null;

    private int remaining = 0;

    private boolean handShakeDone = false;

    private final SocketClient socketClient;
    private static final SSLContext SSL_CONTEXT;

    static {
        try {
            SSL_CONTEXT = SSLContext.getInstance("TLS");
            final KeyStore ks = KeyStore.getInstance(System.getProperty("javax.net.ssl.keyStoreType", "JKS"));
            final KeyStore ts = KeyStore.getInstance(System.getProperty("javax.net.ssl.trustStoreType", "JKS"));

            final char[] ksPassPhrase = System.getProperty("javax.net.ssl.keyStorePassword").toCharArray();
            final char[] tsPassPhrase = System.getProperty("javax.net.ssl.trustStorePassword").toCharArray();

            load(ks, ksPassPhrase, System.getProperty("javax.net.ssl.keyStore"));
            load(ts, tsPassPhrase, System.getProperty("javax.net.ssl.trustStore"));

            final KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
            kmf.init(ks, ksPassPhrase);

            final TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
            tmf.init(ts);

            SSL_CONTEXT.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        } catch (GeneralSecurityException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static void load(final KeyStore ks, final char[] passphrase, final String keyStoreFile)
        throws IOException, NoSuchAlgorithmException, CertificateException {
        FileInputStream stream = new FileInputStream(keyStoreFile);
        try {
            ks.load(stream, passphrase);
        } finally {
            stream.close();
        }
    }

    public SSLHandler(final SocketClient socketClient, final SocketChannel channel) {
        this.socketClient = socketClient;
        this.channel = channel;

        sslEngine = SSL_CONTEXT.createSSLEngine(socketClient.getServerAddress().getHost(), socketClient.getServerAddress().getPort());
        sslEngine.setUseClientMode(true);

        this.sendBuffer = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());
        this.receiveBuffer = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize()).compact();
        this.unwrappedBuffer = ByteBuffer.allocate(sslEngine.getSession().getApplicationBufferSize()).compact();

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

    protected int doRead(final ByteBuffer buff) throws IOException {

        if (readInto(buff) >= 0) {
            doHandshake();
        } else {
            return -1;
        }

        return buff.position();
    }

    private int readInto(final ByteBuffer buffer) {
        while (remaining(unwrappedBuffer) < buffer.limit()) {
            unwrappedBuffer.compact();
            unwrap(unwrappedBuffer);
            unwrappedBuffer.flip();
        }
        int read = 0;
        try {
            final byte[] array = new byte[buffer.limit()];
            unwrappedBuffer.get(array);
            buffer.put(array);
            read = array.length;
        } catch (BufferUnderflowException e) {
            e.printStackTrace();
        }
        return read;
    }

    private int wrapAndWrite(final ByteBuffer buff) {
        try {
            Status status;
            sendBuffer.clear();
            do {
                status = sslEngine.wrap(buff, sendBuffer).getStatus();
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
        sendBuffer.flip();
        int count = 0;
        while (sendBuffer.hasRemaining()) {
            final int x = channel.write(sendBuffer);
            if (x >= 0) {
                count += x;
            } else {
                count = x;
            }
        }
        sendBuffer.compact();
        return count;
    }

    private int unwrap(final ByteBuffer buffer) {
        if (!channel.isOpen()) {
            throw new MongoSocketReadException("Channel is closed", socketClient.getServerAddress());
        }
        boolean needRead;

        if (remaining > 0) {
            receiveBuffer.compact();
            receiveBuffer.flip();
            needRead = false;
        } else {
            receiveBuffer.clear();
            needRead = true;
        }

        int x = 0;
        try {
            Status status;
            do {
                if (needRead) {

                    x = channel.read(receiveBuffer);
                    if (x == -1) {
                        return -1;
                    }
                    receiveBuffer.flip();
                }
                status = sslEngine.unwrap(receiveBuffer, buffer).getStatus();
                if (x == 0 && receiveBuffer.position() == 0) {
                    receiveBuffer.clear();
                }
                if (x == 0 && handShakeDone) {
                    return 0;
                }
                if (status == Status.BUFFER_UNDERFLOW) {
                    needRead = true;
                } else if (status == Status.BUFFER_OVERFLOW) {
                    throw new BufferOverflowException();
                } else if (status == Status.CLOSED) {
                    buffer.flip();
                    return -1;
                }
            } while (status != Status.OK);
        } catch (IOException e) {
            throw new MongoSocketReadException(e.getMessage(), socketClient.getServerAddress(), e);
        }

        remaining = receiveBuffer.remaining();
        return buffer.position();
    }

    private int remaining(final ByteBuffer buffer) {
        return buffer.limit() - buffer.position();
    }

    private void doHandshake() {

        handShakeDone = false;
        final ByteBuffer tmpBuff = ByteBuffer.allocate(sslEngine.getSession().getApplicationBufferSize());
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
                    if (wrapAndWrite(tmpBuff) < 0) {
                        throw new MongoSocketOpenException("SSLHandshake failed", socketClient.getServerAddress());
                    }
                    break;

                case NEED_UNWRAP:
                    tmpBuff.clear();
                    if (unwrap(tmpBuff) < 0) {
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
