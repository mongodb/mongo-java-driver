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

import org.mongodb.connection.BufferProvider;


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

    public SSLHandler(final SocketClient socketClient, final BufferProvider bufferProvider, final SocketChannel channel)
        throws SSLException {
        this.socketClient = socketClient;
        this.bufferProvider = bufferProvider;
        this.channel = channel;

        sslEngine = SSL_CONTEXT.createSSLEngine(socketClient.getServerAddress().getHost(), socketClient.getServerAddress().getPort());
        sslEngine.setUseClientMode(true);

        this.netSendBuffer = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());
        this.netRecvBuffer = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());

        sslEngine.beginHandshake();
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

        assert buff.position() == 0;
        if (readAndUnwrap(buff) >= 0) {
            doHandshake();
        } else {
            return -1;
        }
        return buff.position();

    }

    private int wrapAndWrite(final ByteBuffer buff) throws IOException {

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
            throw new IOException("SSLEngine Closed");

        }
        return flush();

    }

    private int flush() throws IOException {
        netSendBuffer.flip();
        int count = 0;
        while (netSendBuffer.hasRemaining()) {
            final int x = channel.write(netSendBuffer);
            if (x >= 0) {
                count += x;
            } else {
                count = x;
                break;
            }
        }
        netSendBuffer.compact();
        return count;
    }

    private int readAndUnwrap(final ByteBuffer buff) throws IOException {
           Status status = Status.OK;
           if (!channel.isOpen()) {
               return -1;
               //throw new IOException ("Engine is closed");
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
           do {
               if (needRead) {

                   x = channel.read(netRecvBuffer);
                   if (x == -1) {
                       return x;
                       //throw new IOException ("connection closed for reading");
                   }
                   netRecvBuffer.flip();
               }
               status = sslEngine.unwrap(netRecvBuffer, buff).getStatus();
               if (x == 0 && netRecvBuffer.position() == 0) {
                   netRecvBuffer.clear();
               }
               if (x == 0 && handShakeDone) {
                   return 0;
               }
               //status = r.result.getStatus();
               if (status == Status.BUFFER_UNDERFLOW) {
                   // Not enogh data read from the channel need to read more from the socket
                   needRead = true;
               } else if (status == Status.BUFFER_OVERFLOW) {
                   // Buffer over flow, the caller does not have enough buffer space in the buff
                   // re do the call after freeing the current data in the buffer

                   break;
               } else if (status == Status.CLOSED) {
                   buff.flip();
                   return -1;
                   //throw new IOException("SSLEngine Closed");

               }
           } while (status != Status.OK);

           remaining = netRecvBuffer.remaining();
           return buff.position();
       }

    private void doHandshake() throws IOException {

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
                           throw new IOException("SSLHandshake failed");
                       }
                       break;

                   case NEED_UNWRAP:
                       tmpBuff.clear();
                       if (readAndUnwrap(tmpBuff) < 0) {
                           throw new IOException("SSLHandshake failed");
                       }
                       assert tmpBuff.position() == 0;
                       break;
                   default:
               }
               status = sslEngine.getHandshakeStatus();
           }
           handShakeDone = true;
       }}
