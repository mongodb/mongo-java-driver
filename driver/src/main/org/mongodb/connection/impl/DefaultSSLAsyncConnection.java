package org.mongodb.connection.impl;


import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.CompletionHandler;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.List;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.bson.ByteBuf;
import org.mongodb.MongoInternalException;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.MongoSocketOpenException;
import org.mongodb.connection.MongoSocketReadException;
import org.mongodb.connection.MongoSocketWriteException;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.SingleResultCallback;


class DefaultSSLAsyncConnection extends DefaultAsyncConnection {
    private SocketClient socketClient;
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

    public DefaultSSLAsyncConnection(final ServerAddress serverAddress, final BufferProvider bufferProvider) {
        super(serverAddress, bufferProvider);
    }

    @Override
    public void sendMessage(final List<ByteBuf> byteBuffers, final SingleResultCallback<Void> callback) {
        ensureOpen(new AsyncCompletionHandler() {
            @Override
            public void completed() {
                try {
                    for (final ByteBuf byteBuffer : byteBuffers) {
                        socketClient.write(byteBuffer.asNIO());
                    }
                } catch (IOException e) {
                    throw new MongoSocketWriteException(e.getMessage(), getServerAddress(), e);
                }
                callback.onResult(null, null);
            }

            @Override
            public void failed(final Throwable t) {
                callback.onResult(null, new MongoInternalException(t.getMessage(), t));  // TODO
            }
        });
    }

    @Override
    void fillAndFlipBuffer(final ByteBuf buffer, final SingleResultCallback<ByteBuf> callback) {
        socketClient.read(buffer, new BasicCompletionHandler(buffer, callback));
    }

    private final class BasicCompletionHandler implements CompletionHandler<Integer, Void> {
        private final ByteBuf dst;
        private final SingleResultCallback<ByteBuf> callback;

        private BasicCompletionHandler(final ByteBuf dst, final SingleResultCallback<ByteBuf> callback) {
            this.dst = dst;
            this.callback = callback;
        }

        @Override
        public void completed(final Integer result, final Void attachment) {
            callback.onResult(dst, null);
        }

        @Override
        public void failed(final Throwable t, final Void attachment) {
            callback.onResult(null, new MongoSocketReadException("Exception reading from channel", getServerAddress(), t));
        }
    }

    protected void ensureOpen(final AsyncCompletionHandler handler) {
        try {
            if (socketClient != null) {
                handler.completed();
            } else {
                socketClient = new SocketClient(getServerAddress().getSocketAddress());
                socketClient.setSSLContext(SSL_CONTEXT);
                socketClient.connect(handler);
            }
        } catch (IOException e) {
            throw new MongoSocketOpenException(e.getMessage(), getServerAddress(), e);
        }
    }

    @Override
    public void close() {
        try {
            socketClient.close();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public boolean isClosed() {
        return socketClient == null || !socketClient.isConnected();
    }
}