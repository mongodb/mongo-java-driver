package org.mongodb.connection.impl;


import java.io.IOException;
import java.nio.channels.CompletionHandler;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.bson.ByteBuf;
import org.mongodb.MongoInternalException;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.MongoSocketOpenException;
import org.mongodb.connection.MongoSocketReadException;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.SingleResultCallback;


class DefaultSSLAsyncConnection extends DefaultAsyncConnection {
    private SocketClient socketClient;
    private final ExecutorService executorService;

    public DefaultSSLAsyncConnection(final ServerAddress serverAddress, final BufferProvider bufferProvider,
        final ExecutorService service) {
        super(serverAddress, bufferProvider);
        executorService = service;
    }

    @Override
    public void sendMessage(final List<ByteBuf> byteBuffers, final SingleResultCallback<Void> callback) {
        ensureOpen(new AsyncCompletionHandler() {
            @Override
            public void completed() {
                socketClient.write(byteBuffers, callback);
            }

            @Override
            public void failed(final Throwable t) {
                callback.onResult(null, new MongoInternalException(t.getMessage(), t));  // TODO
            }
        });
    }

    @Override
    void fillAndFlipBuffer(final ByteBuf buffer, final SingleResultCallback<ByteBuf> callback) {
        socketClient.read(buffer.asNIO(), new BasicCompletionHandler(buffer, callback));
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
                socketClient = new SocketClient(getServerAddress(), executorService);
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