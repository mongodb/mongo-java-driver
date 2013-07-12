package org.mongodb.connection.impl;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.bson.ByteBuf;
import org.mongodb.connection.AsyncConnection;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ResponseSettings;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.SingleResultCallback;

// Just preserving this.  probably get deleted.
class DefaultAsyncSSLConnection implements AsyncConnection {
    private final ServerAddress serverAddress;
    private final BufferProvider bufferProvider;
    private ByteBuffer inBuffer;
    private ByteBuffer outBuffer;
    private SocketClient socketClient;
    private boolean handShakeDone = false;

    private int remaining = 0;

    public DefaultAsyncSSLConnection(final ServerAddress serverAddress, final BufferProvider bufferProvider) {
        this.serverAddress = serverAddress;
        this.bufferProvider = bufferProvider;
    }

    protected void doOpen() {
    }

    @Override
    public void sendMessage(final List<ByteBuf> byteBuffers, final SingleResultCallback<Void> callback) {
        System.out.println("DefaultSSLAsyncConnection.sendMessage : byteBuffers = [" + byteBuffers + "], callback = [" + callback + "]");
        if (socketClient == null) {
            doOpen();
        }
//        for (ByteBuf byteBuffer : byteBuffers) {
//        }
        callback.onResult(null, null);
    }

    @Override
    public void receiveMessage(final ResponseSettings responseSettings, final SingleResultCallback<ResponseBuffers> callback) {
        System.out.println(
            "DefaultSSLAsyncConnection.receiveMessage : responseSettings = [" + responseSettings + "], callback = [" + callback + "]");
//        ByteBuffer response = bufferProvider.get(responseSettings.getMaxMessageSize()).asNIO();
//        try {
//            socketClient.unwrap(response, inBuffer);
//        } catch (SSLException e) {
//            throw new RuntimeException(e.getMessage(), e);
//        }
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

    @Override
    public ServerAddress getServerAddress() {
        return serverAddress;
    }
}