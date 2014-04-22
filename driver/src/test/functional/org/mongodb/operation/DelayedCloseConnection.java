package org.mongodb.operation;

import org.bson.ByteBuf;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.SingleResultCallback;

import java.util.List;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

class DelayedCloseConnection implements Connection {
    private final Connection wrapped;
    private boolean isClosed;


    public DelayedCloseConnection(final Connection wrapped) {
        this.wrapped = notNull("wrapped", wrapped);
    }

    Connection getWrapped() {
        return wrapped;
    }

    @Override
    public ServerAddress getServerAddress() {
        isTrue("open", !isClosed());
        return wrapped.getServerAddress();
    }

    @Override
    public ByteBuf getBuffer(final int capacity) {
        isTrue("open", !isClosed());
        return wrapped.getBuffer(capacity);
    }

    @Override
    public ServerDescription getServerDescription() {
        isTrue("open", !isClosed());
        return wrapped.getServerDescription();
    }

    @Override
    public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
        isTrue("open", !isClosed());
        wrapped.sendMessage(byteBuffers, lastRequestId);
    }

    @Override
    public ResponseBuffers receiveMessage(final int responseTo) {
        isTrue("open", !isClosed());
        return wrapped.receiveMessage(responseTo);
    }

    @Override
    public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
        isTrue("open", !isClosed());
        wrapped.sendMessageAsync(byteBuffers, lastRequestId, callback);
    }

    @Override
    public void receiveMessageAsync(final int responseTo,
                                    final SingleResultCallback<ResponseBuffers> callback) {
        isTrue("open", !isClosed());
        wrapped.receiveMessageAsync(responseTo, callback);
    }

    @Override
    public String getId() {
        return wrapped.getId();
    }

    @Override
    public void close() {
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }
}
