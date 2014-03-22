/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb.session;

import org.bson.ByteBuf;
import org.mongodb.annotations.NotThreadSafe;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.SingleResultCallback;

import java.util.List;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

@NotThreadSafe
class DelayedCloseConnection implements Connection {
    private final Connection wrapped;
    private boolean isClosed;

    public DelayedCloseConnection(final Connection wrapped) {
        this.wrapped = notNull("wrapped", wrapped);
    }

    @Override
    public ByteBuf getBuffer(final int capacity) {
        isTrue("open", !isClosed());
        return wrapped.getBuffer(capacity);
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
    public void close() {
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public ServerAddress getServerAddress() {
        isTrue("open", !isClosed());
        return wrapped.getServerAddress();
    }

    @Override
    public String getId() {
        return wrapped.getId();
    }
}
