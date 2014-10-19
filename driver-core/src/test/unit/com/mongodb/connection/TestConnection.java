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

package com.mongodb.connection;

import com.mongodb.ServerAddress;
import com.mongodb.async.SingleResultCallback;
import org.bson.ByteBuf;
import org.bson.io.BsonInput;

import java.util.List;

public class TestConnection implements Connection {
    private final TestInternalConnection wrapped;
    private boolean unexpectedServerState;

    TestConnection(final ServerAddress serverAddress) {
        wrapped = new TestInternalConnection(serverAddress);
    }

    public void enqueueReply(final ResponseBuffers buffers) {
        wrapped.enqueueReply(buffers);
    }

    public List<BsonInput> getSent() {
        return wrapped.getSent();
    }

    public boolean unexpectedServerStateCalled() {
        return unexpectedServerState;
    }

    @Override
    public int getCount() {
        return 0;
    }

    @Override
    public Connection retain() {
        return this;
    }

    @Override
    public void release() {
    }

    @Override
    public ConnectionDescription getDescription() {
        return wrapped.getDescription();
    }

    @Override
    public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
        wrapped.sendMessage(byteBuffers, lastRequestId);
    }

    @Override
    public ResponseBuffers receiveMessage(final int responseTo) {
        return wrapped.receiveMessage(responseTo);
    }

    @Override
    public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
        wrapped.sendMessageAsync(byteBuffers, lastRequestId, callback);
    }

    @Override
    public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
        wrapped.receiveMessageAsync(responseTo, callback);
    }

    @Override
    public ServerAddress getServerAddress() {
        return null;
    }

    @Override
    public String getId() {
        return null;
    }

    @Override
    public void unexpectedServerState() {
        unexpectedServerState = true;
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return wrapped.getBuffer(size);
    }
}
