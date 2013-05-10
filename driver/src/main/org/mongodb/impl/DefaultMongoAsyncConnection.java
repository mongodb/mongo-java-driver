/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.impl;

import org.mongodb.MongoCredential;
import org.mongodb.ServerAddress;
import org.mongodb.async.SingleResultCallback;
import org.mongodb.io.BufferPool;
import org.mongodb.io.ChannelAwareOutputBuffer;
import org.mongodb.io.ResponseBuffers;
import org.mongodb.io.async.CachingAsyncAuthenticator;
import org.mongodb.io.async.MongoAsynchronousSocketChannelGateway;
import org.mongodb.pool.SimplePool;

import java.nio.ByteBuffer;
import java.util.List;

// TODO: Take this class private
public class DefaultMongoAsyncConnection implements MongoAsyncConnection {
    private final ServerAddress serverAddress;
    private final SimplePool<MongoAsyncConnection> connectionPool;
    private volatile MongoAsynchronousSocketChannelGateway channel;
    private volatile boolean activeAsyncCall;
    private volatile boolean releasePending;

    public DefaultMongoAsyncConnection(final ServerAddress serverAddress, final List<MongoCredential> credentialList,
                                       final SimplePool<MongoAsyncConnection> connectionPool, final BufferPool<ByteBuffer> bufferPool) {
        this.serverAddress = serverAddress;
        this.connectionPool = connectionPool;
        this.channel = new MongoAsynchronousSocketChannelGateway(serverAddress,
                new CachingAsyncAuthenticator(new MongoCredentialsStore(credentialList), this, bufferPool), bufferPool);
    }

    @Override
    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    @Override
    public synchronized void close() {
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public boolean isClosed() {
        return channel == null;
    }

    @Override
    public synchronized void release() {
        if (channel == null) {
            throw new IllegalStateException("Can not release a channel that's already closed");
        }
        if (connectionPool == null) {
            throw new IllegalStateException("Can not release a channel not associated with a pool");
        }

        if (activeAsyncCall) {
            releasePending = true;
        }
        else {
            releasePending = false;
            connectionPool.done(this);
        }
    }

    @Override
    public synchronized void releaseIfPending() {
        activeAsyncCall = false;
        if (releasePending) {
            release();
        }
    }

    @Override
    public void setActiveAsyncCall() {
        activeAsyncCall = true;
    }

    @Override
    public void sendMessage(final ChannelAwareOutputBuffer buffer, final SingleResultCallback<ResponseBuffers> callback) {
        channel.sendMessage(buffer, callback);
    }

    @Override
    public void sendAndReceiveMessage(final ChannelAwareOutputBuffer buffer, final SingleResultCallback<ResponseBuffers> callback) {
        channel.sendAndReceiveMessage(buffer, callback);
    }
}
