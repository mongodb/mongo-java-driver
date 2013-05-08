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

import org.mongodb.MongoClientOptions;
import org.mongodb.MongoCredential;
import org.mongodb.ServerAddress;
import org.mongodb.io.BufferPool;
import org.mongodb.io.CachingAuthenticator;
import org.mongodb.io.ChannelAwareOutputBuffer;
import org.mongodb.io.DefaultMongoGateway;
import org.mongodb.io.ResponseBuffers;
import org.mongodb.pool.SimplePool;

import java.nio.ByteBuffer;
import java.util.List;

class DefaultMongoSyncConnection implements MongoSyncConnection {
    private final BufferPool<ByteBuffer> bufferPool;
    private final SimplePool<MongoSyncConnection> channelPool;
    private DefaultMongoGateway channel;

    DefaultMongoSyncConnection(final ServerAddress serverAddress, final List<MongoCredential> credentialList,
                               final SimplePool<MongoSyncConnection> channelPool, final BufferPool<ByteBuffer> bufferPool,
                               final MongoClientOptions options) {
        this.channelPool = channelPool;
        this.bufferPool = bufferPool;
        this.channel = DefaultMongoGateway.create(serverAddress, bufferPool, options,
                new CachingAuthenticator(new MongoCredentialsStore(credentialList), this));
    }

    @Override
    public void close() {
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public ServerAddress getServerAddress() {
        return channel.getServerAddress();
    }

    @Override
    public void sendMessage(final ChannelAwareOutputBuffer buffer) {
        channel.sendMessage(buffer);
    }

    @Override
    public ResponseBuffers sendAndReceiveMessage(final ChannelAwareOutputBuffer buffer) {
        return channel.sendAndReceiveMessage(buffer);
    }

    @Override
    public BufferPool<ByteBuffer> getBufferPool() {
        return bufferPool;
    }

    @Override
    public void release() {
        if (channel == null) {
            throw new IllegalStateException("Can not release a channel that's already closed");
        }
        if (channelPool == null) {
            throw new IllegalStateException("Can not release a channel not associated with a pool");
        }

        channelPool.done(this);
    }
}
