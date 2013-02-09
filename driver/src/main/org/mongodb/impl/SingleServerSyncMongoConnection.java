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
 *
 */

package org.mongodb.impl;

import org.bson.util.BufferPool;
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoConnection;
import org.mongodb.ServerAddress;
import org.mongodb.io.MongoChannel;
import org.mongodb.pool.SimplePool;
import org.mongodb.serialization.serializers.DocumentSerializer;

import java.nio.ByteBuffer;

class SingleServerSyncMongoConnection extends SingleServerMongoConnection {
    private final SimplePool<MongoChannel> channelPool;

    public SingleServerSyncMongoConnection(final ServerAddress serverAddress, final MongoClientOptions options,
                                       final BufferPool<ByteBuffer> bufferPool) {
        super(options, bufferPool, serverAddress);
        channelPool = new SimplePool<MongoChannel>(serverAddress.toString(), getOptions().getConnectionsPerHost()) {
            @Override
            protected MongoChannel createNew() {
                return new MongoChannel(getServerAddress(), getBufferPool(),
                        new DocumentSerializer(getOptions().getPrimitiveSerializers()));
            }
        };
    }

    @Override
    public void close() {
        channelPool.close();
    }

    @Override
    protected MongoConnection createSingleChannelMongoConnection() {
        return new SingleChannelSyncMongoConnection(getServerAddress(), channelPool, getBufferPool(), getOptions());
    }
}
