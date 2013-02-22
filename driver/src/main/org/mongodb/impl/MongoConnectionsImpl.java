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

import org.mongodb.io.BufferPool;
import org.mongodb.MongoClientOptions;
import org.mongodb.ServerAddress;
import org.mongodb.async.AsyncDetector;
import org.mongodb.io.PowerOfTwoByteBufferPool;
import org.mongodb.pool.SimplePool;

import java.nio.ByteBuffer;

public final class MongoConnectionsImpl {
    private MongoConnectionsImpl() {
    }

    public static SingleServerMongoConnection create(final ServerAddress serverAddress, final MongoClientOptions options) {
        return create(serverAddress, options, new PowerOfTwoByteBufferPool());
    }

    public static SingleServerMongoConnection create(final ServerAddress serverAddress, final MongoClientOptions options,
                                                     final BufferPool<ByteBuffer> bufferPool) {
        if (AsyncDetector.javaVersionSupportsAsync()) {
            return new SingleServerMongoConnection(options, serverAddress,
                    new SimplePool<MongoPoolableConnection>(serverAddress.toString(), options.getConnectionsPerHost()) {
                        @Override
                        protected MongoPoolableConnection createNew() {
                            return new SingleChannelAsyncMongoConnection(serverAddress, this, bufferPool, options);
                        }
                    });
        }
        else {
            return new SingleServerMongoConnection(options, serverAddress,
                    new SimplePool<MongoPoolableConnection>(serverAddress.toString(), options.getConnectionsPerHost()) {
                        @Override
                        protected MongoPoolableConnection createNew() {
                            return new SingleChannelSyncMongoConnection(serverAddress, this, bufferPool, options);
                        }
                    });
        }
    }

}
