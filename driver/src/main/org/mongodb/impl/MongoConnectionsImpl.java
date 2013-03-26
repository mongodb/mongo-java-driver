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

import org.mongodb.MongoClientOptions;
import org.mongodb.ServerAddress;
import org.mongodb.io.BufferPool;
import org.mongodb.io.PowerOfTwoByteBufferPool;
import org.mongodb.pool.SimplePool;

import java.nio.ByteBuffer;
import java.util.List;

public final class MongoConnectionsImpl {
    private MongoConnectionsImpl() {
    }

    public static SingleServerMongoConnector create(final ServerAddress serverAddress, final MongoClientOptions options) {
        return create(serverAddress, options, new PowerOfTwoByteBufferPool());
    }

    public static SingleServerMongoConnector create(final ServerAddress serverAddress, final MongoClientOptions options,
                                                     final BufferPool<ByteBuffer> bufferPool) {
        if (options.isAsyncEnabled()
                && !options.isSSLEnabled()
                && !System.getProperty("org.mongodb.useSocket", "false").equals("true")) {
            return new SingleServerMongoConnector(options, serverAddress,
                    new SimplePool<MongoPoolableConnector>(serverAddress.toString(), options.getConnectionsPerHost()) {
                        @Override
                        protected MongoPoolableConnector createNew() {
                            return new SingleChannelAsyncMongoConnector(serverAddress, this, bufferPool, options);
                        }

                        @Override
                        public void close() {
                            super.close();
                            bufferPool.close();
                        }
                    });
        } else {
            return new SingleServerMongoConnector(options, serverAddress,
                    new SimplePool<MongoPoolableConnector>(serverAddress.toString(), options.getConnectionsPerHost()) {
                        @Override
                        protected MongoPoolableConnector createNew() {
                            return new SingleChannelSyncMongoConnector(serverAddress, this, bufferPool, options);
                        }

                        @Override
                        public void close() {
                            super.close();
                            bufferPool.close();
                        }
                    });
        }
    }

    public static MultipleServerMongoConnector create(final List<ServerAddress> seedList, final MongoClientOptions options) {
        return new MultipleServerMongoConnector(new ReplicaSetConnectionStrategy(seedList), options);
    }

}
