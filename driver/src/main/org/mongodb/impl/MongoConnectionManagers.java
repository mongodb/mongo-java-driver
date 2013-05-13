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
import org.mongodb.MongoServer;
import org.mongodb.ServerAddress;
import org.mongodb.io.BufferPool;
import org.mongodb.pool.Pool;

import java.nio.ByteBuffer;
import java.util.List;

public final class MongoConnectionManagers {
    static MongoServer createConnectionManager(final ServerAddress serverAddress, final List<MongoCredential> credentialList,
                                                          final MongoClientOptions options, final BufferPool<ByteBuffer> bufferPool) {

        Pool<MongoSyncConnection> connectionPool = new DefaultMongoConnectionPool(new DefaultMongoSyncConnectionFactory(options,
                serverAddress, bufferPool, credentialList), options);
        Pool<MongoAsyncConnection> asyncConnectionPool = null;

        if (options.isAsyncEnabled() && !options.isSSLEnabled() && !System.getProperty("org.mongodb.useSocket", "false").equals("true")) {
            asyncConnectionPool = new DefaultMongoAsyncConnectionPool(new DefaultMongoAsyncConnectionFactory(options, serverAddress,
                    bufferPool, credentialList), options);
        }
        return new DefaultMongoServer(serverAddress, connectionPool, asyncConnectionPool);
    }

    private MongoConnectionManagers() {
    }
}
