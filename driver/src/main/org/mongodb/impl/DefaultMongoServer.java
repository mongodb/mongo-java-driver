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

import org.mongodb.MongoServer;
import org.mongodb.ServerAddress;
import org.mongodb.pool.Pool;

import static org.mongodb.assertions.Assertions.notNull;

public class DefaultMongoServer implements MongoServer {
    private ServerAddress serverAddress;
    private final Pool<MongoSyncConnection> connectionPool;
    private Pool<MongoAsyncConnection> asyncConnectionPool;

    public DefaultMongoServer(final ServerAddress serverAddress, final Pool<MongoSyncConnection> connectionPool,
                              final Pool<MongoAsyncConnection> asyncConnectionPool) {
        this.serverAddress = notNull("serverAddress", serverAddress);
        this.connectionPool = notNull("connectionPool", connectionPool);
        this.asyncConnectionPool = asyncConnectionPool;
    }

    @Override
    public MongoSyncConnection getConnection() {
        return connectionPool.get();
    }

    @Override
    public MongoAsyncConnection getAsyncConnection() {
        if (asyncConnectionPool == null) {
            throw new UnsupportedOperationException("Asynchronous connections not supported in this version of Java");
        }
        return asyncConnectionPool.get();
    }

    @Override
    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    @Override
    public void close() {
        try {
            if (asyncConnectionPool != null) {
                asyncConnectionPool.close();
            }
        } finally {
            connectionPool.close();
        }
    }
}
