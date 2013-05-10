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

import org.mongodb.MongoConnectionManager;
import org.mongodb.ServerAddress;
import org.mongodb.pool.Pool;

public class MongoConnectionManagerImpl implements MongoConnectionManager {
    private ServerAddress serverAddress;
    private final Pool<MongoSyncConnection> connectionPool;
    private Pool<MongoAsyncConnection> asyncConnectionPool;

    public MongoConnectionManagerImpl(final ServerAddress serverAddress, final Pool<MongoSyncConnection> connectionPool,
                                      final Pool<MongoAsyncConnection> asyncConnectionPool) {
        this.serverAddress = serverAddress;
        this.connectionPool = connectionPool;
        this.asyncConnectionPool = asyncConnectionPool;
    }

    @Override
    public MongoSyncConnection getConnection() {
        return connectionPool.get();
    }

    @Override
    public void releaseConnection(final MongoSyncConnection connection) {
        connection.close();
    }

    @Override
    public MongoAsyncConnection getAsyncConnection() {
        return asyncConnectionPool.get();
    }

    @Override
    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    @Override
    public void close() {
        try {
            connectionPool.close();
        } finally {
            asyncConnectionPool.close();
        }
    }
}
