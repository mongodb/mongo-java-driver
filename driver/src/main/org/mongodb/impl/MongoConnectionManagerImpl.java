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
import org.mongodb.pool.SimplePool;

public class MongoConnectionManagerImpl implements MongoConnectionManager {
    private ServerAddress serverAddress;
    private final SimplePool<MongoConnection> connectionPool;

    public MongoConnectionManagerImpl(final ServerAddress serverAddress, final SimplePool<MongoConnection> connectionPool) {
        this.serverAddress = serverAddress;
        this.connectionPool = connectionPool;
    }

    @Override
    public MongoConnection getConnection() {
        return connectionPool.get();
    }

    @Override
    public void releaseConnection(final MongoConnection connection) {
        connection.release();
    }

    @Override
    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    @Override
    public void close() {
        connectionPool.close();
    }
}
