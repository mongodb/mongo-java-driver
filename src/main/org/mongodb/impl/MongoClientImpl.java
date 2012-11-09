/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb.impl;

import org.bson.util.BufferPool;
import org.bson.util.PowerOfTwoByteBufferPool;
import org.mongodb.MongoClient;
import org.mongodb.MongoConnection;
import org.mongodb.ServerAddress;
import org.mongodb.util.pool.SimplePool;

import java.nio.ByteBuffer;

class MongoClientImpl implements MongoClient {

    private final SimplePool<MongoConnection> connectionPool;
    private final BufferPool<ByteBuffer> bufferPool = new PowerOfTwoByteBufferPool(24);
    private final ServerAddress serverAddress;

    public MongoClientImpl(ServerAddress serverAddress) {
        this.serverAddress = serverAddress;
        connectionPool = new SimplePool<MongoConnection>(serverAddress.toString(), 100) {
             @Override
             protected MongoConnection createNew() {
                 return new MongoConnection(MongoClientImpl.this.serverAddress, bufferPool);
             }
         };
    }

    @Override
    public DatabaseImpl getDB(final String name) {
         return new DatabaseImpl(name, this);
    }

    BufferPool<ByteBuffer> getBufferPool() {
        return bufferPool;
    }

    SimplePool<MongoConnection> getConnectionPool() {
        return connectionPool;
    }
}
