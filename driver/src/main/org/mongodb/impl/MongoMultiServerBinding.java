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
import org.mongodb.MongoServerBinding;
import org.mongodb.ServerAddress;
import org.mongodb.io.BufferPool;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mongodb.assertions.Assertions.isTrue;

public abstract class MongoMultiServerBinding implements MongoServerBinding {
    private final List<MongoCredential> credentialList;
    private final MongoClientOptions options;
    private final BufferPool<ByteBuffer> bufferPool;
    private final Map<ServerAddress, MongoServer> mongoClientMap = new HashMap<ServerAddress, MongoServer>();
    private boolean isClosed;

    protected MongoMultiServerBinding(final List<MongoCredential> credentialList,
                                      final MongoClientOptions options, final BufferPool<ByteBuffer> bufferPool) {
        this.credentialList = credentialList;
        this.options = options;
        this.bufferPool = bufferPool;
    }

    @Override
    public BufferPool<ByteBuffer> getBufferPool() {
        isTrue("open", !isClosed());

        return bufferPool;
    }

    @Override
    public synchronized MongoServer getConnectionManagerForServer(final ServerAddress serverAddress) {
        isTrue("open", !isClosed());

        MongoServer connection = mongoClientMap.get(serverAddress);
        if (connection == null) {
            connection = MongoConnectionManagers.createConnectionManager(serverAddress, credentialList, options, bufferPool);
            mongoClientMap.put(serverAddress, connection);
        }
        return connection;
    }

    @Override
    public void close() {
        if (!isClosed()) {
            isClosed = true;
            for (MongoServer cur : mongoClientMap.values()) {
                cur.close();
            }
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }
}