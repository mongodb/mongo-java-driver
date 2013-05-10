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
import org.mongodb.MongoConnectionManager;
import org.mongodb.MongoConnectionStrategy;
import org.mongodb.MongoCredential;
import org.mongodb.MongoServerBinding;
import org.mongodb.ReadPreference;
import org.mongodb.ServerAddress;
import org.mongodb.io.BufferPool;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mongodb.assertions.Assertions.isTrue;

public class MongoMultiServerBinding implements MongoServerBinding {
    private final List<MongoCredential> credentialList;
    private final MongoClientOptions options;
    private final BufferPool<ByteBuffer> bufferPool;
    private final Map<ServerAddress, MongoConnectionManager> mongoClientMap = new HashMap<ServerAddress, MongoConnectionManager>();
    private final MongoConnectionStrategy connectionStrategy;
    private boolean isClosed;

    public MongoMultiServerBinding(final MongoConnectionStrategy connectionStrategy, final List<MongoCredential> credentialList,
                                   final MongoClientOptions options, final BufferPool<ByteBuffer> bufferPool) {
        this.connectionStrategy = connectionStrategy;
        this.credentialList = credentialList;
        this.options = options;
        this.bufferPool = bufferPool;
    }

    @Override
    public MongoConnectionManager getConnectionManagerForWrite() {
        isTrue("open", !isClosed());
        final ServerAddress serverAddress = connectionStrategy.getAddressOfPrimary();
        return getConnectionManagerForServer(serverAddress);
    }

    @Override
    public MongoConnectionManager getConnectionManagerForRead(final ReadPreference readPreference) {
        isTrue("open", !isClosed());

        final ServerAddress serverAddress = connectionStrategy.getAddressForReadPreference(readPreference);

        return getConnectionManagerForServer(serverAddress);
    }

    @Override
    public List<ServerAddress> getAllServerAddresses() {
        isTrue("open", !isClosed());

        return connectionStrategy.getAllAddresses();
    }

    @Override
    public BufferPool<ByteBuffer> getBufferPool() {
        isTrue("open", !isClosed());

        return bufferPool;
    }

    @Override
    public synchronized MongoConnectionManager getConnectionManagerForServer(final ServerAddress serverAddress) {
        isTrue("open", !isClosed());

        MongoConnectionManager connection = mongoClientMap.get(serverAddress);
        if (connection == null) {
            connection = MongoServerBindings.create(serverAddress, credentialList, options, bufferPool);
            mongoClientMap.put(serverAddress, connection);
        }
        return connection;
    }

    @Override
    public void close() {
        if (!isClosed()) {
            isClosed = true;
            for (MongoConnectionManager cur : mongoClientMap.values()) {
                cur.close();
            }
            connectionStrategy.close();
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }
}