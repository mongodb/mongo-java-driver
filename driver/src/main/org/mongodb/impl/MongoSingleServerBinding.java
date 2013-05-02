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

import org.mongodb.MongoServerBinding;
import org.mongodb.PoolableConnectionManager;
import org.mongodb.ReadPreference;
import org.mongodb.ServerAddress;

import java.util.Arrays;
import java.util.List;

public class MongoSingleServerBinding implements MongoServerBinding {
    private final PoolableConnectionManager connectionManager;

    public MongoSingleServerBinding(final PoolableConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    public PoolableConnectionManager getConnectionManagerForWrite() {
        return connectionManager;
    }

    @Override
    public PoolableConnectionManager getConnectionManagerForRead(final ReadPreference readPreference) {
        return connectionManager;
    }

    @Override
    public PoolableConnectionManager getConnectionManagerForServer(final ServerAddress serverAddress) {
        return connectionManager;
    }

    @Override
    public List<ServerAddress> getAllServerAddresses() {
        return Arrays.asList(connectionManager.getServerAddress());
    }

    @Override
    public void close() {
        connectionManager.close();
    }
}
