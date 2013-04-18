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

import org.mongodb.MongoConnector;
import org.mongodb.PoolableConnectionManager;
import org.mongodb.ReadPreference;
import org.mongodb.ServerAddress;
import org.mongodb.ServerConnectorManager;

import java.util.List;

public class MongoSessionFactory {
    private final MongoConnector rootConnector;

    public MongoSessionFactory(final MongoConnector rootConnector) {
        this.rootConnector = rootConnector;
    }

    public MongoConnector getSession() {
        final ServerConnectorManager connectorManager =
                ((DelegatingMongoConnector) rootConnector).getConnectorManager();
        return new DelegatingMongoConnector(new ServerConnectorManager() {
            @Override
            public PoolableConnectionManager getConnectionManagerForWrite() {
                return connectorManager.getConnectionManagerForWrite();
            }

            @Override
            public PoolableConnectionManager getConnectionManagerForRead(final ReadPreference readPreference) {
                return connectorManager.getConnectionManagerForRead(readPreference);
            }

            @Override
            public PoolableConnectionManager getConnectionManagerForServer(final ServerAddress serverAddress) {
                return connectorManager.getConnectionManagerForServer(serverAddress);
            }

            @Override
            public List<ServerAddress> getAllServerAddresses() {
                return rootConnector.getServerAddressList();
            }

            @Override
            public void close() {
            }
        });
    }
}


