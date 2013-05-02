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

import org.mongodb.MongoConnectionManager;
import org.mongodb.MongoServerBinding;
import org.mongodb.ReadPreference;
import org.mongodb.ServerAddress;

import java.util.List;

class MonotonicallyConsistentMongoServerBinding implements MongoServerBinding {
    private final MongoServerBinding serverConnectorManager;
    private ReadPreference lastRequestedReadPreference;
    private MongoConnectionManager connectionManagerForReads;
    private MongoConnectionManager connectionManagerForWrites;
    private MongoConnection connectorForReads;
    private MongoConnection connectorForWrites;

    public MonotonicallyConsistentMongoServerBinding(final MongoServerBinding serverConnectorManager) {
        this.serverConnectorManager = serverConnectorManager;
    }

    @Override
    public MongoConnectionManager getConnectionManagerForWrite() {
        return new MongoConnectionManagerForWrites();
    }

    @Override
    public MongoConnectionManager getConnectionManagerForRead(final ReadPreference readPreference) {
        return new MongoConnectionManagerForReads(readPreference);
    }

    @Override
    public MongoConnectionManager getConnectionManagerForServer(final ServerAddress serverAddress) {
        return serverConnectorManager.getConnectionManagerForServer(serverAddress);
    }

    @Override
    public List<ServerAddress> getAllServerAddresses() {
        return serverConnectorManager.getAllServerAddresses();
    }

    @Override
    public void close() {
        if (connectorForReads != null) {
            connectionManagerForReads.releaseConnection(connectorForReads);
            connectionManagerForReads = null;
            connectorForReads = null;
        }
        if (connectorForWrites != null) {
            connectionManagerForWrites.releaseConnection(connectorForWrites);
            connectionManagerForWrites = null;
            connectorForWrites = null;
        }
    }

    private synchronized MongoConnection getConnectorForWrites() {
        if (connectorForWrites == null) {
            connectionManagerForWrites = serverConnectorManager.getConnectionManagerForWrite();
            connectorForWrites = connectionManagerForWrites.getConnection();
            if (connectorForReads != null) {
                connectionManagerForReads.releaseConnection(connectorForReads);
                connectorForReads = null;
                connectionManagerForReads = null;
            }
        }
        return connectorForWrites;
    }

    private synchronized MongoConnection getConnectorForReads(final ReadPreference readPreference) {
        if (connectorForWrites != null) {
            return connectorForWrites;
        }
        else if (connectorForReads == null || !readPreference.equals(lastRequestedReadPreference)) {
            lastRequestedReadPreference = readPreference;
            if (connectorForReads != null) {
                connectionManagerForReads.releaseConnection(connectorForReads);
            }
            connectionManagerForReads = serverConnectorManager.getConnectionManagerForRead(readPreference);
            connectorForReads = connectionManagerForReads.getConnection();
        }
        return connectorForReads;
    }


    private abstract class AbstractConnectionManager implements MongoConnectionManager {
        @Override
        public void releaseConnection(final MongoConnection connection) {
            // Do nothing.  Release when the containing instance is closed.
        }

        @Override
        public ServerAddress getServerAddress() {
            return getConnection().getServerAddress();
        }

        @Override
        public void close() {
        }
    }

    private final class MongoConnectionManagerForReads extends AbstractConnectionManager {
        private final ReadPreference readPreference;

        private MongoConnectionManagerForReads(final ReadPreference readPreference) {
            this.readPreference = readPreference;
        }

        @Override
        public MongoConnection getConnection() {
            return getConnectorForReads(readPreference);
        }
    }

    private final class MongoConnectionManagerForWrites extends AbstractConnectionManager {
        @Override
        public MongoConnection getConnection() {
            return getConnectorForWrites();
        }
    }
}
