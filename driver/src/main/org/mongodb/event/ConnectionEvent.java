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

package org.mongodb.event;

import org.mongodb.connection.ServerAddress;

/**
 * A connection-related event.
 *
 * @since 3.0
 */
public class ConnectionEvent extends ClusterEvent {
    private final ServerAddress serverAddress;
    private final String connectionId;

    /**
     * Constructs a new instance of the event.
     *
     * @param clusterId the cluster id
     * @param serverAddress the server address
     * @param connectionId the connection id
     */
    public ConnectionEvent(final String clusterId, final ServerAddress serverAddress, final String connectionId) {
        super(clusterId);
        this.serverAddress = serverAddress;
        this.connectionId = connectionId;
    }

    /**
     * Gets the server address associated with this connection.
     *
     * @return the server address
     */
    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    /**
     * Gets the identifier for this connection.
     *
     * @return the connection id
     */
    public String getConnectionId() {
        return connectionId;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        final ConnectionEvent that = (ConnectionEvent) o;

        if (!getClusterId().equals(that.getClusterId())) {
            return false;
        }
        if (!getServerAddress().equals(that.getServerAddress())) {
            return false;
        }
        if (!connectionId.equals(that.connectionId)) {
            return false;
        }
        if (!serverAddress.equals(that.serverAddress)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + serverAddress.hashCode();
        result = 31 * result + connectionId.hashCode();
        return result;
    }
}
