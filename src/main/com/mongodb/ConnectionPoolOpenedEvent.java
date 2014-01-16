/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb;

/**
 * An event signifying the opening of a connection pool.
 */
class ConnectionPoolOpenedEvent extends ConnectionPoolEvent {
    private final ConnectionPoolSettings settings;

    /**
     * Constructs a new instance of the event.
     *
     * @param clusterId     the cluster id
     * @param serverAddress the server address
     * @param settings      the connection pool settings
     */
    public ConnectionPoolOpenedEvent(final String clusterId, final ServerAddress serverAddress, final ConnectionPoolSettings settings) {
        super(clusterId, serverAddress);
        this.settings = settings;
    }

    /**
     * Gets the settings for this connection pool.
     *
     * @return the settings
     */
    public ConnectionPoolSettings getSettings() {
        return settings;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConnectionPoolOpenedEvent that = (ConnectionPoolOpenedEvent) o;

        if (!getClusterId().equals(that.getClusterId())) {
            return false;
        }
        if (!getServerAddress().equals(that.getServerAddress())) {
            return false;
        }
        if (!settings.equals(that.getSettings())) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + settings.hashCode();
        return result;
    }
}

