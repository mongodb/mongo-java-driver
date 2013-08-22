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

package org.mongodb.connection.impl;

import org.mongodb.connection.ServerAddress;

final class ConnectionPoolStatistics implements ConnectionPoolStatisticsMBean {
    private ServerAddress serverAddress;
    private DefaultConnectionProviderSettings settings;
    private ConcurrentPool<UsageTrackingConnection> pool;
    private String objectName;

    ConnectionPoolStatistics(final ServerAddress serverAddress, final DefaultConnectionProviderSettings settings,
                             final ConcurrentPool<UsageTrackingConnection> pool, final String objectName) {
        this.serverAddress = serverAddress;
        this.settings = settings;
        this.pool = pool;
        this.objectName = objectName;
    }

    /**
     * Gets the host that this connection pool is connecting to.
     *
     * @return the host
     */
    @Override
    public String getHost() {
        return serverAddress.getHost();
    }

    /**
     * Gets the port that this connection pool is connecting to.
     *
     * @return the port
     */
    @Override
    public int getPort() {
        return serverAddress.getPort();
    }

    /**
     * Gets the minimum allowed size of the pool, including idle and in-use members.
     *
     * @return the minimum size
     */
    @Override
    public int getMinSize() {
        return settings.getMinSize();
    }

    /**
     * Gets the maximum allowed size of the pool, including idle and in-use members.
     *
     * @return the maximum size
     */
    @Override
    public int getMaxSize() {
        return settings.getMaxSize();
    }

    /**
     * Gets the total number of pool members, including idle and and in-use members.
     *
     * @return total number of members
     */
    @Override
    public int getTotal() {
        return pool.getCount();
    }

    /**
     * Gets the number of pool members that are currently in use.
     *
     * @return number of in-use members
     */
    @Override
    public int getInUse() {
        return pool.getInUseCount();
    }

    String getObjectName() {
        return objectName;
    }
}
