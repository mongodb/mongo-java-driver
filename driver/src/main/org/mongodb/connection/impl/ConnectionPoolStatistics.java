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

import java.util.HashMap;
import java.util.Map;

/**
 * An MBean implementation for connection pool statistics.
 */
final class ConnectionPoolStatistics implements ConnectionPoolStatisticsMBean {
    private static Map<ServerAddress, Integer> serverAddressIntegerMap = new HashMap<ServerAddress, Integer>();

    private ServerAddress serverAddress;
    private final int minSize;
    private final int maxSize;
    private ConcurrentPool<UsageTrackingConnection> pool;
    private String objectName;

    ConnectionPoolStatistics(final ServerAddress serverAddress, final int minSize, final int maxSize,
                             final ConcurrentPool<UsageTrackingConnection> pool) {
        this.serverAddress = serverAddress;
        this.minSize = minSize;
        this.maxSize = maxSize;
        this.pool = pool;
        this.objectName = createObjectName();
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
        return minSize;
    }

    /**
     * Gets the maximum allowed size of the pool, including idle and in-use members.
     *
     * @return the maximum size
     */
    @Override
    public int getMaxSize() {
        return maxSize;
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

    private String createObjectName() {
        return "org.mongodb.driver:type=ConnectionPool,host=" + serverAddress.getHost() + ",port=" + serverAddress.getPort()
                + ",instance=" + getInstanceNumber(serverAddress);
    }

    private static synchronized int getInstanceNumber(final ServerAddress serverAddress) {
        Integer instanceNumber = serverAddressIntegerMap.get(serverAddress);
        if (instanceNumber == null) {
            serverAddressIntegerMap.put(serverAddress, 1);
            return 0;
        }
        else {
            serverAddressIntegerMap.put(serverAddress, instanceNumber + 1);
            return instanceNumber;
        }
    }
}
