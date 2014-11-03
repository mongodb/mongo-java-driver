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

package com.mongodb.management;

import com.mongodb.connection.ServerId;
import com.mongodb.event.ConnectionEvent;
import com.mongodb.event.ConnectionPoolEvent;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.ConnectionPoolOpenedEvent;
import com.mongodb.event.ConnectionPoolWaitQueueEvent;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.lang.String.format;

/**
 * A connection pool listener that manages a set of JMX MBeans, one for each connection pool.
 *
 * @since 3.0
 */
public class JMXConnectionPoolListener implements ConnectionPoolListener {
    private final ConcurrentMap<ServerId, ConnectionPoolStatistics> map =
        new ConcurrentHashMap<ServerId, ConnectionPoolStatistics>();

    @Override
    public void connectionPoolOpened(final ConnectionPoolOpenedEvent event) {
        ConnectionPoolStatistics statistics = new ConnectionPoolStatistics(event);
        map.put(event.getServerId(), statistics);
        MBeanServerFactory.getMBeanServer().registerMBean(statistics, getMBeanObjectName(event.getServerId()));
    }

    @Override
    public void connectionPoolClosed(final ConnectionPoolEvent event) {
        map.remove(event.getServerId());
        MBeanServerFactory.getMBeanServer().unregisterMBean(getMBeanObjectName(event.getServerId()));
    }

    @Override
    public void connectionCheckedOut(final ConnectionEvent event) {
        ConnectionPoolStatistics statistics = getStatistics(event);
        if (statistics != null) {
            statistics.connectionCheckedOut(event);
        }
    }

    @Override
    public void connectionCheckedIn(final ConnectionEvent event) {
        ConnectionPoolStatistics statistics = getStatistics(event);
        if (statistics != null) {
            statistics.connectionCheckedIn(event);
        }
    }

    @Override
    public void waitQueueEntered(final ConnectionPoolWaitQueueEvent event) {
        ConnectionPoolListener statistics = getStatistics(event);
        if (statistics != null) {
            statistics.waitQueueEntered(event);
        }
    }

    @Override
    public void waitQueueExited(final ConnectionPoolWaitQueueEvent event) {
        ConnectionPoolListener statistics = getStatistics(event);
        if (statistics != null) {
            statistics.waitQueueExited(event);
        }
    }

    @Override
    public void connectionAdded(final ConnectionEvent event) {
        ConnectionPoolStatistics statistics = getStatistics(event);
        if (statistics != null) {
            statistics.connectionAdded(event);
        }
    }

    @Override
    public void connectionRemoved(final ConnectionEvent event) {
        ConnectionPoolStatistics statistics = getStatistics(event);
        if (statistics != null) {
            statistics.connectionRemoved(event);
        }
    }

    String getMBeanObjectName(final ServerId serverId) {
        // we could do a url encode, but since : is the only invalid character in an object name, then
        // we'll simply do it.
        String adjustedClusterId = serverId.getClusterId().getValue().replace(":", "%3A");
        String adjustedHost = serverId.getAddress().getHost().replace(":", "%3A");

        return format("org.mongodb.driver:type=ConnectionPool,clusterId=%s,host=%s,port=%s", adjustedClusterId, adjustedHost,
                      serverId.getAddress().getPort());
    }

    ConnectionPoolStatisticsMBean getMBean(final ServerId serverId) {
        return getStatistics(serverId);
    }

    private ConnectionPoolStatistics getStatistics(final ConnectionEvent event) {
        return getStatistics(event.getConnectionId().getServerId());
    }

    private ConnectionPoolListener getStatistics(final ConnectionPoolEvent event) {
        return getStatistics(event.getServerId());
    }

    private ConnectionPoolStatistics getStatistics(final ServerId serverId) {
        return map.get(serverId);
    }
}
