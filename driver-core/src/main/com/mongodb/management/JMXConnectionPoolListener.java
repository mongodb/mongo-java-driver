/*
 * Copyright 2008-present MongoDB, Inc.
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

import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ServerId;
import com.mongodb.event.ConnectionAddedEvent;
import com.mongodb.event.ConnectionCheckedInEvent;
import com.mongodb.event.ConnectionCheckedOutEvent;
import com.mongodb.event.ConnectionPoolClosedEvent;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.ConnectionPoolOpenedEvent;
import com.mongodb.event.ConnectionPoolWaitQueueEnteredEvent;
import com.mongodb.event.ConnectionPoolWaitQueueExitedEvent;
import com.mongodb.event.ConnectionRemovedEvent;

import javax.management.ObjectName;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.lang.String.format;
import static java.util.Arrays.asList;

/**
 * A connection pool listener that manages a set of JMX MBeans, one for each connection pool.
 *
 * @since 3.5
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
    public void connectionPoolClosed(final ConnectionPoolClosedEvent event) {
        map.remove(event.getServerId());
        MBeanServerFactory.getMBeanServer().unregisterMBean(getMBeanObjectName(event.getServerId()));
    }

    @Override
    public void connectionCheckedOut(final ConnectionCheckedOutEvent event) {
        ConnectionPoolStatistics statistics = getStatistics(event.getConnectionId());
        if (statistics != null) {
            statistics.connectionCheckedOut(event);
        }
    }

    @Override
    public void connectionCheckedIn(final ConnectionCheckedInEvent event) {
        ConnectionPoolStatistics statistics = getStatistics(event.getConnectionId());
        if (statistics != null) {
            statistics.connectionCheckedIn(event);
        }
    }

    @Override
    public void waitQueueEntered(final ConnectionPoolWaitQueueEnteredEvent event) {
        ConnectionPoolListener statistics = getStatistics(event.getServerId());
        if (statistics != null) {
            statistics.waitQueueEntered(event);
        }
    }

    @Override
    public void waitQueueExited(final ConnectionPoolWaitQueueExitedEvent event) {
        ConnectionPoolListener statistics = getStatistics(event.getServerId());
        if (statistics != null) {
            statistics.waitQueueExited(event);
        }
    }

    @Override
    public void connectionAdded(final ConnectionAddedEvent event) {
        ConnectionPoolStatistics statistics = getStatistics(event.getConnectionId());
        if (statistics != null) {
            statistics.connectionAdded(event);
        }
    }

    @Override
    public void connectionRemoved(final ConnectionRemovedEvent event) {
        ConnectionPoolStatistics statistics = getStatistics(event.getConnectionId());
        if (statistics != null) {
            statistics.connectionRemoved(event);
        }
    }

    String getMBeanObjectName(final ServerId serverId) {
        String name = format("org.mongodb.driver:type=ConnectionPool,clusterId=%s,host=%s,port=%s",
                             ensureValidValue(serverId.getClusterId().getValue()),
                             ensureValidValue(serverId.getAddress().getHost()),
                             serverId.getAddress().getPort());
        if (serverId.getClusterId().getDescription() != null) {
            name = format("%s,description=%s", name, ensureValidValue(serverId.getClusterId().getDescription()));
        }
        return name;
    }

    // for unit test
    ConnectionPoolStatisticsMBean getMBean(final ServerId serverId) {
        return getStatistics(serverId);
    }

    private ConnectionPoolStatistics getStatistics(final ConnectionId connectionId) {
        return getStatistics(connectionId.getServerId());
    }

    private ConnectionPoolStatistics getStatistics(final ServerId serverId) {
        return map.get(serverId);
    }

    private String ensureValidValue(final String value) {
        if (containsQuotableCharacter(value)) {
            return ObjectName.quote(value);
        } else {
            return value;
        }
    }

    private boolean containsQuotableCharacter(final String value) {
        if (value == null || value.length() == 0) {
            return false;
        }
        List<String> quoteableCharacters = asList(",", ":", "?", "*", "=", "\"", "\\", "\n");
        for (String quotable : quoteableCharacters) {
            if (value.contains(quotable)) {
                return true;
            }
        }
        return false;
    }
}
