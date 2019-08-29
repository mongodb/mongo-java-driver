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

import com.mongodb.ServerAddress;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.event.ConnectionAddedEvent;
import com.mongodb.event.ConnectionCheckedInEvent;
import com.mongodb.event.ConnectionCheckedOutEvent;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.ConnectionPoolOpenedEvent;
import com.mongodb.event.ConnectionPoolWaitQueueEnteredEvent;
import com.mongodb.event.ConnectionPoolWaitQueueExitedEvent;
import com.mongodb.event.ConnectionRemovedEvent;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * An MBean implementation for connection pool statistics.
 */
final class ConnectionPoolStatistics implements ConnectionPoolListener, ConnectionPoolStatisticsMBean {
    private final ServerAddress serverAddress;
    private final ConnectionPoolSettings settings;
    private final AtomicInteger size = new AtomicInteger();
    private final AtomicInteger checkedOutCount = new AtomicInteger();
    private final AtomicInteger waitQueueSize = new AtomicInteger();

    ConnectionPoolStatistics(final ConnectionPoolOpenedEvent event) {
        serverAddress = event.getServerId().getAddress();
        settings = event.getSettings();
    }

    @Override
    public String getHost() {
        return serverAddress.getHost();
    }

    @Override
    public int getPort() {
        return serverAddress.getPort();
    }

    @Override
    public int getMinSize() {
        return settings.getMinSize();
    }

    @Override
    public int getMaxSize() {
        return settings.getMaxSize();
    }

    @Override
    public int getSize() {
        return size.get();
    }

    @Override
    public int getCheckedOutCount() {
        return checkedOutCount.get();
    }

    @Override
    public int getWaitQueueSize() {
        return waitQueueSize.get();
    }

    @Override
    public void connectionCheckedOut(final ConnectionCheckedOutEvent event) {
        checkedOutCount.incrementAndGet();
    }

    @Override
    public void connectionCheckedIn(final ConnectionCheckedInEvent event) {
        checkedOutCount.decrementAndGet();
    }

    @Override
    public void connectionAdded(final ConnectionAddedEvent event) {
        size.incrementAndGet();
    }

    @Override
    public void connectionRemoved(final ConnectionRemovedEvent event) {
        size.decrementAndGet();
    }

    @Override
    public void waitQueueEntered(final ConnectionPoolWaitQueueEnteredEvent event) {
        waitQueueSize.incrementAndGet();
    }

    @Override
    public void waitQueueExited(final ConnectionPoolWaitQueueExitedEvent event) {
        waitQueueSize.decrementAndGet();
    }
}
