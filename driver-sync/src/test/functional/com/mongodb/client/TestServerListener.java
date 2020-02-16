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

package com.mongodb.client;

import com.mongodb.connection.ServerType;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.event.ServerListener;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TestServerListener implements ServerListener {
    private final List<ServerDescriptionChangedEvent> events = new ArrayList<>();
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private volatile int waitingForEventCount;
    private volatile ServerType waitingForServerType;

    @Override
    public void serverDescriptionChanged(final ServerDescriptionChangedEvent event) {
       addEvent(event);
    }

    public List<ServerDescriptionChangedEvent> getEvents() {
        lock.lock();
        try {
            return new ArrayList<>(events);
        } finally {
            lock.unlock();
        }
    }

    public void waitForEvent(final ServerType serverType, final int count, final long time, final TimeUnit unit)
            throws InterruptedException, TimeoutException {
        lock.lock();
        try {
            waitingForServerType = serverType;
            waitingForEventCount = count;
            if (containsEvent(serverType, count)) {
                return;
            }
            if (!condition.await(time, unit)) {
                throw new TimeoutException("Timed out waiting for " + count + " server description changed events with serverType "
                        + serverType);
            }
        } finally {
            waitingForServerType = null;
            lock.unlock();
        }
    }

    public int countEvents(final ServerType serverType) {
        int eventCount = 0;
        for (ServerDescriptionChangedEvent event : getEvents()) {
            if (event.getNewDescription().getType() == serverType) {
                eventCount++;
            }
        }
        return eventCount;
    }

    private void addEvent(final ServerDescriptionChangedEvent event) {
        lock.lock();
        try {
            events.add(event);
            if (waitingForServerType != null && containsEvent(waitingForServerType, waitingForEventCount)) {
                condition.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    private boolean containsEvent(final ServerType serverType, final int expectedEventCount) {
        return countEvents(serverType) >= expectedEventCount;
    }
}
