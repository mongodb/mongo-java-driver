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

package com.mongodb.internal.connection;

import com.mongodb.event.ServerClosedEvent;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerOpeningEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import static com.mongodb.assertions.Assertions.notNull;

public class TestServerListener implements ServerListener {
    private ServerOpeningEvent serverOpeningEvent;
    private ServerClosedEvent serverClosedEvent;
    private final List<ServerDescriptionChangedEvent> serverDescriptionChangedEvents = new ArrayList<ServerDescriptionChangedEvent>();
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private volatile int waitingForEventCount;
    private Predicate<ServerDescriptionChangedEvent> waitingForEventMatcher;

    @Override
    public void serverOpening(final ServerOpeningEvent event) {
        serverOpeningEvent = event;
    }

    @Override
    public void serverClosed(final ServerClosedEvent event) {
        serverClosedEvent = event;
    }

    @Override
    public void serverDescriptionChanged(final ServerDescriptionChangedEvent event) {
        notNull("event", event);
        lock.lock();
        try {
            serverDescriptionChangedEvents.add(event);
            if (waitingForEventCount != 0 && containsEvents()) {
                condition.signalAll();
            }

        } finally {
            lock.unlock();
        }
    }

    public ServerOpeningEvent getServerOpeningEvent() {
        return serverOpeningEvent;
    }

    public ServerClosedEvent getServerClosedEvent() {
        return serverClosedEvent;
    }

    public List<ServerDescriptionChangedEvent> getServerDescriptionChangedEvents() {
        return serverDescriptionChangedEvents;
    }

    public void waitForServerDescriptionChangedEvent(final Predicate<ServerDescriptionChangedEvent> matcher, final int count,
            final int time, final TimeUnit unit) throws InterruptedException, TimeoutException {
        if (count <= 0) {
            throw new IllegalArgumentException();
        }
        lock.lock();
        try {
            if (waitingForEventCount != 0) {
                throw new IllegalStateException("Already waiting for events");
            }
            waitingForEventCount = count;
            waitingForEventMatcher = matcher;
            if (containsEvents()) {
                return;
            }
            if (!condition.await(time, unit)) {
                throw new TimeoutException("Timed out waiting for " + count + " ServerDescriptionChangedEvent events. "
                        + "The count after timing out is " + countEvents());
            }
        } finally {
            waitingForEventCount = 0;
            waitingForEventMatcher = null;
            lock.unlock();
        }
    }

    private long countEvents() {
        return serverDescriptionChangedEvents.stream().filter(waitingForEventMatcher).count();
    }

    private boolean containsEvents() {
        return countEvents() >= waitingForEventCount;
    }
}
