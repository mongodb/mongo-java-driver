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

import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ServerClosedEvent;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerOpeningEvent;
import com.mongodb.lang.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.Locks.withLock;

public class TestServerListener implements ServerListener {
    @Nullable
    private volatile ServerOpeningEvent serverOpeningEvent;
    @Nullable
    private volatile ServerClosedEvent serverClosedEvent;
    private final List<ServerDescriptionChangedEvent> serverDescriptionChangedEvents = new ArrayList<>();
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

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
        withLock(lock, () -> {
            serverDescriptionChangedEvents.add(event);
            condition.signalAll();
        });
    }

    @Nullable
    public ServerOpeningEvent getServerOpeningEvent() {
        return serverOpeningEvent;
    }

    @Nullable
    public ServerClosedEvent getServerClosedEvent() {
        return serverClosedEvent;
    }

    public List<ServerDescriptionChangedEvent> getServerDescriptionChangedEvents() {
        return withLock(lock, () -> new ArrayList<>(serverDescriptionChangedEvents));
    }

    public void waitForServerDescriptionChangedEvents(
            final Predicate<ServerDescriptionChangedEvent> matcher, final int count, final Duration duration)
            throws InterruptedException, TimeoutException {
        if (count <= 0) {
            throw new IllegalArgumentException();
        }
        long nanosRemaining = duration.toNanos();
        lock.lock();
        try {
            long observedCount = unguardedCount(matcher);
            while (observedCount < count) {
                if (nanosRemaining <= 0) {
                    throw new TimeoutException(String.format("Timed out waiting for %d %s events. The observed count is %d.",
                            count, ClusterDescriptionChangedEvent.class.getSimpleName(), observedCount));
                }
                nanosRemaining = condition.awaitNanos(nanosRemaining);
                observedCount = unguardedCount(matcher);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Must be guarded by {@link #lock}.
     */
    private long unguardedCount(final Predicate<ServerDescriptionChangedEvent> matcher) {
        return serverDescriptionChangedEvents.stream().filter(matcher).count();
    }
}
