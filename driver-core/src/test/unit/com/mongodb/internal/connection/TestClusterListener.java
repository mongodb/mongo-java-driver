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

import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterOpeningEvent;
import com.mongodb.lang.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.Locks.withLock;

public final class TestClusterListener implements ClusterListener {
    @Nullable
    private volatile ClusterOpeningEvent clusterOpeningEvent;
    @Nullable
    private volatile ClusterClosedEvent clusterClosingEvent;
    private final ArrayList<ClusterDescriptionChangedEvent> clusterDescriptionChangedEvents = new ArrayList<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition newClusterDescriptionChangedEventCondition = lock.newCondition();
    private final CountDownLatch closedLatch = new CountDownLatch(1);

    @Override
    public void clusterOpening(final ClusterOpeningEvent event) {
        isTrue("clusterOpeningEvent is null", clusterOpeningEvent == null);
        clusterOpeningEvent = event;
    }

    @Override
    public void clusterClosed(final ClusterClosedEvent event) {
        isTrue("clusterClosingEvent is null", clusterClosingEvent == null);
        closedLatch.countDown();
        clusterClosingEvent = event;
    }

    @Override
    public void clusterDescriptionChanged(final ClusterDescriptionChangedEvent event) {
        notNull("event", event);
        withLock(lock, () -> {
            clusterDescriptionChangedEvents.add(event);
            newClusterDescriptionChangedEventCondition.signalAll();
        });
    }

    @Nullable
    public ClusterOpeningEvent getClusterOpeningEvent() {
        return clusterOpeningEvent;
    }

    @Nullable
    public ClusterClosedEvent getClusterClosingEvent() {
        return clusterClosingEvent;
    }

    public List<ClusterDescriptionChangedEvent> getClusterDescriptionChangedEvents() {
        return withLock(lock, () -> new ArrayList<>(clusterDescriptionChangedEvents));
    }

    /**
     * Calling this method concurrently with {@link #waitForClusterDescriptionChangedEvents(Predicate, int, Duration)},
     * may result in {@link #waitForClusterDescriptionChangedEvents(Predicate, int, Duration)} not working as expected.
     */
    public void clearClusterDescriptionChangedEvents() {
        withLock(lock, clusterDescriptionChangedEvents::clear);
    }

    /**
     * Calling this method concurrently with {@link #clearClusterDescriptionChangedEvents()},
     * may result in {@link #waitForClusterDescriptionChangedEvents(Predicate, int, Duration)} not working as expected.
     */
    public void waitForClusterDescriptionChangedEvents(
            final Predicate<ClusterDescriptionChangedEvent> matcher, final int count, final Duration duration)
            throws InterruptedException, TimeoutException {
        long nanosRemaining = duration.toNanos();
        lock.lock();
        try {
            long observedCount = unguardedCount(matcher);
            while (observedCount < count) {
                if (nanosRemaining <= 0) {
                    throw new TimeoutException(String.format("Timed out waiting for %d %s events. The observed count is %d.",
                            count, ClusterDescriptionChangedEvent.class.getSimpleName(), observedCount));
                }
                nanosRemaining = newClusterDescriptionChangedEventCondition.awaitNanos(nanosRemaining);
                observedCount = unguardedCount(matcher);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Waits for the cluster to be closed, which is signaled by a {@link ClusterClosedEvent}.
     */
    public void waitForClusterClosedEvent(final Duration duration)
            throws InterruptedException, TimeoutException {
        boolean await = closedLatch.await(duration.toMillis(), TimeUnit.MILLISECONDS);
        if (!await) {
            throw new TimeoutException("Timed out waiting for cluster to close");
        }
    }

    /**
     * Must be guarded by {@link #lock}.
     */
    private long unguardedCount(final Predicate<ClusterDescriptionChangedEvent> matcher) {
        return clusterDescriptionChangedEvents.stream().filter(matcher).count();
    }
}
