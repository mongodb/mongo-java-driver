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

package com.mongodb.event;

import com.mongodb.annotations.ThreadSafe;
import com.mongodb.lang.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.StreamSupport.stream;

@ThreadSafe
public final class TestServerMonitorListener implements ServerMonitorListener {
    private final Set<Class<?>> listenableEventTypes;
    private final Lock lock;
    private final Condition condition;
    private final List<Object> events;
    private final List<Object> allEvents;

    public TestServerMonitorListener(final Iterable<String> listenableEventTypes) {
        this.listenableEventTypes = unmodifiableSet(stream(listenableEventTypes.spliterator(), false)
                .map(TestServerMonitorListener::nullableEventType)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
        lock = new ReentrantLock();
        condition = lock.newCondition();
        events = new ArrayList<>();
        allEvents = new ArrayList<>();
    }

    public void serverHearbeatStarted(final ServerHeartbeatStartedEvent event) {
        register(event);
    }

    public void serverHeartbeatSucceeded(final ServerHeartbeatSucceededEvent event) {
        register(event);
    }

    public void serverHeartbeatFailed(final ServerHeartbeatFailedEvent event) {
        register(event);
    }

    public <T> void waitForEvents(final Class<T> type, final Predicate<? super T> matcher, final int count, final Duration duration)
            throws InterruptedException, TimeoutException {
        assertTrue(listenable(type));
        long remainingNanos = duration.toNanos();
        lock.lock();
        try {
            long observedCount = countEvents(type, matcher);
            while (observedCount < count) {
                if (remainingNanos <= 0) {
                    throw new TimeoutException(String.format("Timed out waiting for %d %s events. The observed count is %d. Seen: %s",
                            count, type.getSimpleName(), observedCount, allEvents));
                }
                remainingNanos = condition.awaitNanos(remainingNanos);
                observedCount = countEvents(type, matcher);
            }
        } finally {
            lock.unlock();
        }
    }

    public <T> long countEvents(final Class<T> type, final Predicate<? super T> matcher) {
        assertTrue(listenable(type));
        lock.lock();
        try {
            return events.stream()
                    .filter(type::isInstance)
                    .map(type::cast)
                    .filter(matcher)
                    .count();
        } finally {
            lock.unlock();
        }
    }

    public List<Object> getEvents() {
        lock.lock();
        try {
            return new ArrayList<>(events);
        } finally {
            lock.unlock();
        }
    }

    public static Class<?> eventType(final String eventType) {
        return assertNotNull(nullableEventType(eventType));
    }

    @Nullable
    private static Class<?> nullableEventType(final String eventType) {
        switch (eventType) {
            case "serverHeartbeatStartedEvent": {
                return ServerHeartbeatStartedEvent.class;
            }
            case "serverHeartbeatSucceededEvent": {
                return ServerHeartbeatSucceededEvent.class;
            }
            case "serverHeartbeatFailedEvent": {
                return ServerHeartbeatFailedEvent.class;
            }
            default: {
                return null;
            }
        }
    }

    private boolean listenable(final Class<?> eventType) {
        return listenableEventTypes.contains(eventType);
    }

    private void register(final Object event) {
        allEvents.add(event);
        if (!listenable(event.getClass())) {
            return;
        }
        lock.lock();
        try {
            events.add(event);
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
