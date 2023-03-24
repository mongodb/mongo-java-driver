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

import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.mongodb.ClusterFixture.TIMEOUT;
import static com.mongodb.internal.connection.InternalStreamConnection.getSecuritySensitiveCommands;
import static com.mongodb.internal.connection.InternalStreamConnection.getSecuritySensitiveHelloCommands;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestCommandListener implements CommandListener {
    private final List<String> eventTypes;
    private final List<String> ignoredCommandMonitoringEvents;
    private final List<CommandEvent> events = new ArrayList<>();
    private final Lock lock = new ReentrantLock();
    private final Condition commandCompletedCondition = lock.newCondition();
    private final boolean observeSensitiveCommands;
    private boolean ignoreNextSucceededOrFailedEvent;
    private static final CodecRegistry CODEC_REGISTRY_HACK;

    static {
        CODEC_REGISTRY_HACK = CodecRegistries.fromProviders(new BsonValueCodecProvider(),
                new CodecProvider() {
                    @Override
                    @SuppressWarnings("unchecked")
                    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
                        // Use BsonDocumentCodec even for a private sub-class of BsonDocument
                        if (BsonDocument.class.isAssignableFrom(clazz)) {
                            return (Codec<T>) new BsonDocumentCodec(registry);
                        }
                        return null;
                    }
                });
    }

    public TestCommandListener() {
        this(Arrays.asList("commandStartedEvent", "commandSucceededEvent", "commandFailedEvent"), emptyList());
    }

    public TestCommandListener(final List<String> eventTypes, final List<String> ignoredCommandMonitoringEvents) {
        this(eventTypes, ignoredCommandMonitoringEvents, true);
    }

    public TestCommandListener(final List<String> eventTypes, final List<String> ignoredCommandMonitoringEvents,
            final boolean observeSensitiveCommands) {
        this.eventTypes = eventTypes;
        this.ignoredCommandMonitoringEvents = ignoredCommandMonitoringEvents;
        this.observeSensitiveCommands = observeSensitiveCommands;
    }

    public void reset() {
        lock.lock();
        try {
            events.clear();
        } finally {
            lock.unlock();
        }
    }

    public List<CommandEvent> getEvents() {
        lock.lock();
        try {
            return new ArrayList<>(events);
        } finally {
            lock.unlock();
        }
    }

    public CommandStartedEvent getCommandStartedEvent(final String commandName) {
        for (CommandEvent event : getCommandStartedEvents()) {
            if (event instanceof CommandStartedEvent) {
                CommandStartedEvent startedEvent = (CommandStartedEvent) event;
                if (startedEvent.getCommandName().equals(commandName)) {
                    return startedEvent;
                }
            }
        }
        throw new IllegalArgumentException(commandName + " not found in command started event list");
    }

    public CommandSucceededEvent getCommandSucceededEvent(final String commandName) {
        for (CommandEvent event : getEvents()) {
            if (event instanceof CommandSucceededEvent) {
                CommandSucceededEvent succeededEvent = (CommandSucceededEvent) event;
                if (succeededEvent.getCommandName().equals(commandName)) {
                    return succeededEvent;
                }
            }
        }
        throw new IllegalArgumentException(commandName + " not found in command succeeded event list");
    }

    public CommandFailedEvent getCommandFailedEvent(final String commandName) {
        return getEvents()
                .stream()
                .filter(e -> e instanceof CommandFailedEvent)
                .filter(e -> e.getCommandName().equals(commandName))
                .map(e -> (CommandFailedEvent) e)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(commandName + " not found in command failed event list"));
    }

    public List<CommandEvent> getCommandStartedEvents() {
        return getCommandStartedEvents(Integer.MAX_VALUE);
    }

    private List<CommandEvent> getCommandStartedEvents(final int maxEvents) {
        lock.lock();
        try {
            List<CommandEvent> commandStartedEvents = new ArrayList<>();
            for (CommandEvent cur : getEvents()) {
                if (cur instanceof CommandStartedEvent) {
                    commandStartedEvents.add(cur);
                }
                if (commandStartedEvents.size() == maxEvents) {
                    break;
                }
            }
            return commandStartedEvents;
        } finally {
            lock.unlock();
        }
    }

    public List<CommandEvent> waitForStartedEvents(final int numEvents) {
        lock.lock();
        try {
            while (!hasCompletedEvents(numEvents)) {
                try {
                    if (!commandCompletedCondition.await(TIMEOUT, TimeUnit.SECONDS)) {
                        throw new MongoTimeoutException("Timeout waiting for event");
                    }
                } catch (InterruptedException e) {
                    throw new MongoInterruptedException("Interrupted waiting for event", e);
                }
            }
            return getCommandStartedEvents(numEvents);
        } finally {
            lock.unlock();
        }
    }

    public void waitForFirstCommandCompletion() {
        lock.lock();
        try {
            while (!hasCompletedEvents(1)) {
                try {
                    if (!commandCompletedCondition.await(TIMEOUT, TimeUnit.SECONDS)) {
                        throw new MongoTimeoutException("Timeout waiting for event");
                    }
                } catch (InterruptedException e) {
                    throw new MongoInterruptedException("Interrupted waiting for event", e);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private boolean hasCompletedEvents(final int numEventsCompleted) {
        int count = 0;
        for (CommandEvent event : events) {
            if (event instanceof CommandSucceededEvent || event instanceof CommandFailedEvent) {
                count++;
            }
        }
        return count >= numEventsCompleted;
    }


    @Override
    public void commandStarted(final CommandStartedEvent event) {
        if (!eventTypes.contains("commandStartedEvent") || ignoredCommandMonitoringEvents.contains(event.getCommandName())) {
            return;
        }
        else if (!observeSensitiveCommands) {
            if (getSecuritySensitiveCommands().contains(event.getCommandName())) {
                return;
            } else if (getSecuritySensitiveHelloCommands().contains(event.getCommandName()) && event.getCommand().isEmpty()) {
                ignoreNextSucceededOrFailedEvent = true;
                return;
            }
        }
        lock.lock();
        try {
            events.add(new CommandStartedEvent(event.getRequestContext(), event.getOperationId(), event.getRequestId(),
                    event.getConnectionDescription(), event.getDatabaseName(), event.getCommandName(),
                    event.getCommand() == null ? null : getWritableClone(event.getCommand())));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commandSucceeded(final CommandSucceededEvent event) {
        if (!eventTypes.contains("commandSucceededEvent") || ignoredCommandMonitoringEvents.contains(event.getCommandName())) {
            return;
        }
        else if (!observeSensitiveCommands) {
            if (getSecuritySensitiveCommands().contains(event.getCommandName())) {
                return;
            } else if (getSecuritySensitiveHelloCommands().contains(event.getCommandName()) && ignoreNextSucceededOrFailedEvent) {
                ignoreNextSucceededOrFailedEvent = false;
                return;
            }
        }
        lock.lock();
        try {
            events.add(new CommandSucceededEvent(event.getRequestContext(), event.getOperationId(), event.getRequestId(),
                    event.getConnectionDescription(), event.getCommandName(),
                    event.getResponse() == null ? null : event.getResponse().clone(),
                    event.getElapsedTime(TimeUnit.NANOSECONDS)));
            commandCompletedCondition.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commandFailed(final CommandFailedEvent event) {
        if (!eventTypes.contains("commandFailedEvent") || ignoredCommandMonitoringEvents.contains(event.getCommandName())) {
            return;
        }
        else if (!observeSensitiveCommands) {
            if (getSecuritySensitiveCommands().contains(event.getCommandName())) {
                return;
            } else if (getSecuritySensitiveHelloCommands().contains(event.getCommandName()) && ignoreNextSucceededOrFailedEvent) {
                ignoreNextSucceededOrFailedEvent = false;
                return;
            }
        }
        lock.lock();
        try {
            events.add(event);
            commandCompletedCondition.signal();
        } finally {
            lock.unlock();
        }
    }

    public void eventsWereDelivered(final List<CommandEvent> expectedEvents) {
        lock.lock();
        try {
            assertEquals(expectedEvents.size(), events.size());

            int currentlyExpectedRequestId = 0;
            for (int i = 0; i < events.size(); i++) {
                CommandEvent actual = events.get(i);
                CommandEvent expected = expectedEvents.get(i);

                if (actual instanceof CommandStartedEvent) {
                    currentlyExpectedRequestId = actual.getRequestId();
                } else {
                    assertEquals(currentlyExpectedRequestId, actual.getRequestId());
                }

                assertEventEquivalence(actual, expected);
            }
        } finally {
            lock.unlock();
        }
    }

    public void eventWasDelivered(final CommandEvent expectedEvent, final int index) {
        lock.lock();
        try {
            assertTrue(events.size() > index);
            assertEventEquivalence(events.get(index), expectedEvent);
        } finally {
            lock.unlock();
        }
    }

    private BsonDocument getWritableClone(final BsonDocument original) {
        BsonDocument clone = new BsonDocument();
        BsonDocumentWriter writer = new BsonDocumentWriter(clone);
        new BsonDocumentCodec(CODEC_REGISTRY_HACK).encode(writer, original, EncoderContext.builder().build());
        return clone;
    }

    private void assertEventEquivalence(final CommandEvent actual, final CommandEvent expected) {
        assertEquals(expected.getClass(), actual.getClass());

        assertEquals(expected.getConnectionDescription(), actual.getConnectionDescription());

        assertEquals(expected.getCommandName(), actual.getCommandName());

        if (actual.getClass().equals(CommandStartedEvent.class)) {
            assertEquivalence((CommandStartedEvent) actual, (CommandStartedEvent) expected);
        } else if (actual.getClass().equals(CommandSucceededEvent.class)) {
            assertEquivalence((CommandSucceededEvent) actual, (CommandSucceededEvent) expected);
        } else if (actual.getClass().equals(CommandFailedEvent.class)) {
            assertEquivalence((CommandFailedEvent) actual, (CommandFailedEvent) expected);
        } else {
            throw new UnsupportedOperationException("Unsupported event type: " + actual.getClass());
        }
    }

    private void assertEquivalence(final CommandFailedEvent actual, final CommandFailedEvent expected) {
        assertEquals(expected.getThrowable(), actual.getThrowable());
    }

    private void assertEquivalence(final CommandSucceededEvent actual, final CommandSucceededEvent expected) {
        if (actual.getResponse() == null) {
            assertNull(expected.getResponse());
        } else {
            // ignore extra elements in the actual response
            assertTrue("Expected response contains elements not in the actual response",
                    massageResponse(actual.getResponse()).entrySet()
                            .containsAll(massageResponse(expected.getResponse()).entrySet()));
        }
    }

    private BsonDocument massageResponse(final BsonDocument response) {
        BsonDocument massagedResponse = getWritableClone(response);
        // massage numbers to the same BSON type
        if (massagedResponse.containsKey("ok")) {
            massagedResponse.put("ok", new BsonDouble(response.getNumber("ok").doubleValue()));
        }
        if (massagedResponse.containsKey("n")) {
            massagedResponse.put("n", new BsonInt32(response.getNumber("n").intValue()));
        }
        return massagedResponse;
    }

    private void assertEquivalence(final CommandStartedEvent actual, final CommandStartedEvent expected) {
        assertEquals(expected.getDatabaseName(), actual.getDatabaseName());
        assertEquals(expected.getCommand(), actual.getCommand());
    }
}
