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

import com.mongodb.MongoTimeoutException;
import com.mongodb.client.TestListener;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.lang.Nullable;
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.mongodb.ClusterFixture.TIMEOUT;
import static com.mongodb.internal.connection.InternalStreamConnection.getSecuritySensitiveCommands;
import static com.mongodb.internal.connection.InternalStreamConnection.getSecuritySensitiveHelloCommands;
import static com.mongodb.internal.thread.InterruptionUtil.interruptAndCreateMongoInterruptedException;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestCommandListener implements CommandListener {
    private final List<String> eventTypes;
    private final List<String> ignoredCommandMonitoringEvents;
    private final List<CommandEvent> events = new ArrayList<>();
    @Nullable
    private final TestListener listener;
    private final Lock lock = new ReentrantLock();
    private final Condition commandCompletedCondition = lock.newCondition();
    private final Condition commandAnyEventCondition = lock.newCondition();
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

    /**
     * When a test listener is set, this command listener will send string events to the
     * test listener in the form {@code "<command name> <eventType>"}, where the event
     * type will be lowercase and will omit the terms "command" and "event".
     * For example: {@code "saslContinue succeeded"}.
     *
     * @see InternalMongoClientSettings.Builder#recordEverything(boolean)
     * @param listener the test listener
     */
    public TestCommandListener(final TestListener listener) {
        this(Arrays.asList("commandStartedEvent", "commandSucceededEvent", "commandFailedEvent"), emptyList(), true, listener);
    }

    public TestCommandListener() {
        this(Arrays.asList("commandStartedEvent", "commandSucceededEvent", "commandFailedEvent"), emptyList());
    }

    public TestCommandListener(final List<String> eventTypes, final List<String> ignoredCommandMonitoringEvents) {
        this(eventTypes, ignoredCommandMonitoringEvents, true, null);
    }

    public TestCommandListener(final List<String> eventTypes, final List<String> ignoredCommandMonitoringEvents,
            final boolean observeSensitiveCommands, @Nullable final TestListener listener) {
        this.eventTypes = eventTypes;
        this.ignoredCommandMonitoringEvents = ignoredCommandMonitoringEvents;
        this.observeSensitiveCommands = observeSensitiveCommands;
        this.listener = listener;
    }



    public void reset() {
        lock.lock();
        try {
            events.clear();
            if (listener != null) {
                listener.clear();
            }
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

    private void addEvent(final CommandEvent c) {
        events.add(c);
        String className = c.getClass().getSimpleName()
                .replace("Command", "")
                .replace("Event", "")
                .toLowerCase();
        // example: "saslContinue succeeded"
        if (listener != null) {
            listener.add(c.getCommandName() + " " + className);
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

    public List<CommandFailedEvent> getCommandFailedEvents() {
        return getEvents(CommandFailedEvent.class, Integer.MAX_VALUE);
    }

    public List<CommandFailedEvent> getCommandFailedEvents(final String commandName) {
        return getEvents(CommandFailedEvent.class,
                commandEvent -> commandEvent.getCommandName().equals(commandName),
                Integer.MAX_VALUE);
    }

    public List<CommandStartedEvent> getCommandStartedEvents() {
        return getEvents(CommandStartedEvent.class, Integer.MAX_VALUE);
    }

    public List<CommandStartedEvent> getCommandStartedEvents(final String commandName) {
        return getEvents(CommandStartedEvent.class,
                commandEvent -> commandEvent.getCommandName().equals(commandName),
                Integer.MAX_VALUE);
    }

    public List<CommandSucceededEvent> getCommandSucceededEvents() {
        return getEvents(CommandSucceededEvent.class, Integer.MAX_VALUE);
    }

    private <T extends CommandEvent> List<T> getEvents(final Class<T> type, final int maxEvents) {
      return getEvents(type, e -> true, maxEvents);
    }

    private <T extends CommandEvent> List<T> getEvents(final Class<T> type,
                                                       final Predicate<? super CommandEvent> filter,
                                                       final int maxEvents) {
        lock.lock();
        try {
            return getEvents().stream()
                    .filter(e -> e.getClass() == type)
                    .filter(filter)
                    .map(type::cast)
                    .limit(maxEvents).collect(Collectors.toList());
        } finally {
            lock.unlock();
        }
    }

    private <T extends CommandEvent> long getEventCount(final Class<T> eventClass, final Predicate<T> matcher) {
        return getEvents().stream()
                .filter(eventClass::isInstance)
                .map(eventClass::cast)
                .filter(matcher)
                .count();
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
                    throw interruptAndCreateMongoInterruptedException("Interrupted waiting for event", e);
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
            addEvent(new CommandStartedEvent(event.getRequestContext(), event.getOperationId(), event.getRequestId(),
                    event.getConnectionDescription(), event.getDatabaseName(), event.getCommandName(),
                    event.getCommand() == null ? null : getWritableClone(event.getCommand())));
            commandAnyEventCondition.signal();
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
            addEvent(new CommandSucceededEvent(event.getRequestContext(), event.getOperationId(), event.getRequestId(),
                    event.getConnectionDescription(), event.getDatabaseName(), event.getCommandName(),
                    event.getResponse() == null ? null : event.getResponse().clone(),
                    event.getElapsedTime(TimeUnit.NANOSECONDS)));
            commandCompletedCondition.signal();
            commandAnyEventCondition.signal();
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
            addEvent(event);
            commandCompletedCondition.signal();
            commandAnyEventCondition.signal();
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

    public <T extends CommandEvent> void waitForEvents(final Class<T> eventClass, final Predicate<T> matcher, final int count)
            throws TimeoutException {
        lock.lock();
        try {
            while (getEventCount(eventClass, matcher) < count) {
                try {
                    if (!commandAnyEventCondition.await(TIMEOUT, TimeUnit.SECONDS)) {
                        throw new MongoTimeoutException("Timeout waiting for command event");
                    }
                } catch (InterruptedException e) {
                    throw interruptAndCreateMongoInterruptedException("Interrupted waiting for event", e);
                }
            }
        } finally {
            lock.unlock();
        }
    }
}
