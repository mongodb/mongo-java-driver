/*
 * Copyright 2015 MongoDB, Inc.
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
 *
 *
 */

package com.mongodb.connection;

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
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestCommandListener implements CommandListener {
    private final List<CommandEvent> events = new ArrayList<CommandEvent>();
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

    public void reset() {
        events.clear();
    }

    public List<CommandEvent> getEvents() {
        return events;
    }

    @Override
    public void commandStarted(final CommandStartedEvent event) {
        events.add(new CommandStartedEvent(event.getRequestId(), event.getConnectionDescription(), event.getDatabaseName(),
                                           event.getCommandName(),
                                           event.getCommand() == null ? null : getWritableClone(event.getCommand())));
    }

    private BsonDocument getWritableClone(final BsonDocument original) {
        BsonDocument clone = new BsonDocument();
        BsonDocumentWriter writer = new BsonDocumentWriter(clone);
        new BsonDocumentCodec(CODEC_REGISTRY_HACK).encode(writer, original, EncoderContext.builder().build());
        return clone;
    }

    @Override
    public void commandSucceeded(final CommandSucceededEvent event) {
        events.add(new CommandSucceededEvent(event.getRequestId(), event.getConnectionDescription(), event.getCommandName(),
                                             event.getResponse() == null ? null : event.getResponse().clone(),
                                             event.getElapsedTime(TimeUnit.NANOSECONDS)));
    }

    @Override
    public void commandFailed(final CommandFailedEvent event) {
        events.add(event);
    }

    public void eventsWereDelivered(final List<CommandEvent> expectedEvents) {
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
    }

    public void eventWasDelivered(final CommandEvent expectedEvent, final int index) {
        assertTrue(events.size() > index);
        assertEventEquivalence(events.get(index), expectedEvent);
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
