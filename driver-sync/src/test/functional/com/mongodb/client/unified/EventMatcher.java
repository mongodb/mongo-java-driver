/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.unified;

import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

final class EventMatcher {

    public static List<CommandEvent> getExpectedEvents(final BsonArray expectedEventDocuments) {
        List<CommandEvent> expectedEvents = new ArrayList<CommandEvent>(expectedEventDocuments.size());
        for (BsonValue expectedEventDocument : expectedEventDocuments) {
            BsonDocument curExpectedEventDocument = expectedEventDocument.asDocument();
            String eventType = curExpectedEventDocument.getFirstKey();
            BsonDocument eventDescriptionDocument = curExpectedEventDocument.getDocument(eventType);
            String commandName = eventDescriptionDocument.getString("commandName", new BsonString("")).getValue();
            CommandEvent commandEvent;
            switch (eventType) {
                case "commandStartedEvent":
                    commandEvent = new CommandStartedEvent(1, null,
                            eventDescriptionDocument.getString("databaseName").getValue(), commandName,
                            eventDescriptionDocument.getDocument("command"));
                    break;
                case "commandSucceededEvent":
                    BsonDocument replyDocument = eventDescriptionDocument.get("reply").asDocument();
                    commandEvent = new CommandSucceededEvent(1, null, commandName, replyDocument, 1);

                    break;
                case "commandFailedEvent":
                    commandEvent = new CommandFailedEvent(1, null, commandName, 1, null);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported command event type: " + eventType);
            }
            expectedEvents.add(commandEvent);
        }
        return expectedEvents;
    }

    public static void assertEventsEquality(final List<CommandEvent> expectedEvents, final List<CommandEvent> events) {
        assertEquals(expectedEvents.size(), events.size());

        for (int i = 0; i < events.size(); i++) {
            CommandEvent actual = events.get(i);
            CommandEvent expected = expectedEvents.get(i);

            assertEquals(expected.getClass(), actual.getClass());
            assertEquals(expected.getCommandName(), actual.getCommandName());

            if (actual.getClass().equals(CommandStartedEvent.class)) {
                CommandStartedEvent expectedCommandStartedEvent = (CommandStartedEvent) expected;
                CommandStartedEvent actualCommandStartedEvent = (CommandStartedEvent) actual;

                assertEquals(expectedCommandStartedEvent.getDatabaseName(), actualCommandStartedEvent.getDatabaseName());
                ValueMatcher.assertValuesMatch(expectedCommandStartedEvent.getCommand(), actualCommandStartedEvent.getCommand());

            } else if (actual.getClass().equals(CommandSucceededEvent.class)) {
                CommandSucceededEvent actualCommandSucceededEvent = (CommandSucceededEvent) actual;
                CommandSucceededEvent expectedCommandSucceededEvent = (CommandSucceededEvent) expected;

                if (expectedCommandSucceededEvent.getResponse() == null) {
                    assertNull(actualCommandSucceededEvent.getResponse());
                } else {
                    ValueMatcher.assertValuesMatch(expectedCommandSucceededEvent.getResponse(), actualCommandSucceededEvent.getResponse());
                }
            } else if (!actual.getClass().equals(CommandFailedEvent.class)) {
                throw new UnsupportedOperationException("Unsupported event type: " + actual.getClass());
            }
        }
    }

    private EventMatcher() {
    }
}
