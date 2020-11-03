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

import java.util.List;

import static org.junit.Assert.assertEquals;

final class EventMatcher {
    private final ValueMatcher valueMatcher;

    EventMatcher(final ValueMatcher valueMatcher) {
        this.valueMatcher = valueMatcher;
    }

    public void assertEventsEquality(final BsonArray expectedEventDocuments, final List<CommandEvent> events) {
        assertEquals("Number of events must be the same", expectedEventDocuments.size(), events.size());

        for (int i = 0; i < events.size(); i++) {
            CommandEvent actual = events.get(i);
            String eventType = expectedEventDocuments.get(i).asDocument().getFirstKey();
            BsonDocument expected = expectedEventDocuments.get(i).asDocument().getDocument(eventType);
            if (expected.containsKey("commandName")) {
                assertEquals("Command names must be equal", expected.getString("commandName").getValue(), actual.getCommandName());
            }

            if (actual.getClass().equals(CommandStartedEvent.class)) {
                assertEquals(eventType, "commandStartedEvent");
                CommandStartedEvent actualCommandStartedEvent = (CommandStartedEvent) actual;

                if (expected.containsKey("databaseName")) {
                    assertEquals(expected.getString("databaseName").getValue(), actualCommandStartedEvent.getDatabaseName());
                }
                if (expected.containsKey("command")) {
                    valueMatcher.assertValuesMatch(expected.getDocument("command"), actualCommandStartedEvent.getCommand());
                }
            } else if (actual.getClass().equals(CommandSucceededEvent.class)) {
                assertEquals(eventType, "commandSucceededEvent");
                CommandSucceededEvent actualCommandSucceededEvent = (CommandSucceededEvent) actual;

                if (expected.containsKey("reply")) {
                    valueMatcher.assertValuesMatch(expected.getDocument("reply"), actualCommandSucceededEvent.getResponse());
                }
            } else if (actual.getClass().equals(CommandFailedEvent.class)) {
                assertEquals(eventType, "commandFailedEvent");
            } else {
                throw new UnsupportedOperationException("Unsupported event type: " + actual.getClass());
            }
        }
    }
}
