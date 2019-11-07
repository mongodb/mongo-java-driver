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

package com.mongodb.client;

import com.mongodb.ClusterFixture;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.CrudTestHelper.replaceTypeAssertionWithActual;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public final class CommandMonitoringTestHelper {
    private static final CodecRegistry CODEC_REGISTRY_HACK = CodecRegistries.fromProviders(new BsonValueCodecProvider(),
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

    public static List<CommandEvent> getExpectedEvents(final BsonArray expectedEventDocuments, final String databaseName,
                                                       final BsonDocument operation) {
        List<CommandEvent> expectedEvents = new ArrayList<CommandEvent>(expectedEventDocuments.size());
        for (BsonValue expectedEventDocument : expectedEventDocuments) {
            BsonDocument curExpectedEventDocument = expectedEventDocument.asDocument();
            String eventType = curExpectedEventDocument.keySet().iterator().next();
            BsonDocument eventDescriptionDocument = curExpectedEventDocument.getDocument(eventType);
            CommandEvent commandEvent;
            String commandName = eventDescriptionDocument.getString("command_name", new BsonString("")).getValue();
            if (eventType.equals("command_started_event")) {
                BsonDocument commandDocument = eventDescriptionDocument.getDocument("command");
                String actualDatabaseName = databaseName;
                // If the spec test supplies a $db field in the command, then use that database.
                if (commandDocument.containsKey("$db")) {
                    actualDatabaseName = commandDocument.getString("$db").getValue();
                }
                else if (commandName.equals("commitTransaction") || commandName.equals("abortTransaction")) {
                    actualDatabaseName = "admin";
                } else if (commandName.equals("")) {
                    commandName = commandDocument.keySet().iterator().next();
                }
                // Not clear whether these global fields should be included, but also not clear how to efficiently exclude them
                if (ClusterFixture.serverVersionAtLeast(3, 6)) {
                    commandDocument.put("$db", new BsonString(actualDatabaseName));
                    if (operation != null && operation.containsKey("read_preference")) {
                        commandDocument.put("$readPreference", operation.getDocument("read_preference"));
                    }
                }
                commandEvent = new CommandStartedEvent(1, null, actualDatabaseName, commandName,
                        commandDocument);
            } else if (eventType.equals("command_succeeded_event")) {
                BsonDocument replyDocument = eventDescriptionDocument.get("reply").asDocument();
                commandEvent = new CommandSucceededEvent(1, null, commandName, replyDocument, 1);

            } else if (eventType.equals("command_failed_event")) {
                commandEvent = new CommandFailedEvent(1, null, commandName, 1, null);
            } else {
                throw new UnsupportedOperationException("Unsupported command event type: " + eventType);
            }
            expectedEvents.add(commandEvent);
        }
        return expectedEvents;
    }

    static boolean isWriteCommand(final String commandName) {
        return asList("insert", "update", "delete").contains(commandName);
    }

    public static void assertEventsEquality(final List<CommandEvent> expectedEvents, final List<CommandEvent> events) {
        assertEventsEquality(expectedEvents, events, null);
    }

    public static void assertEventsEquality(final List<CommandEvent> expectedEvents, final List<CommandEvent> events,
                                            @Nullable final Map<String, BsonDocument> lsidMap) {
        assertEquals(expectedEvents.size(), events.size());

        for (int i = 0; i < events.size(); i++) {
            CommandEvent actual = events.get(i);
            CommandEvent expected = expectedEvents.get(i);

            assertEquals(expected.getClass(), actual.getClass());
            assertEquals(expected.getCommandName().toLowerCase(), actual.getCommandName().toLowerCase());

            if (actual.getClass().equals(CommandStartedEvent.class)) {
                CommandStartedEvent expectedCommandStartedEvent = massageExpectedCommandStartedEvent((CommandStartedEvent) expected,
                        (CommandStartedEvent) actual, lsidMap);
                CommandStartedEvent actualCommandStartedEvent = massageActualCommandStartedEvent((CommandStartedEvent) actual,
                        lsidMap, expectedCommandStartedEvent);

                assertEquals(expectedCommandStartedEvent.getDatabaseName(), actualCommandStartedEvent.getDatabaseName());
                assertEquals(expectedCommandStartedEvent.getCommand(), actualCommandStartedEvent.getCommand());
                if (((CommandStartedEvent) expected).getCommand().containsKey("recoveryToken")) {
                    if (((CommandStartedEvent) expected).getCommand().get("recoveryToken").isNull()) {
                        assertFalse(((CommandStartedEvent) actual).getCommand().containsKey("recoveryToken"));
                    } else {
                        assertTrue(((CommandStartedEvent) actual).getCommand().containsKey("recoveryToken"));
                    }
                }

            } else if (actual.getClass().equals(CommandSucceededEvent.class)) {
                CommandSucceededEvent actualCommandSucceededEvent = massageActualCommandSucceededEvent((CommandSucceededEvent) actual);
                CommandSucceededEvent expectedCommandSucceededEvent = massageExpectedCommandSucceededEvent((CommandSucceededEvent)
                                                                                                           expected);

                assertEquals(expectedCommandSucceededEvent.getCommandName(), actualCommandSucceededEvent.getCommandName());
                assertTrue(actualCommandSucceededEvent.getElapsedTime(TimeUnit.NANOSECONDS) > 0);

                if (expectedCommandSucceededEvent.getResponse() == null) {
                    assertNull(actualCommandSucceededEvent.getResponse());
                } else {
                    assertTrue(String.format("\nExpected: %s\nActual:   %s",
                                             expectedCommandSucceededEvent.getResponse(),
                                             actualCommandSucceededEvent.getResponse()),
                              actualCommandSucceededEvent.getResponse().entrySet()
                                                          .containsAll(expectedCommandSucceededEvent.getResponse().entrySet()));
                }
            } else if (actual.getClass().equals(CommandFailedEvent.class)) {
                // nothing else to assert here
            } else {
                throw new UnsupportedOperationException("Unsupported event type: " + actual.getClass());
            }
        }
    }

    private static CommandSucceededEvent massageExpectedCommandSucceededEvent(final CommandSucceededEvent expected) {
        // massage numbers that are the wrong BSON type
        expected.getResponse().put("ok", new BsonDouble(expected.getResponse().getNumber("ok").doubleValue()));
        return expected;
    }

    private static CommandSucceededEvent massageActualCommandSucceededEvent(final CommandSucceededEvent actual) {
        BsonDocument response = getWritableCloneOfCommand(actual.getResponse());

        // massage numbers that are the wrong BSON type
        response.put("ok", new BsonDouble(response.getNumber("ok").doubleValue()));
        if (response.containsKey("n")) {
            response.put("n", new BsonInt32(response.getNumber("n").intValue()));
        }

        if (actual.getCommandName().equals("find") || actual.getCommandName().equals("getMore")) {
            if (response.containsKey("cursor")) {
                if (response.getDocument("cursor").containsKey("id")
                    && !response.getDocument("cursor").getInt64("id").equals(new BsonInt64(0))) {
                    response.getDocument("cursor").put("id", new BsonInt64(42));
                }
            }
        } else if (actual.getCommandName().equals("killCursors")) {
            response.getArray("cursorsUnknown").set(0, new BsonInt64(42));
        } else if (isWriteCommand(actual.getCommandName())) {
            if (response.containsKey("writeErrors")) {
                for (BsonValue bsonValue : response.getArray("writeErrors")) {
                    BsonDocument cur = bsonValue.asDocument();
                    BsonDocument newWriteErrorDocument =
                            new BsonDocument().append("index", cur.get("index"))
                                    .append("code", new BsonInt32(42))
                                    .append("errmsg", new BsonString(""));
                    cur.clear();
                    cur.putAll(newWriteErrorDocument);
                }
            }
            if (actual.getCommandName().equals("update")) {
                response.remove("nModified");
            }
        }
        return new CommandSucceededEvent(actual.getRequestId(), actual.getConnectionDescription(), actual.getCommandName(), response,
                actual.getElapsedTime(TimeUnit.NANOSECONDS));
    }

    private static CommandStartedEvent massageActualCommandStartedEvent(final CommandStartedEvent event,
                                                                        @Nullable final Map<String, BsonDocument> lsidMap,
                                                                        final CommandStartedEvent expectedCommandStartedEvent) {
        BsonDocument command = getWritableCloneOfCommand(event.getCommand());

        massageCommand(event, command);

        if (command.containsKey("readConcern") && (command.getDocument("readConcern").containsKey("afterClusterTime"))) {
            command.getDocument("readConcern").put("afterClusterTime", new BsonInt32(42));
        }

        massageActualCommand(command, expectedCommandStartedEvent.getCommand());

        return new CommandStartedEvent(event.getRequestId(), event.getConnectionDescription(), event.getDatabaseName(),
                event.getCommandName(), command);
    }

    private static void massageActualCommand(final BsonDocument command, final BsonDocument expectedCommand) {
        String[] keySet = command.keySet().toArray(new String[command.keySet().size()]);
        for (String key : keySet) {
            if (!expectedCommand.containsKey(key)) {
                command.remove(key);
            } else if (command.isDocument(key) && expectedCommand.isDocument(key)) {
                massageActualCommand(command.getDocument(key), expectedCommand.getDocument(key));
            }
        }

    }

    private static CommandStartedEvent massageExpectedCommandStartedEvent(final CommandStartedEvent event,
                                                                          final CommandStartedEvent actualEvent,
                                                                          @Nullable final Map<String, BsonDocument> lsidMap) {
        BsonDocument command = getWritableCloneOfCommand(event.getCommand());

        massageCommand(event, command);

        if (lsidMap == null) {
            command.remove("lsid");
        } else if (command.containsKey("lsid")) {
            command.put("lsid", lsidMap.get(command.getString("lsid").getValue()));
        }

        if (command.containsKey("txnNumber") && command.isNull("txnNumber")) {
            command.remove("txnNumber");
        }
        if (command.containsKey("stmtId") && command.isNull("stmtId")) {
            command.remove("stmtId");
        }
        if (command.containsKey("startTransaction") && command.isNull("startTransaction")) {
            command.remove("startTransaction");
        }
        if (command.containsKey("autocommit") && command.isNull("autocommit")) {
            command.remove("autocommit");
        }
        if (command.containsKey("maxTimeMS") && command.isNull("maxTimeMS")) {
            command.remove("maxTimeMS");
        }
        if (command.containsKey("writeConcern") && command.isNull("writeConcern")) {
            command.remove("writeConcern");
        }
        if (command.containsKey("readConcern")) {
            if (command.isNull("readConcern")) {
                command.remove("readConcern");
            }
        }
        if (command.containsKey("recoveryToken")) {
            command.remove("recoveryToken");
        }
        if (command.containsKey("query")) {
            command.remove("query");
        }
        if (command.containsKey("filter") && command.getDocument("filter").isEmpty()) {
            command.remove("filter");
        }
        if (command.containsKey("mapReduce")) {
            command.remove("mapReduce");
        }

        replaceTypeAssertionWithActual(command, actualEvent.getCommand());

        return new CommandStartedEvent(event.getRequestId(), event.getConnectionDescription(), event.getDatabaseName(),
                event.getCommandName(), command);
    }

    private static void massageCommand(final CommandStartedEvent event, final BsonDocument command) {
        if (event.getCommandName().equals("update")) {
            for (BsonValue bsonValue : command.getArray("updates")) {
                BsonDocument curUpdate = bsonValue.asDocument();
                if (!curUpdate.containsKey("multi")) {
                    curUpdate.put("multi", BsonBoolean.FALSE);
                }
                if (!curUpdate.containsKey("upsert")) {
                    curUpdate.put("upsert", BsonBoolean.FALSE);
                }
            }
        } else if (event.getCommandName().equals("getMore")) {
            command.put("getMore", new BsonInt64(42));
        } else if (event.getCommandName().equals("killCursors")) {
            command.getArray("cursors").set(0, new BsonInt64(42));
        }
        command.remove("$clusterTime");
    }

    private static BsonDocument getWritableCloneOfCommand(final BsonDocument original) {
        BsonDocument clone = new BsonDocument();
        BsonDocumentWriter writer = new BsonDocumentWriter(clone);
        new BsonDocumentCodec(CODEC_REGISTRY_HACK).encode(writer, original, EncoderContext.builder().build());
        return clone;
    }

    private CommandMonitoringTestHelper() {
    }
}
