/*
 * Copyright (c) 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.connection.TestCommandListener;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import util.JsonPoweredTestHelper;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.Fixture.getMongoClientURI;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

// See https://github.com/mongodb/specifications/tree/master/source/command-monitoring/tests
@RunWith(Parameterized.class)
public class CommandMonitoringTest {
    private static MongoClient mongoClient;
    private static TestCommandListener commandListener;
    private final String filename;
    private final String description;
    private final String databaseName;
    private final String collectionName;
    private final BsonArray data;
    private final BsonDocument definition;
    private MongoCollection<BsonDocument> collection;
    private JsonPoweredCrudTestHelper helper;

    public CommandMonitoringTest(final String filename, final String description, final String databaseName, final String collectionName,
                                 final BsonArray data, final BsonDocument definition) {
        this.filename = filename;
        this.description = description;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.data = data;
        this.definition = definition;
    }

    @BeforeClass
    public static void beforeClass() {
        commandListener = new TestCommandListener();
        mongoClient = new MongoClient(getMongoClientURI(MongoClientOptions.builder().addCommandListener(commandListener)));
    }

    @AfterClass
    public static void afterClass() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Before
    public void setUp() {
        List<BsonDocument> documents = new ArrayList<BsonDocument>();
        for (BsonValue document : data) {
            documents.add(document.asDocument());
        }
        CollectionHelper<Document> collectionHelper = new CollectionHelper<Document>(new DocumentCodec(),
                                                                                     new MongoNamespace(databaseName,
                                                                                                        collectionName));
        collectionHelper.drop();
        collectionHelper.insertDocuments(documents);

        commandListener.reset();
        collection = mongoClient.getDatabase(databaseName).getCollection(collectionName, BsonDocument.class);
        if (definition.getDocument("operation").containsKey("read_preference")) {
            collection = collection.withReadPreference(ReadPreference.valueOf(definition.getDocument("operation")
                                                                                        .getDocument("read_preference")
                                                                                        .getString("mode").getValue()));
        }
        helper = new JsonPoweredCrudTestHelper(description, collection);
    }

    @Test
    public void shouldPassAllOutcomes() {
        // On server <= 2.4, insertMany generates an insert command for every document, so the test fails
        assumeFalse(filename.startsWith("insertMany") && !serverVersionAtLeast(asList(2, 6, 0)));

        executeOperation();

        List<CommandEvent> expectedEvents = getExpectedEvents(definition.getArray("expectations"));
        List<CommandEvent> events = commandListener.getEvents();

        assertEquals(expectedEvents.size(), events.size());

        for (int i = 0; i < events.size(); i++) {
            CommandEvent actual = events.get(i);
            CommandEvent expected = expectedEvents.get(i);

            assertEquals(expected.getClass(), actual.getClass());
            assertEquals(expected.getCommandName(), actual.getCommandName());

            if (actual.getClass().equals(CommandStartedEvent.class)) {
                CommandStartedEvent actualCommandStartedEvent = massageActualCommandStartedEvent((CommandStartedEvent) actual);
                CommandStartedEvent expectedCommandStartedEvent = (CommandStartedEvent) expected;

                assertEquals(expectedCommandStartedEvent.getDatabaseName(), actualCommandStartedEvent.getDatabaseName());
                assertEquals(expectedCommandStartedEvent.getCommand(), actualCommandStartedEvent.getCommand());

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

    private CommandSucceededEvent massageExpectedCommandSucceededEvent(final CommandSucceededEvent expected) {
        // massage numbers that are the wrong BSON type
        expected.getResponse().put("ok", new BsonDouble(expected.getResponse().getNumber("ok").doubleValue()));
        return expected;
    }

    private CommandSucceededEvent massageActualCommandSucceededEvent(final CommandSucceededEvent actual) {
        // massage numbers that are the wrong BSON type
        actual.getResponse().put("ok", new BsonDouble(actual.getResponse().getNumber("ok").doubleValue()));
        if (actual.getResponse().containsKey("n")) {
            actual.getResponse().put("n", new BsonInt32(actual.getResponse().getNumber("n").intValue()));
        }

        if (actual.getCommandName().equals("find") || actual.getCommandName().equals("getMore")) {
            if (actual.getResponse().containsKey("cursor")) {
                if (actual.getResponse().getDocument("cursor").containsKey("id")
                    && !actual.getResponse().getDocument("cursor").getInt64("id").equals(new BsonInt64(0))) {
                    actual.getResponse().getDocument("cursor").put("id", new BsonInt64(42));
                }
            }
        } else if (actual.getCommandName().equals("killCursors")) {
            actual.getResponse().getArray("cursorsUnknown").set(0, new BsonInt64(42));
        } else if (isWriteCommand(actual.getCommandName())) {
            if (actual.getResponse().containsKey("writeErrors")) {
                for (Iterator<BsonValue> iter = actual.getResponse().getArray("writeErrors").iterator(); iter.hasNext();) {
                    BsonDocument cur = iter.next().asDocument();
                    cur.put("code", new BsonInt32(42));
                    cur.put("errmsg", new BsonString(""));
                }
            }
            if (actual.getCommandName().equals("update")) {
                actual.getResponse().remove("nModified");
            }
        }
        return actual;
    }

    private boolean isWriteCommand(final String commandName) {
        return asList("insert", "update", "delete").contains(commandName);
    }

    private CommandStartedEvent massageActualCommandStartedEvent(final CommandStartedEvent actual) {
        if (actual.getCommandName().equals("update")) {
            for (Iterator<BsonValue> iter = actual.getCommand().getArray("updates").iterator(); iter.hasNext();) {
                BsonDocument curUpdate = iter.next().asDocument();
                if (!curUpdate.containsKey("multi")) {
                    curUpdate.put("multi", BsonBoolean.FALSE);
                }
                if (!curUpdate.containsKey("upsert")) {
                    curUpdate.put("upsert", BsonBoolean.FALSE);
                }
            }
        } else if (actual.getCommandName().equals("getMore")) {
            actual.getCommand().put("getMore", new BsonInt64(42));
        } else if (actual.getCommandName().equals("killCursors")) {
            actual.getCommand().getArray("cursors").set(0, new BsonInt64(42));
        }

        return actual;
    }

    private void executeOperation() {
        try {
            helper.getOperationResults(definition.getDocument("operation"));
        } catch (MongoException e) {
            // ignore, as some of these are expected to throw exceptions
        }
    }

    private List<CommandEvent> getExpectedEvents(final BsonArray expectedEventDocuments) {
        List<CommandEvent> expectedEvents = new ArrayList<CommandEvent>(expectedEventDocuments.size());
        for (Iterator<BsonValue> iterator = expectedEventDocuments.iterator(); iterator.hasNext();) {
            BsonDocument curExpectedEventDocument = iterator.next().asDocument();
            String eventType = curExpectedEventDocument.keySet().iterator().next();
            BsonDocument eventDescriptionDocument = curExpectedEventDocument.getDocument(eventType);
            CommandEvent commandEvent;
            if (eventType.equals("command_started_event")) {
                commandEvent = new CommandStartedEvent(1, null, databaseName,
                                                       eventDescriptionDocument.getString("command_name").getValue(),
                                                       eventDescriptionDocument.getDocument("command"));
            } else if (eventType.equals("command_succeeded_event")) {
                BsonDocument replyDocument = eventDescriptionDocument.get("reply").asDocument();
                commandEvent = new CommandSucceededEvent(1, null, eventDescriptionDocument.getString("command_name").getValue(),
                                                         replyDocument, 1);

            } else if (eventType.equals("command_failed_event")) {
                commandEvent = new CommandFailedEvent(1, null, eventDescriptionDocument.getString("command_name").getValue(), 1, null);
            } else {
                throw new UnsupportedOperationException("Unsupported command event type: " + eventType);
            }
            expectedEvents.add(commandEvent);
        }
        return expectedEvents;
    }

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/command-monitoring")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            for (BsonValue test : testDocument.getArray("tests")) {
                data.add(new Object[]{file.getName(), test.asDocument().getString("description").getValue(),
                                      testDocument.getString("database_name").getValue(),
                                      testDocument.getString("collection_name").getValue(),
                                      testDocument.getArray("data"), test.asDocument()});
            }
        }
        return data;
    }
}
