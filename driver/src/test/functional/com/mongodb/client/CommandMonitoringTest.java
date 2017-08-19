/*
 * Copyright 2015-2016 MongoDB, Inc.
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

import com.mongodb.ClusterFixture;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.connection.ServerVersion;
import com.mongodb.connection.TestCommandListener;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
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

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.Fixture.getMongoClientURI;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

// See https://github.com/mongodb/specifications/tree/master/source/command-monitoring/tests
@RunWith(Parameterized.class)
public class CommandMonitoringTest {
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

        ServerVersion serverVersion = ClusterFixture.getServerVersion();
        if (definition.containsKey("ignore_if_server_version_less_than")) {
            assumeFalse(serverVersion.compareTo(getServerVersion("ignore_if_server_version_less_than")) < 0);
        }
        if (definition.containsKey("ignore_if_server_version_greater_than")) {
            assumeFalse(serverVersion.compareTo(getServerVersion("ignore_if_server_version_greater_than")) > 0);
        }
        if (definition.containsKey("ignore_if_topology_type")) {
            BsonArray topologyTypes = definition.getArray("ignore_if_topology_type");
            for (BsonValue type : topologyTypes) {
                String typeString = type.asString().getValue();
                if (typeString.equals("sharded")) {
                    assumeFalse(isSharded());
                } else if (typeString.equals("replica_set")) {
                    assumeFalse(isDiscoverableReplicaSet());
                } else if (typeString.equals("standalone")) {
                    assumeFalse(isSharded());
                }
            }
        }

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

    private ServerVersion getServerVersion(final String fieldName) {
        String[] versionStringArray = definition.getString(fieldName).getValue().split("\\.");
        return new ServerVersion(Integer.parseInt(versionStringArray[0]), Integer.parseInt(versionStringArray[1]));
    }

    @Test
    public void shouldPassAllOutcomes() {
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
                for (Iterator<BsonValue> iter = response.getArray("writeErrors").iterator(); iter.hasNext();) {
                    BsonDocument cur = iter.next().asDocument();
                    cur.put("code", new BsonInt32(42));
                    cur.put("errmsg", new BsonString(""));
                }
            }
            if (actual.getCommandName().equals("update")) {
                response.remove("nModified");
            }
        }
        return new CommandSucceededEvent(actual.getRequestId(), actual.getConnectionDescription(), actual.getCommandName(), response,
                actual.getElapsedTime(TimeUnit.NANOSECONDS));
    }

    private boolean isWriteCommand(final String commandName) {
        return asList("insert", "update", "delete").contains(commandName);
    }

    private CommandStartedEvent massageActualCommandStartedEvent(final CommandStartedEvent actual) {
        BsonDocument command = getWritableCloneOfCommand(actual.getCommand());

        if (actual.getCommandName().equals("update")) {
            for (Iterator<BsonValue> iter = command.getArray("updates").iterator(); iter.hasNext();) {
                BsonDocument curUpdate = iter.next().asDocument();
                if (!curUpdate.containsKey("multi")) {
                    curUpdate.put("multi", BsonBoolean.FALSE);
                }
                if (!curUpdate.containsKey("upsert")) {
                    curUpdate.put("upsert", BsonBoolean.FALSE);
                }
            }
        } else if (actual.getCommandName().equals("getMore")) {
            command.put("getMore", new BsonInt64(42));
        } else if (actual.getCommandName().equals("killCursors")) {
            command.getArray("cursors").set(0, new BsonInt64(42));
        }

        return new CommandStartedEvent(actual.getRequestId(), actual.getConnectionDescription(), actual.getDatabaseName(),
                actual.getCommandName(), command);
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
            String commandName = eventDescriptionDocument.getString("command_name").getValue();
            if (eventType.equals("command_started_event")) {
                BsonDocument commandDocument = eventDescriptionDocument.getDocument("command");
                // Not clear whether these global fields should be included, but also not clear how to efficiently exclude them
                if (ClusterFixture.serverVersionAtLeast(3, 5)) {
                    commandDocument.put("$db", new BsonString(databaseName));
                    BsonDocument operation = definition.getDocument("operation");
                    if (operation.containsKey("read_preference")) {
                        commandDocument.put("$readPreference", operation.getDocument("read_preference"));
                    }
                }
                commandEvent = new CommandStartedEvent(1, null, databaseName, commandName, commandDocument);
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

    private BsonDocument getWritableCloneOfCommand(final BsonDocument original) {
        BsonDocument clone = new BsonDocument();
        BsonDocumentWriter writer = new BsonDocumentWriter(clone);
        new BsonDocumentCodec(CODEC_REGISTRY_HACK).encode(writer, original, EncoderContext.builder().build());
        return clone;
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
