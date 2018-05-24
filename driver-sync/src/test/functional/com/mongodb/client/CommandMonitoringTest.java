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

package com.mongodb.client;

import com.mongodb.ClusterFixture;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.connection.ServerVersion;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.event.CommandEvent;
import org.bson.BsonArray;
import org.bson.BsonDocument;
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
import java.util.List;

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ClusterFixture.isStandalone;
import static com.mongodb.client.CommandMonitoringTestHelper.getExpectedEvents;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
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
        mongoClient = MongoClients.create(getMongoClientSettingsBuilder().addCommandListener(commandListener).build());
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
                    assumeFalse(isStandalone());
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
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        collection = database.getCollection(collectionName, BsonDocument.class);
        if (definition.getDocument("operation").containsKey("read_preference")) {
            collection = collection.withReadPreference(ReadPreference.valueOf(definition.getDocument("operation")
                                                                                        .getDocument("read_preference")
                                                                                        .getString("mode").getValue()));
        }
        helper = new JsonPoweredCrudTestHelper(description, database, collection);
    }

    private ServerVersion getServerVersion(final String fieldName) {
        String[] versionStringArray = definition.getString(fieldName).getValue().split("\\.");
        return new ServerVersion(Integer.parseInt(versionStringArray[0]), Integer.parseInt(versionStringArray[1]));
    }

    @Test
    public void shouldPassAllOutcomes() {
        executeOperation();

        List<CommandEvent> expectedEvents = getExpectedEvents(definition.getArray("expectations"), databaseName,
                definition.getDocument("operation"));
        List<CommandEvent> events = commandListener.getEvents();

        CommandMonitoringTestHelper.assertEventsEquality(expectedEvents, events);
    }

    private void executeOperation() {
        try {
            helper.getOperationResults(definition.getDocument("operation"));
        } catch (MongoException e) {
            // ignore, as some of these are expected to throw exceptions
        }
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
