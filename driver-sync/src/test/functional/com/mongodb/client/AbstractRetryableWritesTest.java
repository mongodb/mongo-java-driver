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

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.client.test.CollectionHelper;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.After;
import org.junit.Before;
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
import static com.mongodb.JsonTestServerVersionChecker.skipTest;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

// See https://github.com/mongodb/specifications/tree/master/source/retryable-writes/tests
@RunWith(Parameterized.class)
public abstract class AbstractRetryableWritesTest {
    private final String filename;
    private final String description;
    private final String databaseName;
    private final String collectionName;
    private final BsonArray data;
    private final BsonDocument definition;
    private final boolean skipTest;
    private MongoClient mongoClient;
    private CollectionHelper<Document> collectionHelper;
    private MongoCollection<BsonDocument> collection;
    private JsonPoweredCrudTestHelper helper;

    public AbstractRetryableWritesTest(final String filename, final String description, final String databaseName, final BsonArray data,
                                       final BsonDocument definition, final boolean skipTest) {
        this.filename = filename;
        this.description = description;
        this.databaseName = databaseName;
        this.collectionName = filename.substring(0, filename.lastIndexOf("."));
        this.data = data;
        this.definition = definition;
        this.skipTest = skipTest;
    }

    public abstract MongoClient createMongoClient(MongoClientSettings settings);

    @Before
    public void setUp() {
        assumeFalse(skipTest);
        collectionHelper = new CollectionHelper<Document>(new DocumentCodec(), new MongoNamespace(databaseName, collectionName));
        BsonDocument clientOptions = definition.getDocument("clientOptions", new BsonDocument());
        MongoClientSettings.Builder builder = getMongoClientSettingsBuilder();

        if (clientOptions.containsKey("retryWrites")) {
            builder.retryWrites(clientOptions.getBoolean("retryWrites").getValue());
        }
        mongoClient = createMongoClient(builder.build());

        List<BsonDocument> documents = new ArrayList<BsonDocument>();
        for (BsonValue document : data) {
            documents.add(document.asDocument());
        }

        collectionHelper.drop();
        collectionHelper.insertDocuments(documents);

        MongoDatabase database = mongoClient.getDatabase(databaseName);
        collection = database.getCollection(collectionName, BsonDocument.class);
        helper = new JsonPoweredCrudTestHelper(description, database, collection);
        if (definition.containsKey("failPoint")) {
            collectionHelper.runAdminCommand(definition.getDocument("failPoint"));
        }
    }

    @After
    public void cleanUp() {
        if (mongoClient != null) {
            mongoClient.close();
        }
        if (collectionHelper != null && definition.containsKey("failPoint")) {
            collectionHelper.runAdminCommand(new BsonDocument("configureFailPoint",
                    definition.getDocument("failPoint").getString("configureFailPoint"))
                    .append("mode", new BsonString("off")));
        }
    }

    @Test
    public void shouldPassAllOutcomes() {

        BsonDocument operation = definition.getDocument("operation");
        BsonDocument outcome = definition.getDocument("outcome");

        BsonDocument result = new BsonDocument();
        boolean wasException = false;
        try {
            result = helper.getOperationResults(operation);
        } catch (Exception e) {
            wasException = true;
        }

        if (outcome.containsKey("collection")) {
            List<BsonDocument> collectionData = collection.withDocumentClass(BsonDocument.class).find().into(new ArrayList<BsonDocument>());
            assertEquals(outcome.getDocument("collection").getArray("data").getValues(), collectionData);
        }

        if (outcome.getBoolean("error", BsonBoolean.FALSE).getValue()) {
            assertEquals(outcome.containsKey("error"), wasException);
        } else {
            BsonDocument fixedExpectedResult = outcome.getDocument("result", new BsonDocument());
            assertEquals(fixedExpectedResult, result.getDocument("result", new BsonDocument()));
        }
    }

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/retryable-writes")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            for (BsonValue test : testDocument.getArray("tests")) {
                data.add(new Object[]{file.getName(), test.asDocument().getString("description").getValue(),
                        testDocument.getString("database_name", new BsonString(getDefaultDatabaseName())).getValue(),
                        testDocument.getArray("data"), test.asDocument(),
                        !isDiscoverableReplicaSet() || skipTest(testDocument, test.asDocument())});
            }
        }
        return data;
    }
}
