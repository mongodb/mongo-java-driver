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

package com.mongodb.embedded.client;

import com.mongodb.client.JsonPoweredCrudTestHelper;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
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

import static com.mongodb.JsonTestServerVersionChecker.skipTest;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.embedded.client.Fixture.getMongoClient;
import static com.mongodb.embedded.client.Fixture.getServerVersion;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

// See https://github.com/mongodb/specifications/tree/master/source/crud/tests
@RunWith(Parameterized.class)
public class CrudTest extends DatabaseTestCase {
    private final String filename;
    private final String description;
    private final String databaseName;
    private final BsonArray data;
    private final BsonDocument definition;
    private final boolean skipTest;
    private MongoDatabase database;
    private MongoCollection<BsonDocument> collection;
    private JsonPoweredCrudTestHelper helper;

    public CrudTest(final String filename, final String description, final String databaseName, final BsonArray data,
                    final BsonDocument definition, final boolean skipTest) {
        this.filename = filename;
        this.description = description;
        this.databaseName = databaseName;
        this.data = data;
        this.definition = definition;
        this.skipTest = skipTest;
    }

    @Before
    public void setUp() {
        assumeFalse(skipTest);
        assumeFalse(description.startsWith("Aggregate with $listLocalSessions"));
        database = getMongoClient().getDatabase(databaseName);
        collection = database.getCollection(getClass().getName(), BsonDocument.class);
        if (!data.isEmpty()) {
            List<BsonDocument> documents = new ArrayList<BsonDocument>();
            for (BsonValue document : data) {
                documents.add(document.asDocument());
            }
            collection.insertMany(documents);
        }
        helper = new JsonPoweredCrudTestHelper(description, database, collection);
    }

    @After
    public void tearDown() {
        if (collection != null) {
            collection.drop();
        }
    }

    @Test
    public void shouldPassAllOutcomes() {
        BsonDocument expectedOutcome = definition.getDocument("outcome", null);
        // check if v1 test
        if (definition.containsKey("operation")) {
            runOperation(expectedOutcome, definition.getDocument("operation"),
                    expectedOutcome != null && expectedOutcome.containsKey("result") && expectedOutcome.isDocument("result")
                            ? expectedOutcome.get("result") : null);
        } else {  // v2 test
            BsonArray operations = definition.getArray("operations");
            for (BsonValue operation : operations) {
                runOperation(expectedOutcome, operation.asDocument(), operation.asDocument().get("result", null));
            }
        }
    }

    private void runOperation(final BsonDocument expectedOutcome, final BsonDocument operation, final BsonValue expectedResult) {
        BsonDocument outcome = null;
        boolean wasException = false;
        try {
            outcome = helper.getOperationResults(operation);
        } catch (Exception e) {
            wasException = true;
        }

        if (operation.getBoolean("error", BsonBoolean.FALSE).getValue()) {
            assertEquals(operation.containsKey("error"), wasException);
        }

        if (expectedResult != null) {
            // Hack to workaround the lack of upsertedCount
            BsonValue actualResult = outcome.get("result");
            if (actualResult.isDocument()
                    && actualResult.asDocument().containsKey("upsertedCount")
                    && actualResult.asDocument().getNumber("upsertedCount").intValue() == 0
                    && !expectedResult.asDocument().containsKey("upsertedCount")) {
                expectedResult.asDocument().append("upsertedCount", actualResult.asDocument().get("upsertedCount"));
            }

            // Hack to workaround the lack of insertedIds
            if (expectedResult.isDocument()
                    && !expectedResult.asDocument().containsKey("insertedIds")) {
                actualResult.asDocument().remove("insertedIds");
            }

            assertEquals(description, expectedResult, actualResult);
        }

        if (expectedOutcome.containsKey("collection")) {
            assertCollectionEquals(expectedOutcome.getDocument("collection"));
        }
    }

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        if (!Fixture.runEmbeddedTests()) {
            return data;
        }

        for (File file : JsonPoweredTestHelper.getTestFiles("/crud")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            for (BsonValue test: testDocument.getArray("tests")) {
                data.add(new Object[]{file.getName(), test.asDocument().getString("description").getValue(),
                        testDocument.getString("database_name", new BsonString(getDefaultDatabaseName())).getValue(),
                        testDocument.getArray("data", new BsonArray()), test.asDocument(),
                        skipTest(testDocument, test.asDocument(), getServerVersion())});
            }
        }
        return data;
    }

    private void assertCollectionEquals(final BsonDocument expectedCollection) {
        MongoCollection<BsonDocument> collectionToCompare = collection;
        if (expectedCollection.containsKey("name")) {
            collectionToCompare = database.getCollection(expectedCollection.getString("name").getValue(), BsonDocument.class);
        }
        assertEquals(description, expectedCollection.getArray("data"), collectionToCompare.find().into(new BsonArray()));
    }
}
