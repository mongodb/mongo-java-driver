/*
 * Copyright 2015 MongoDB, Inc.
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

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.junit.Assume;
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

import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static org.junit.Assert.assertEquals;

// See https://github.com/mongodb/specifications/tree/master/source/crud/tests
@RunWith(Parameterized.class)
public class CrudTest extends DatabaseTestCase {
    private final String filename;
    private final String description;
    private final BsonArray data;
    private final BsonDocument definition;
    private MongoCollection<BsonDocument> collection;
    private JsonPoweredCrudTestHelper helper;

    public CrudTest(final String filename, final String description, final BsonArray data, final BsonDocument definition) {
        this.filename = filename;
        this.description = description;
        this.data = data;
        this.definition = definition;
    }

    @Before
    public void setUp() {
        super.setUp();
        List<BsonDocument> documents = new ArrayList<BsonDocument>();
        for (BsonValue document: data) {
            documents.add(document.asDocument());
        }
        getCollectionHelper().insertDocuments(documents);
        collection = database.getCollection(getClass().getName(), BsonDocument.class);
        helper = new JsonPoweredCrudTestHelper(description, collection);
    }

    @Test
    public void shouldPassAllOutcomes() {
        // Server versions prior to 2.6 do not properly recognize non-ObjectId _id values for upserts, so skipping a test that relies on
        // that unless the server version is at least 2.6
        if (description.equals("ReplaceOne with upsert when no documents match without an id specified")){
            Assume.assumeTrue(serverVersionAtLeast(2, 6));
        }

        BsonDocument outcome = helper.getOperationResults(definition.getDocument("operation"));
        BsonDocument expectedOutcome = definition.getDocument("outcome");

        if (checkResult()) {
            if (!serverVersionAtLeast(2, 6)) {
                if (expectedOutcome.isDocument("result")) {
                    expectedOutcome.getDocument("result").remove("modifiedCount");
                }
            }
            assertEquals(description, expectedOutcome.get("result"), outcome.get("result"));
        }
        if (expectedOutcome.containsKey("collection")) {
            assertCollectionEquals(expectedOutcome.getDocument("collection"));
        }
    }

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/crud")) {
            BsonDocument testDocument = util.JsonPoweredTestHelper.getTestDocument(file);
            if (testDocument.containsKey("minServerVersion")
                    && !serverAtLeastMinVersion(testDocument.getString("minServerVersion").getValue())) {
                continue;
            }
            for (BsonValue test: testDocument.getArray("tests")) {
                data.add(new Object[]{file.getName(), test.asDocument().getString("description").getValue(),
                        testDocument.getArray("data"), test.asDocument()});
            }
        }
        return data;
    }

    private boolean checkResult() {
        if (filename.contains("insert")) {
            // We don't return any id's for insert commands
            return false;
        } else if (!serverVersionAtLeast(3, 0)
                && description.contains("when no documents match with upsert returning the document before modification")) {
            // Pre 3.0 versions of MongoDB return an empty document rather than a null
            return false;
        }
        return true;
    }

    private void assertCollectionEquals(final BsonDocument expectedCollection) {
        MongoCollection<BsonDocument> collectionToCompare = collection;
        if (expectedCollection.containsKey("name")) {
            collectionToCompare = database.getCollection(expectedCollection.getString("name").getValue(), BsonDocument.class);
        }
        assertEquals(description, expectedCollection.getArray("data"), collectionToCompare.find().into(new BsonArray()));
    }

    private static boolean serverAtLeastMinVersion(final String minServerVersionString) {
        List<Integer> versionList = new ArrayList<Integer>();
        for (String s : minServerVersionString.split("\\.")) {
            versionList.add(Integer.valueOf(s));
        }
        while (versionList.size() < 3) {
            versionList.add(0);
        }
        return serverVersionAtLeast(versionList.subList(0, 3));
    }
}
