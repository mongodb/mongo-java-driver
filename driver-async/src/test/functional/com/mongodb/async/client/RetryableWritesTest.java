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

package com.mongodb.async.client;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.client.test.CollectionHelper;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.After;
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

import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.JsonTestServerVersionChecker.skipTest;
import static com.mongodb.async.client.Fixture.getMongoClientBuilderFromConnectionString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

// See https://github.com/mongodb/specifications/tree/master/source/retryable-writes/tests
@RunWith(Parameterized.class)
public class RetryableWritesTest extends DatabaseTestCase {
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

    public RetryableWritesTest(final String filename, final String description, final BsonArray data, final BsonDocument definition,
                               final boolean skipTest) {
        this.filename = filename;
        this.description = description;
        this.databaseName = getDefaultDatabaseName();
        this.collectionName = filename.substring(0, filename.lastIndexOf("."));
        this.data = data;
        this.definition = definition;
        this.skipTest = skipTest;
    }

    @BeforeClass
    public static void beforeClass() {
    }

    @AfterClass
    public static void afterClass() {
    }

    @Before
    @Override
    public void setUp() {
        assumeFalse(skipTest);
        collectionHelper = new CollectionHelper<Document>(new DocumentCodec(), new MongoNamespace(databaseName, collectionName));
        BsonDocument clientOptions = definition.getDocument("clientOptions", new BsonDocument());
        MongoClientSettings.Builder builder = getMongoClientBuilderFromConnectionString();
        if (clientOptions.containsKey("retryWrites")) {
            builder.retryWrites(clientOptions.getBoolean("retryWrites").getValue());
        }
        mongoClient = MongoClients.create(builder.build());

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
        Exception raisedException = null;
        try {
            result = helper.getOperationResults(operation);
        } catch (Exception e) {
            wasException = true;
            raisedException = e;
        }

        if (outcome.getBoolean("error", BsonBoolean.FALSE).getValue()) {
            assertEquals(outcome.containsKey("error"), wasException);
        } else if (wasException) {
            fail("Unexpected exception: " + raisedException);
        } else {
            BsonDocument fixedExpectedResult = outcome.getDocument("result", new BsonDocument());
            assertEquals(fixedExpectedResult, result.getDocument("result", new BsonDocument()));
        }

        if (outcome.containsKey("collection")) {
            FutureResultCallback<List<BsonDocument>> futureResultCallback = new FutureResultCallback<List<BsonDocument>>();
            collection.withDocumentClass(BsonDocument.class).find().into(new ArrayList<BsonDocument>(), futureResultCallback);
            assertEquals(outcome.getDocument("collection").getArray("data").getValues(), futureResult(futureResultCallback));
        }

    }

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/retryable-writes")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            for (BsonValue test : testDocument.getArray("tests")) {
                data.add(new Object[]{file.getName(), test.asDocument().getString("description").getValue(),
                        testDocument.getArray("data"), test.asDocument(),
                        !isDiscoverableReplicaSet() || skipTest(testDocument, test.asDocument())});
            }
        }
        return data;
    }

    <T> T futureResult(final FutureResultCallback<T> callback) {
        try {
            return callback.get();
        } catch (Throwable t) {
            throw new MongoException("FutureResultCallback failed", t);
        }
    }

}
