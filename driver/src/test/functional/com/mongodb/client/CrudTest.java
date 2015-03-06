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

import com.mongodb.JsonPoweredTestHelper;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.junit.AssumptionViolatedException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

// See https://github.com/mongodb/specifications/tree/master/source/crud/tests
@RunWith(Parameterized.class)
public class CrudTest extends DatabaseTestCase {
    private final String filename;
    private final String description;
    private final BsonArray data;
    private final BsonDocument definition;
    private MongoCollection<BsonDocument> collection;

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
    }

    @Test
    public void shouldPassAllOutcomes() {
        BsonDocument outcome = getOperationResults(definition.getDocument("operation"));
        BsonDocument expectedOutcome = definition.getDocument("outcome");

        if (checkResult()) {
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
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
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
        } else if (!serverVersionAtLeast(asList(3, 0, 0))
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

    private BsonDocument getOperationResults(final BsonDocument operation) {
        String name = operation.getString("name").getValue();
        BsonDocument arguments = operation.getDocument("arguments");

        String methodName = "get" + name.substring(0, 1).toUpperCase() + name.substring(1) + "Result";
        try {
            Method method = getClass().getDeclaredMethod(methodName, BsonDocument.class);
            return (BsonDocument) method.invoke(this, arguments);
        } catch (NoSuchMethodException e) {
            throw new UnsupportedOperationException("No handler for operation " + methodName);
        } catch (InvocationTargetException e) {
            if (e.getTargetException() instanceof AssumptionViolatedException) {
                throw (AssumptionViolatedException) e.getTargetException();
            }
            throw new UnsupportedOperationException("Invalid handler for operation " + methodName);
        } catch (IllegalAccessException e) {
            throw new UnsupportedOperationException("Invalid handler access for operation " + methodName);
        }
    }

    private BsonDocument toResult(final int count) {
        return toResult(new BsonInt32(count));
    }
    private BsonDocument toResult(final MongoIterable<BsonDocument> results) {
        return toResult(new BsonArray(results.into(new ArrayList<BsonDocument>())));
    }
    private BsonDocument toResult(final String key, final BsonValue value) {
        return toResult(new BsonDocument(key, value));
    }
    private BsonDocument toResult(final UpdateResult updateResult) {
        assumeTrue(serverVersionAtLeast(asList(2, 6, 0))); // ModifiedCount is not accessible pre 2.6
        BsonDocument resultDoc = new BsonDocument("matchedCount", new BsonInt32((int) updateResult.getMatchedCount()))
                .append("modifiedCount", new BsonInt32((int) updateResult.getModifiedCount()));
        if (updateResult.getUpsertedId() != null) {
            resultDoc.append("upsertedId", updateResult.getUpsertedId());
        }
        return toResult(resultDoc);
    }
    private BsonDocument toResult(final BsonValue results) {
        return new BsonDocument("result", results != null ? results : BsonNull.VALUE);
    }
    private BsonDocument getAggregateResult(final BsonDocument arguments) {
        if (!serverVersionAtLeast(asList(2, 6, 0))) {
            assumeFalse(description.contains("$out"));
        }

        List<BsonDocument> pipeline = new ArrayList<BsonDocument>();
        for (BsonValue stage: arguments.getArray("pipeline")) {
            pipeline.add(stage.asDocument());
        }
        return toResult(collection.aggregate(pipeline).batchSize(arguments.getNumber("batchSize").intValue()));
    }

    private BsonDocument getCountResult(final BsonDocument arguments) {
        CountOptions options = new CountOptions();
        if (arguments.containsKey("skip")) {
            options.skip(arguments.getNumber("skip").intValue());
        }
        if (arguments.containsKey("limit")) {
            options.limit(arguments.getNumber("limit").intValue());
        }

        return toResult((int) collection.count(arguments.getDocument("filter"), options));
    }

    private BsonDocument getDistinctResult(final BsonDocument arguments) {
        return toResult(collection.distinct(arguments.getString("fieldName").getValue(), BsonInt32.class)
                .filter(arguments.getDocument("filter")).into(new BsonArray()));
    }

    private BsonDocument getFindResult(final BsonDocument arguments) {
        FindIterable<BsonDocument> findIterable = collection.find(arguments.getDocument("filter"));
        if (arguments.containsKey("skip")) {
            findIterable.skip(arguments.getNumber("skip").intValue());
        }
        if (arguments.containsKey("limit")) {
            findIterable.limit(arguments.getNumber("limit").intValue());
        }
        if (arguments.containsKey("batchSize")) {
            findIterable.batchSize(arguments.getNumber("batchSize").intValue());
        }
        return toResult(findIterable);
    }

    private BsonDocument getDeleteManyResult(final BsonDocument arguments) {
        return toResult("deletedCount",
                new BsonInt32((int) collection.deleteMany(arguments.getDocument("filter")).getDeletedCount()));
    }

    private BsonDocument getDeleteOneResult(final BsonDocument arguments) {
        return toResult("deletedCount", new BsonInt32((int) collection.deleteOne(arguments.getDocument("filter")).getDeletedCount()));
    }

    private BsonDocument getFindOneAndDeleteResult(final BsonDocument arguments) {
        FindOneAndDeleteOptions options = new FindOneAndDeleteOptions();
        if (arguments.containsKey("projection")) {
            options.projection(arguments.getDocument("projection"));
        }
        if (arguments.containsKey("sort")) {
            options.sort(arguments.getDocument("sort"));
        }
        return toResult(collection.findOneAndDelete(arguments.getDocument("filter"), options));
    }

    private BsonDocument getFindOneAndReplaceResult(final BsonDocument arguments) {
        assumeTrue(serverVersionAtLeast(asList(2, 6, 0))); // in 2.4 the server can ignore the supplied _id and creates an ObjectID
        FindOneAndReplaceOptions options = new FindOneAndReplaceOptions();
        if (arguments.containsKey("projection")) {
            options.projection(arguments.getDocument("projection"));
        }
        if (arguments.containsKey("sort")) {
            options.sort(arguments.getDocument("sort"));
        }
        if (arguments.containsKey("upsert")) {
            options.upsert(arguments.getBoolean("upsert").getValue());
        }
        if (arguments.containsKey("returnDocument")) {
            options.returnDocument(arguments.getString("returnDocument").getValue().equals("After") ? ReturnDocument.AFTER
                    : ReturnDocument.BEFORE);
        }
        return toResult(collection.findOneAndReplace(arguments.getDocument("filter"), arguments.getDocument("replacement"), options));
    }

    private BsonDocument getFindOneAndUpdateResult(final BsonDocument arguments) {
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        if (arguments.containsKey("projection")) {
            options.projection(arguments.getDocument("projection"));
        }
        if (arguments.containsKey("sort")) {
            options.sort(arguments.getDocument("sort"));
        }
        if (arguments.containsKey("upsert")) {
            options.upsert(arguments.getBoolean("upsert").getValue());
        }
        if (arguments.containsKey("returnDocument")) {
            options.returnDocument(arguments.getString("returnDocument").getValue().equals("After") ? ReturnDocument.AFTER
                    : ReturnDocument.BEFORE);
        }
        return toResult(collection.findOneAndUpdate(arguments.getDocument("filter"), arguments.getDocument("update"), options));
    }

    private BsonDocument getInsertOneResult(final BsonDocument arguments) {
        collection.insertOne(arguments.getDocument("document"));
        return toResult((BsonValue) null);
    }

    private BsonDocument getInsertManyResult(final BsonDocument arguments) {
        List<BsonDocument> documents = new ArrayList<BsonDocument>();
        for (BsonValue document : arguments.getArray("documents")) {
            documents.add(document.asDocument());
        }
        collection.insertMany(documents);
        return toResult((BsonValue) null);
    }

    private BsonDocument getReplaceOneResult(final BsonDocument arguments) {
        UpdateOptions options = new UpdateOptions();
        if (arguments.containsKey("upsert")) {
            options.upsert(arguments.getBoolean("upsert").getValue());
        }
        return toResult(collection.replaceOne(arguments.getDocument("filter"), arguments.getDocument("replacement"), options));
    }

    private BsonDocument getUpdateManyResult(final BsonDocument arguments) {
        UpdateOptions options = new UpdateOptions();
        if (arguments.containsKey("upsert")) {
            options.upsert(arguments.getBoolean("upsert").getValue());
        }
        return toResult(collection.updateMany(arguments.getDocument("filter"), arguments.getDocument("update"), options));
    }

    private BsonDocument getUpdateOneResult(final BsonDocument arguments) {
        UpdateOptions options = new UpdateOptions();
        if (arguments.containsKey("upsert")) {
            options.upsert(arguments.getBoolean("upsert").getValue());
        }
        return toResult(collection.updateOne(arguments.getDocument("filter"), arguments.getDocument("update"), options));
    }
}
