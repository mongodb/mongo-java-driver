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

package com.mongodb.async.client;

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
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
import util.JsonPoweredTestHelper;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.async.client.Fixture.getDefaultDatabase;
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
    @Override
    public void setUp() {
        super.setUp();
        collection = Fixture.initializeCollection(new MongoNamespace(getDefaultDatabaseName(), getClass().getName()))
                .withDocumentClass(BsonDocument.class);
        new MongoOperation<Void>() {
            @Override
            public void execute() {
                List<BsonDocument> documents = new ArrayList<BsonDocument>();
                for (BsonValue document : data) {
                    documents.add(document.asDocument());
                }
                collection.insertMany(documents, getCallback());
            }
        }.get();
    }

    @Test
    public void shouldPassAllOutcomes() {
        BsonDocument outcome = getOperationMongoOperations(definition.getDocument("operation"));
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
            for (BsonValue test : testDocument.getArray("tests")) {
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
        BsonArray actual = new MongoOperation<BsonArray>() {
            @Override
            public void execute() {
                MongoCollection<BsonDocument> collectionToCompare = collection;
                if (expectedCollection.containsKey("name")) {
                    collectionToCompare = getDefaultDatabase().getCollection(expectedCollection.getString("name").getValue(),
                            BsonDocument.class);
                }
                collectionToCompare.find().into(new BsonArray(), getCallback());
            }
        }.get();
        assertEquals(description, expectedCollection.getArray("data"), actual);
    }

    private BsonDocument getOperationMongoOperations(final BsonDocument operation) {
        String name = operation.getString("name").getValue();
        BsonDocument arguments = operation.getDocument("arguments");

        String methodName = "get" + name.substring(0, 1).toUpperCase() + name.substring(1) + "MongoOperation";
        try {
            Method method = getClass().getDeclaredMethod(methodName, BsonDocument.class);
            return convertMongoOperationToResult(method.invoke(this, arguments));
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

    @SuppressWarnings({"unchecked", "rawtypes"})
    private BsonDocument convertMongoOperationToResult(final Object result) {
        if (result instanceof MongoOperationLong) {
            return toResult(new BsonInt32(((MongoOperationLong) result).get().intValue()));
        } else if (result instanceof MongoOperationVoid) {
            ((MongoOperationVoid) result).get();
            return toResult(BsonNull.VALUE);
        } else if (result instanceof MongoOperationBsonDocument) {
            return toResult((MongoOperationBsonDocument) result);
        } else if (result instanceof MongoOperationUpdateResult) {
            return toResult((MongoOperationUpdateResult) result);
        } else if (result instanceof MongoOperationDeleteResult) {
            return toResult((MongoOperationDeleteResult) result);
        } else if (result instanceof DistinctIterable<?>) {
            return toResult((DistinctIterable<BsonInt32>) result);
        } else if (result instanceof MongoIterable<?>) {
            return toResult((MongoIterable) result);
        } else if (result instanceof BsonValue) {
            return toResult((BsonValue) result);
        }
        throw new UnsupportedOperationException("Unknown object type cannot convert: " + result);
    }

    private BsonDocument toResult(final MongoOperationBsonDocument results) {
        return toResult(results.get());
    }

    private BsonDocument toResult(final DistinctIterable<BsonInt32> results) {
        return toResult(new MongoOperation<BsonArray>() {
            @Override
            public void execute() {
                results.into(new BsonArray(), getCallback());
            }
        }.get());
    }

    private BsonDocument toResult(final MongoIterable<BsonDocument> results) {
        return toResult(new MongoOperation<BsonArray>() {
            @Override
            public void execute() {
                results.into(new BsonArray(), getCallback());
            }
        }.get());
    }

    private BsonDocument toResult(final MongoOperationUpdateResult operation) {
        assumeTrue(serverVersionAtLeast(asList(2, 6, 0))); // ModifiedCount is not accessible pre 2.6

        UpdateResult updateResult = operation.get();
        BsonDocument resultDoc = new BsonDocument("matchedCount", new BsonInt32((int) updateResult.getMatchedCount()))
                .append("modifiedCount", new BsonInt32((int) updateResult.getModifiedCount()));
        if (updateResult.getUpsertedId() != null) {
            resultDoc.append("upsertedId", updateResult.getUpsertedId());
        }
        return toResult(resultDoc);
    }

    private BsonDocument toResult(final MongoOperationDeleteResult operation) {
        DeleteResult deleteResult = operation.get();
        return toResult(new BsonDocument("deletedCount", new BsonInt32((int) deleteResult.getDeletedCount())));
    }

    private BsonDocument toResult(final BsonValue results) {
        return new BsonDocument("result", results != null ? results : BsonNull.VALUE);
    }

    private AggregateIterable<BsonDocument> getAggregateMongoOperation(final BsonDocument arguments) {
        if (!serverVersionAtLeast(asList(2, 6, 0))) {
            assumeFalse(description.contains("$out"));
        }

        List<BsonDocument> pipeline = new ArrayList<BsonDocument>();
        for (BsonValue stage : arguments.getArray("pipeline")) {
            pipeline.add(stage.asDocument());
        }
        return collection.aggregate(pipeline).batchSize(arguments.getNumber("batchSize").intValue());
    }

    private MongoOperationLong getCountMongoOperation(final BsonDocument arguments) {
        return new MongoOperationLong() {
            @Override
            public void execute() {
                CountOptions options = new CountOptions();
                if (arguments.containsKey("skip")) {
                    options.skip(arguments.getNumber("skip").intValue());
                }
                if (arguments.containsKey("limit")) {
                    options.limit(arguments.getNumber("limit").intValue());
                }
                collection.count(arguments.getDocument("filter"), options, getCallback());
            }
        };
    }

    private DistinctIterable<BsonInt32> getDistinctMongoOperation(final BsonDocument arguments) {
        return collection.distinct(arguments.getString("fieldName").getValue(), arguments.getDocument("filter"), BsonInt32.class);
    }

    private FindIterable<BsonDocument> getFindMongoOperation(final BsonDocument arguments) {
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
        return findIterable;
    }

    private MongoOperationDeleteResult getDeleteManyMongoOperation(final BsonDocument arguments) {
        return new MongoOperationDeleteResult() {
            @Override
            public void execute() {
                collection.deleteMany(arguments.getDocument("filter"), getCallback());
            }
        };
    }

    private MongoOperationDeleteResult getDeleteOneMongoOperation(final BsonDocument arguments) {
        return new MongoOperationDeleteResult() {
            @Override
            public void execute() {
                collection.deleteOne(arguments.getDocument("filter"), getCallback());
            }
        };
    }

    private MongoOperationBsonDocument getFindOneAndDeleteMongoOperation(final BsonDocument arguments) {
        return new MongoOperationBsonDocument() {
            @Override
            public void execute() {
                FindOneAndDeleteOptions options = new FindOneAndDeleteOptions();
                if (arguments.containsKey("projection")) {
                    options.projection(arguments.getDocument("projection"));
                }
                if (arguments.containsKey("sort")) {
                    options.sort(arguments.getDocument("sort"));
                }
                collection.findOneAndDelete(arguments.getDocument("filter"), options, getCallback());
            }
        };
    }

    private MongoOperationBsonDocument getFindOneAndReplaceMongoOperation(final BsonDocument arguments) {
        assumeTrue(serverVersionAtLeast(asList(2, 6, 0))); // in 2.4 the server can ignore the supplied _id and creates an ObjectID

        return new MongoOperationBsonDocument() {
            @Override
            public void execute() {
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
                collection.findOneAndReplace(arguments.getDocument("filter"), arguments.getDocument("replacement"), options, getCallback());
            }
        };
    }

    private MongoOperationBsonDocument getFindOneAndUpdateMongoOperation(final BsonDocument arguments) {
        return new MongoOperationBsonDocument() {
            @Override
            public void execute() {

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
                collection.findOneAndUpdate(arguments.getDocument("filter"), arguments.getDocument("update"), options, getCallback());
            }
        };
    }

    private MongoOperationVoid getInsertOneMongoOperation(final BsonDocument arguments) {
        return new MongoOperationVoid() {
            @Override
            public void execute() {
                collection.insertOne(arguments.getDocument("document"), getCallback());
            }
        };
    }

    private MongoOperationVoid getInsertManyMongoOperation(final BsonDocument arguments) {
        return new MongoOperationVoid() {
            @Override
            public void execute() {
                List<BsonDocument> documents = new ArrayList<BsonDocument>();
                for (BsonValue document : arguments.getArray("documents")) {
                    documents.add(document.asDocument());
                }
                collection.insertMany(documents, getCallback());
            }
        };
    }

    private MongoOperationUpdateResult getReplaceOneMongoOperation(final BsonDocument arguments) {
        return new MongoOperationUpdateResult() {
            @Override
            public void execute() {
                UpdateOptions options = new UpdateOptions();
                if (arguments.containsKey("upsert")) {
                    options.upsert(arguments.getBoolean("upsert").getValue());
                }
                collection.replaceOne(arguments.getDocument("filter"), arguments.getDocument("replacement"), options, getCallback());
            }
        };
    }

    private MongoOperationUpdateResult getUpdateManyMongoOperation(final BsonDocument arguments) {
        return new MongoOperationUpdateResult() {
            @Override
            public void execute() {
                UpdateOptions options = new UpdateOptions();
                if (arguments.containsKey("upsert")) {
                    options.upsert(arguments.getBoolean("upsert").getValue());
                }
                collection.updateMany(arguments.getDocument("filter"), arguments.getDocument("update"), options, getCallback());
            }
        };
    }

    private MongoOperationUpdateResult getUpdateOneMongoOperation(final BsonDocument arguments) {
        return new MongoOperationUpdateResult() {
            @Override
            public void execute() {
                UpdateOptions options = new UpdateOptions();
                if (arguments.containsKey("upsert")) {
                    options.upsert(arguments.getBoolean("upsert").getValue());
                }
                collection.updateOne(arguments.getDocument("filter"), arguments.getDocument("update"), options, getCallback());
            }
        };
    }

    abstract class MongoOperationLong extends MongoOperation<Long> {
    }

    abstract class MongoOperationBsonDocument extends MongoOperation<BsonDocument> {
    }


    abstract class MongoOperationUpdateResult extends MongoOperation<UpdateResult> {
    }

    abstract class MongoOperationDeleteResult extends MongoOperation<DeleteResult> {
    }

    abstract class MongoOperationVoid extends MongoOperation<Void> {
    }

}
