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
import com.mongodb.async.SingleResultCallback;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CollationAlternate;
import com.mongodb.client.model.CollationCaseFirst;
import com.mongodb.client.model.CollationMaxVariable;
import com.mongodb.client.model.CollationStrength;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
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
import static com.mongodb.ClusterFixture.serverVersionGreaterThan;
import static com.mongodb.ClusterFixture.serverVersionLessThan;
import static com.mongodb.async.client.Fixture.getDefaultDatabase;
import static org.junit.Assert.assertEquals;

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

        // Hack to workaround the lack of upsertedCount
        BsonValue expectedResult = expectedOutcome.get("result");
        BsonValue actualResult = outcome.get("result");
        if (actualResult.isDocument()
                    && actualResult.asDocument().containsKey("upsertedCount")
                    && actualResult.asDocument().getNumber("upsertedCount").intValue() == 0
                    && !expectedResult.asDocument().containsKey("upsertedCount")) {
            expectedResult.asDocument().append("upsertedCount", actualResult.asDocument().get("upsertedCount"));
        }
        assertEquals(description, expectedResult, actualResult);

        if (expectedOutcome.containsKey("collection")) {
            assertCollectionEquals(expectedOutcome.getDocument("collection"));
        }
    }

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/crud")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            if (testDocument.containsKey("minServerVersion")
                        && serverVersionLessThan(testDocument.getString("minServerVersion").getValue())) {
                continue;
            }
            if (testDocument.containsKey("maxServerVersion")
                        && serverVersionGreaterThan(testDocument.getString("maxServerVersion").getValue())) {
                continue;
            }
            for (BsonValue test : testDocument.getArray("tests")) {
                data.add(new Object[]{file.getName(), test.asDocument().getString("description").getValue(),
                        testDocument.getArray("data"), test.asDocument()});
            }
        }
        return data;
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
        } else if (result instanceof MongoOperationBulkWriteResult) {
            return toResult((MongoOperationBulkWriteResult) result);
        } else if (result instanceof MongoOperationUpdateResult) {
            return toResult((MongoOperationUpdateResult) result);
        } else if (result instanceof MongoOperationDeleteResult) {
            return toResult((MongoOperationDeleteResult) result);
        } else if (result instanceof MongoOperationInsertOneResult) {
            return toResult((MongoOperationInsertOneResult) result);
        } else if (result instanceof MongoOperationInsertManyResult) {
            return toResult((MongoOperationInsertManyResult) result);
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
        UpdateResult updateResult = operation.get();
        BsonDocument resultDoc = new BsonDocument("matchedCount", new BsonInt32((int) updateResult.getMatchedCount()));
        if (updateResult.isModifiedCountAvailable()) {
            resultDoc.append("modifiedCount", new BsonInt32((int) updateResult.getModifiedCount()));
        }
        if (updateResult.getUpsertedId() != null) {
            resultDoc.append("upsertedId", updateResult.getUpsertedId());
        }
        resultDoc.append("upsertedCount", updateResult.getUpsertedId() == null ? new BsonInt32(0) : new BsonInt32(1));
        return toResult(resultDoc);
    }

    private BsonDocument toResult(final MongoOperationBulkWriteResult operation) {
        BulkWriteResult bulkWriteResult = operation.get();
        BsonDocument resultDoc = new BsonDocument();
        if (bulkWriteResult.wasAcknowledged()) {
            resultDoc.append("deletedCount", new BsonInt32(bulkWriteResult.getDeletedCount()));
            resultDoc.append("insertedIds", new BsonDocument());
            resultDoc.append("matchedCount", new BsonInt32(bulkWriteResult.getMatchedCount()));
            if (bulkWriteResult.isModifiedCountAvailable()) {
                resultDoc.append("modifiedCount", new BsonInt32(bulkWriteResult.getModifiedCount()));
            }
            resultDoc.append("upsertedCount", bulkWriteResult.getUpserts() == null
                                                      ? new BsonInt32(0) : new BsonInt32(bulkWriteResult.getUpserts().size()));
            resultDoc.append("upsertedIds", new BsonDocument());
        }
        return toResult(resultDoc);
    }

    private BsonDocument toResult(final MongoOperationDeleteResult operation) {
        DeleteResult deleteResult = operation.get();
        return toResult(new BsonDocument("deletedCount", new BsonInt32((int) deleteResult.getDeletedCount())));
    }

    private BsonDocument toResult(final MongoOperationInsertOneResult operation) {
        InsertOneResult insertOneResult = operation.get();
        return toResult(new BsonDocument("insertedId", insertOneResult.id));
    }

    private BsonDocument toResult(final MongoOperationInsertManyResult operation) {
        InsertManyResult insertManyResult = operation.get();
        return toResult(new BsonDocument("insertedIds", insertManyResult.ids));
    }

    private BsonDocument toResult(final BsonValue results) {
        return new BsonDocument("result", results != null ? results : BsonNull.VALUE);
    }

    private AggregateIterable<BsonDocument> getAggregateMongoOperation(final BsonDocument arguments) {
        List<BsonDocument> pipeline = new ArrayList<BsonDocument>();
        for (BsonValue stage : arguments.getArray("pipeline")) {
            pipeline.add(stage.asDocument());
        }
        AggregateIterable<BsonDocument> iterable = collection.aggregate(pipeline);
        if (arguments.containsKey("batchSize")) {
            iterable.batchSize(arguments.getNumber("batchSize").intValue());
        }
        if (arguments.containsKey("collation")) {
            iterable.collation(getCollation(arguments.getDocument("collation")));
        }

        return iterable;
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
                if (arguments.containsKey("collation")) {
                    options.collation(getCollation(arguments.getDocument("collation")));
                }
                collection.count(arguments.getDocument("filter"), options, getCallback());
            }
        };
    }

    private DistinctIterable<BsonValue> getDistinctMongoOperation(final BsonDocument arguments) {
        DistinctIterable<BsonValue> iterable = collection.distinct(arguments.getString("fieldName").getValue(), BsonValue.class);
        if (arguments.containsKey("filter")) {
            iterable.filter(arguments.getDocument("filter"));
        }
        if (arguments.containsKey("collation")) {
            iterable.collation(getCollation(arguments.getDocument("collation")));
        }
        return iterable;
    }

    private FindIterable<BsonDocument> getFindMongoOperation(final BsonDocument arguments) {
        FindIterable<BsonDocument> iterable = collection.find(arguments.getDocument("filter"));
        if (arguments.containsKey("skip")) {
            iterable.skip(arguments.getNumber("skip").intValue());
        }
        if (arguments.containsKey("limit")) {
            iterable.limit(arguments.getNumber("limit").intValue());
        }
        if (arguments.containsKey("batchSize")) {
            iterable.batchSize(arguments.getNumber("batchSize").intValue());
        }
        if (arguments.containsKey("collation")) {
            iterable.collation(getCollation(arguments.getDocument("collation")));
        }
        return iterable;
    }

    private MongoOperationDeleteResult getDeleteManyMongoOperation(final BsonDocument arguments) {
        return new MongoOperationDeleteResult() {
            @Override
            public void execute() {
                DeleteOptions options = new DeleteOptions();
                if (arguments.containsKey("collation")) {
                    options.collation(getCollation(arguments.getDocument("collation")));
                }
                collection.deleteMany(arguments.getDocument("filter"), options, getCallback());
            }
        };
    }

    private MongoOperationDeleteResult getDeleteOneMongoOperation(final BsonDocument arguments) {
        return new MongoOperationDeleteResult() {
            @Override
            public void execute() {
                DeleteOptions options = new DeleteOptions();
                if (arguments.containsKey("collation")) {
                    options.collation(getCollation(arguments.getDocument("collation")));
                }
                collection.deleteOne(arguments.getDocument("filter"), options, getCallback());
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
                if (arguments.containsKey("collation")) {
                    options.collation(getCollation(arguments.getDocument("collation")));
                }
                collection.findOneAndDelete(arguments.getDocument("filter"), options, getCallback());
            }
        };
    }

    private MongoOperationBsonDocument getFindOneAndReplaceMongoOperation(final BsonDocument arguments) {
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
                if (arguments.containsKey("collation")) {
                    options.collation(getCollation(arguments.getDocument("collation")));
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
                if (arguments.containsKey("collation")) {
                    options.collation(getCollation(arguments.getDocument("collation")));
                }
                if (arguments.containsKey("arrayFilters")) {
                    options.arrayFilters((getArrayFilters(arguments.getArray("arrayFilters"))));
                }
                collection.findOneAndUpdate(arguments.getDocument("filter"), arguments.getDocument("update"), options, getCallback());
            }
        };
    }

    private MongoOperationInsertOneResult getInsertOneMongoOperation(final BsonDocument arguments) {
        return new MongoOperationInsertOneResult() {
            @Override
            public void execute() {
                final BsonDocument document = arguments.getDocument("document");
                collection.insertOne(document, new SingleResultCallback<Void>() {
                    @Override
                    public void onResult(final Void result, final Throwable t) {
                        if (t != null) {
                            getCallback().onResult(null, t);
                        } else {
                            getCallback().onResult(new InsertOneResult(document.get("_id")), null);
                        }

                    }
                });
            }
        };
    }

    private MongoOperationInsertManyResult getInsertManyMongoOperation(final BsonDocument arguments) {
        return new MongoOperationInsertManyResult() {
            @Override
            public void execute() {
                final List<BsonDocument> documents = new ArrayList<BsonDocument>();
                for (BsonValue document : arguments.getArray("documents")) {
                    documents.add(document.asDocument());
                }
                collection.insertMany(documents, new SingleResultCallback<Void>() {
                    @Override
                    public void onResult(final Void result, final Throwable t) {
                        if (t != null) {
                            getCallback().onResult(null, t);
                        } else {
                            BsonArray insertedIds = new BsonArray();
                            for (BsonDocument document : documents) {
                                insertedIds.add(document.get("_id"));
                            }
                            getCallback().onResult(new InsertManyResult(insertedIds), null);
                        }
                    }
                });
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
                if (arguments.containsKey("collation")) {
                    options.collation(getCollation(arguments.getDocument("collation")));
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
                if (arguments.containsKey("collation")) {
                    options.collation(getCollation(arguments.getDocument("collation")));
                }
                if (arguments.containsKey("arrayFilters")) {
                    options.arrayFilters((getArrayFilters(arguments.getArray("arrayFilters"))));
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
                if (arguments.containsKey("collation")) {
                    options.collation(getCollation(arguments.getDocument("collation")));
                }
                if (arguments.containsKey("arrayFilters")) {
                    options.arrayFilters((getArrayFilters(arguments.getArray("arrayFilters"))));
                }
                collection.updateOne(arguments.getDocument("filter"), arguments.getDocument("update"), options, getCallback());
            }
        };
    }

    private MongoOperationBulkWriteResult getBulkWriteMongoOperation(final BsonDocument arguments) {
        return new MongoOperationBulkWriteResult() {
            @Override
            public void execute() {
                List<WriteModel<BsonDocument>> writeModels = new ArrayList<WriteModel<BsonDocument>>();
                for (BsonValue bsonValue : arguments.getArray("requests")) {
                    BsonDocument cur = bsonValue.asDocument();
                    if (cur.containsKey("name")) {
                        String name = cur.getString("name").getValue();
                        BsonDocument requestArguments = cur.getDocument("arguments");
                        if (name.equals("insertOne")) {
                            writeModels.add(new InsertOneModel<BsonDocument>(requestArguments.getDocument("document")));
                        } else if (name.equals("updateOne")) {
                            writeModels.add(new UpdateOneModel<BsonDocument>(requestArguments.getDocument("filter"),
                                                                                    requestArguments.getDocument("update"),
                                                                                    getUpdateOptions(requestArguments)));
                        } else if (name.equals("updateMany")) {
                            writeModels.add(new UpdateManyModel<BsonDocument>(requestArguments.getDocument("filter"),
                                                                                     requestArguments.getDocument("update"),
                                                                                     getUpdateOptions(requestArguments)));
                        } else {
                            throw new UnsupportedOperationException("Unsupported write request type");
                        }
                    } else {
                        if (cur.get("insertOne") != null) {
                            BsonDocument insertOneArguments = cur.getDocument("insertOne");
                            writeModels.add(new InsertOneModel<BsonDocument>(insertOneArguments.getDocument("document")));
                        } else if (cur.get("updateOne") != null) {
                            BsonDocument updateOneArguments = cur.getDocument("updateOne");
                            writeModels.add(new UpdateOneModel<BsonDocument>(updateOneArguments.getDocument("filter"),
                                                                                    updateOneArguments.getDocument("update"),
                                                                                    getUpdateOptions(updateOneArguments)));
                        } else if (cur.get("updateMany") != null) {
                            BsonDocument updateManyArguments = cur.getDocument("updateMany");
                            writeModels.add(new UpdateManyModel<BsonDocument>(updateManyArguments.getDocument("filter"),
                                                                                     updateManyArguments.getDocument("update"),
                                                                                     getUpdateOptions(updateManyArguments)));
                        } else {
                            throw new UnsupportedOperationException("Unsupported write request type");
                        }
                    }
                }
                collection.bulkWrite(writeModels, getCallback());
            }
        };
    }


    Collation getCollation(final BsonDocument bsonCollation) {
        Collation.Builder builder = Collation.builder();
        if (bsonCollation.containsKey("locale")) {
            builder.locale(bsonCollation.getString("locale").getValue());
        }
        if (bsonCollation.containsKey("caseLevel")) {
            builder.caseLevel(bsonCollation.getBoolean("caseLevel").getValue());
        }
        if (bsonCollation.containsKey("caseFirst")) {
            builder.collationCaseFirst(CollationCaseFirst.fromString(bsonCollation.getString("caseFirst").getValue()));
        }
        if (bsonCollation.containsKey("strength")) {
            builder.collationStrength(CollationStrength.fromInt(bsonCollation.getInt32("strength").getValue()));
        }
        if (bsonCollation.containsKey("numericOrdering")) {
            builder.numericOrdering(bsonCollation.getBoolean("numericOrdering").getValue());
        }
        if (bsonCollation.containsKey("strength")) {
            builder.collationStrength(CollationStrength.fromInt(bsonCollation.getInt32("strength").getValue()));
        }
        if (bsonCollation.containsKey("alternate")) {
            builder.collationAlternate(CollationAlternate.fromString(bsonCollation.getString("alternate").getValue()));
        }
        if (bsonCollation.containsKey("maxVariable")) {
            builder.collationMaxVariable(CollationMaxVariable.fromString(bsonCollation.getString("maxVariable").getValue()));
        }
        if (bsonCollation.containsKey("normalization")) {
            builder.normalization(bsonCollation.getBoolean("normalization").getValue());
        }
        if (bsonCollation.containsKey("backwards")) {
            builder.backwards(bsonCollation.getBoolean("backwards").getValue());
        }
        return builder.build();
    }

    private UpdateOptions getUpdateOptions(final BsonDocument requestArguments) {
        return new UpdateOptions()
                       .arrayFilters(getArrayFilters(requestArguments.getArray("arrayFilters", null)));
    }


    private List<BsonDocument> getArrayFilters(final BsonArray bsonArray) {
        if (bsonArray == null) {
            return null;
        }
        List<BsonDocument> arrayFilters = new ArrayList<BsonDocument>(bsonArray.size());
        for (BsonValue cur : bsonArray) {
            arrayFilters.add(cur.asDocument());
        }
        return arrayFilters;
    }

    abstract class MongoOperationLong extends MongoOperation<Long> {
    }

    abstract class MongoOperationBsonDocument extends MongoOperation<BsonDocument> {
    }


    abstract class MongoOperationUpdateResult extends MongoOperation<UpdateResult> {
    }

    abstract class MongoOperationBulkWriteResult extends MongoOperation<BulkWriteResult> {
    }

    abstract class MongoOperationDeleteResult extends MongoOperation<DeleteResult> {
    }

    abstract class MongoOperationVoid extends MongoOperation<Void> {
    }

    abstract class MongoOperationInsertOneResult extends MongoOperation<InsertOneResult> {
    }

    abstract class MongoOperationInsertManyResult extends MongoOperation<InsertManyResult> {
    }

    private final class InsertOneResult {
        private BsonValue id;

        private InsertOneResult(final BsonValue id) {
            this.id = id;
        }
    }

    private final class InsertManyResult {
        private BsonArray ids;

        private InsertManyResult(final BsonArray ids) {
            this.ids = ids;
        }
    }
}
