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

import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.junit.Assume;
import org.junit.AssumptionViolatedException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.mongodb.ClusterFixture.serverVersionAtLeast;

public class JsonPoweredCrudTestHelper {
    private final String description;
    private final MongoCollection<BsonDocument> collection;

    public JsonPoweredCrudTestHelper(final String description, final MongoCollection<BsonDocument> collection) {
        this.description = description;
        this.collection = collection;
    }

    BsonDocument getOperationResults(final BsonDocument operation) {
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
            if (e.getTargetException() instanceof MongoException) {
                throw (MongoException) e.getTargetException();
            }
            throw (RuntimeException) e.getTargetException();
        } catch (IllegalAccessException e) {
            throw new UnsupportedOperationException("Invalid handler access for operation " + methodName);
        }
    }

    BsonDocument toResult(final int count) {
        return toResult(new BsonInt32(count));
    }

    BsonDocument toResult(final MongoIterable<BsonDocument> results) {
        return toResult(new BsonArray(results.into(new ArrayList<BsonDocument>())));
    }

    BsonDocument toResult(final String key, final BsonValue value) {
        return toResult(new BsonDocument(key, value));
    }

    BsonDocument toResult(final UpdateResult updateResult) {
        BsonDocument resultDoc = new BsonDocument("matchedCount", new BsonInt32((int) updateResult.getMatchedCount()));
        if (updateResult.isModifiedCountAvailable()) {
            resultDoc.append("modifiedCount", new BsonInt32((int) updateResult.getModifiedCount()));
        }
        if (updateResult.getUpsertedId() != null) {
            resultDoc.append("upsertedId", updateResult.getUpsertedId());
        }
        return toResult(resultDoc);
    }

    BsonDocument toResult(final BulkWriteResult bulkWriteResult) {
        BsonDocument resultDoc = new BsonDocument();  // TODO: complete this, but not needed for command monitoring tests
        return toResult(resultDoc);
    }

    BsonDocument toResult(final BsonValue results) {
        return new BsonDocument("result", results != null ? results : BsonNull.VALUE);
    }

    BsonDocument getAggregateResult(final BsonDocument arguments) {
        if (!serverVersionAtLeast(Arrays.asList(2, 6, 0))) {
            Assume.assumeFalse(description.contains("$out"));
        }

        List<BsonDocument> pipeline = new ArrayList<BsonDocument>();
        for (BsonValue stage : arguments.getArray("pipeline")) {
            pipeline.add(stage.asDocument());
        }
        return toResult(collection.aggregate(pipeline).batchSize(arguments.getNumber("batchSize").intValue()));
    }

    BsonDocument getCountResult(final BsonDocument arguments) {
        CountOptions options = new CountOptions();
        if (arguments.containsKey("skip")) {
            options.skip(arguments.getNumber("skip").intValue());
        }
        if (arguments.containsKey("limit")) {
            options.limit(arguments.getNumber("limit").intValue());
        }

        return toResult((int) collection.count(arguments.getDocument("filter"), options));
    }

    BsonDocument getDistinctResult(final BsonDocument arguments) {
        return toResult(collection.distinct(arguments.getString("fieldName").getValue(), BsonInt32.class)
                                .filter(arguments.getDocument("filter")).into(new BsonArray()));
    }

    BsonDocument getFindResult(final BsonDocument arguments) {
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
        if (arguments.containsKey("sort")) {
            findIterable.sort(arguments.getDocument("sort"));
        }
        if (arguments.containsKey("modifiers")) {
            findIterable.modifiers(arguments.getDocument("modifiers"));
        }
        return toResult(findIterable);
    }

    BsonDocument getDeleteManyResult(final BsonDocument arguments) {
        return toResult("deletedCount",
                        new BsonInt32((int) collection.deleteMany(arguments.getDocument("filter")).getDeletedCount()));
    }

    BsonDocument getDeleteOneResult(final BsonDocument arguments) {
        return toResult("deletedCount",
                        new BsonInt32((int) collection.deleteOne(arguments.getDocument("filter")).getDeletedCount()));
    }

    BsonDocument getFindOneAndDeleteResult(final BsonDocument arguments) {
        FindOneAndDeleteOptions options = new FindOneAndDeleteOptions();
        if (arguments.containsKey("projection")) {
            options.projection(arguments.getDocument("projection"));
        }
        if (arguments.containsKey("sort")) {
            options.sort(arguments.getDocument("sort"));
        }
        return toResult(collection.findOneAndDelete(arguments.getDocument("filter"), options));
    }

    BsonDocument getFindOneAndReplaceResult(final BsonDocument arguments) {
        // in 2.4 the server can ignore the supplied _id and creates an ObjectID
        Assume.assumeTrue(serverVersionAtLeast(Arrays.asList(2, 6, 0)));

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
        return toResult(collection
                                .findOneAndReplace(arguments.getDocument("filter"), arguments.getDocument("replacement"), options));
    }

    BsonDocument getFindOneAndUpdateResult(final BsonDocument arguments) {
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
        return toResult(collection
                                .findOneAndUpdate(arguments.getDocument("filter"), arguments.getDocument("update"), options));
    }

    BsonDocument getInsertOneResult(final BsonDocument arguments) {
        collection.insertOne(arguments.getDocument("document"));
        return toResult((BsonValue) null);
    }

    BsonDocument getInsertManyResult(final BsonDocument arguments) {
        List<BsonDocument> documents = new ArrayList<BsonDocument>();
        for (BsonValue document : arguments.getArray("documents")) {
            documents.add(document.asDocument());
        }
        collection.insertMany(documents,
                              new InsertManyOptions().ordered(arguments.getBoolean("ordered", BsonBoolean.TRUE).getValue()));
        return toResult((BsonValue) null);
    }

    BsonDocument getReplaceOneResult(final BsonDocument arguments) {
        UpdateOptions options = new UpdateOptions();
        if (arguments.containsKey("upsert")) {
            options.upsert(arguments.getBoolean("upsert").getValue());
        }
        return toResult(collection
                                .replaceOne(arguments.getDocument("filter"), arguments.getDocument("replacement"), options));
    }

    BsonDocument getUpdateManyResult(final BsonDocument arguments) {
        UpdateOptions options = new UpdateOptions();
        if (arguments.containsKey("upsert")) {
            options.upsert(arguments.getBoolean("upsert").getValue());
        }
        return toResult(collection.updateMany(arguments.getDocument("filter"), arguments.getDocument("update"), options));
    }

    BsonDocument getUpdateOneResult(final BsonDocument arguments) {
        UpdateOptions options = new UpdateOptions();
        if (arguments.containsKey("upsert")) {
            options.upsert(arguments.getBoolean("upsert").getValue());
        }
        return toResult(collection.updateOne(arguments.getDocument("filter"), arguments.getDocument("update"), options));
    }

    BsonDocument getBulkWriteResult(final BsonDocument arguments) {
        WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;
        if (arguments.containsKey("writeConcern")) {
            if (arguments.getDocument("writeConcern").size() > 1) {
                throw new UnsupportedOperationException("Write concern document contains unexpected keys: "
                                                        + arguments.getDocument("writeConcern").keySet());
            }
            writeConcern = new WriteConcern(arguments.getDocument("writeConcern").getInt32("w").intValue());
        }

        List<WriteModel<BsonDocument>> writeModels = new ArrayList<WriteModel<BsonDocument>>();
        for (Iterator<BsonValue> iter = arguments.getArray("requests").iterator(); iter.hasNext();) {
            BsonDocument cur = iter.next().asDocument();
            if (cur.get("insertOne") != null) {
                writeModels.add(new InsertOneModel<BsonDocument>(cur.getDocument("insertOne").getDocument("document")));
            } else if (cur.get("updateOne") != null) {
                writeModels.add(new UpdateOneModel<BsonDocument>(cur.getDocument("updateOne").getDocument("filter"),
                                                                 cur.getDocument("updateOne").getDocument("update")));
            } else {
                throw new UnsupportedOperationException("Unsupported write request type");
            }
        }

        return toResult(collection.withWriteConcern(writeConcern).bulkWrite(writeModels,
                                                                            new BulkWriteOptions()
                                                                            .ordered(arguments.getBoolean("ordered", BsonBoolean.TRUE)
                                                                                              .getValue())));

    }
}
