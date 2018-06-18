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


import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CollationAlternate;
import com.mongodb.client.model.CollationCaseFirst;
import com.mongodb.client.model.CollationMaxVariable;
import com.mongodb.client.model.CollationStrength;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.junit.AssumptionViolatedException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class JsonPoweredCrudTestHelper {
    private final String description;
    private final MongoDatabase database;
    private final MongoCollection<BsonDocument> baseCollection;

    public JsonPoweredCrudTestHelper(final String description, final MongoDatabase database,
                                     final MongoCollection<BsonDocument> collection) {
        this.description = description;
        this.database = database;
        this.baseCollection = collection;
    }

    BsonDocument getOperationResults(final BsonDocument operation) {
        return getOperationResults(operation, null);
    }

    BsonDocument getOperationResults(final BsonDocument operation, @Nullable final ClientSession clientSession) {
        String name = operation.getString("name").getValue();
        BsonDocument collectionOptions = operation.getDocument("collectionOptions", new BsonDocument());
        BsonDocument arguments = operation.getDocument("arguments");

        String methodName = "get" + name.substring(0, 1).toUpperCase() + name.substring(1) + "Result";
        try {
            Method method = getClass().getDeclaredMethod(methodName, BsonDocument.class, BsonDocument.class, ClientSession.class);
            return (BsonDocument) method.invoke(this, collectionOptions, arguments, clientSession);
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

    <T> T futureResult(final FutureResultCallback<T> callback) {
        try {
            return callback.get();
        } catch (RuntimeException e) {
            throw e;
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted", e);
        }
    }

    BsonDocument toResult(final int count) {
        return toResult(new BsonInt32(count));
    }

    BsonDocument toResult(final MongoIterable<BsonDocument> results) {
        FutureResultCallback<List<BsonDocument>> futureResultCallback = new FutureResultCallback<List<BsonDocument>>();
        results.into(new ArrayList<BsonDocument>(), futureResultCallback);
        return toResult(new BsonArray(futureResult(futureResultCallback)));
    }

    BsonDocument toResult(final String key, final BsonValue value) {
        return toResult(new BsonDocument(key, value));
    }

    BsonDocument toResult(final UpdateResult updateResult) {
        BsonDocument resultDoc = new BsonDocument("matchedCount", new BsonInt32((int) updateResult.getMatchedCount()));
        if (updateResult.isModifiedCountAvailable()) {
            resultDoc.append("modifiedCount", new BsonInt32((int) updateResult.getModifiedCount()));
        }
        // If the upsertedId is an ObjectId that means it came from the server and can't be verified.
        // This check is to handle the "ReplaceOne with upsert when no documents match without an id specified" test
        // in replaceOne-pre_2.6
        if (updateResult.getUpsertedId() != null && !updateResult.getUpsertedId().isObjectId()) {
            resultDoc.append("upsertedId", updateResult.getUpsertedId());
        }
        resultDoc.append("upsertedCount", updateResult.getUpsertedId() == null ? new BsonInt32(0) : new BsonInt32(1));

        return toResult(resultDoc);
    }

    BsonDocument toResult(final BulkWriteResult bulkWriteResult, final List<WriteModel<BsonDocument>> writeModels) {

        BsonDocument resultDoc = new BsonDocument();
        if (bulkWriteResult.wasAcknowledged()) {
            resultDoc.append("deletedCount", new BsonInt32(bulkWriteResult.getDeletedCount()));

            // Determine insertedIds
            BsonDocument insertedIds = new BsonDocument();
            for (int i = 0; i < writeModels.size(); i++) {
                WriteModel<BsonDocument> cur = writeModels.get(i);
                // TODO: Any need to suport InsertManyModel, and if so, how to represent it?
                if (cur instanceof InsertOneModel) {
                    InsertOneModel<BsonDocument> insertOneModel = (InsertOneModel<BsonDocument>) cur;
                    insertedIds.put(Integer.toString(i), insertOneModel.getDocument().get("_id"));
                }
            }
            resultDoc.append("insertedIds", insertedIds);

            resultDoc.append("matchedCount", new BsonInt32(bulkWriteResult.getMatchedCount()));
            if (bulkWriteResult.isModifiedCountAvailable()) {
                resultDoc.append("modifiedCount", new BsonInt32(bulkWriteResult.getModifiedCount()));
            }
            resultDoc.append("upsertedCount", bulkWriteResult.getUpserts() == null
                    ? new BsonInt32(0) : new BsonInt32(bulkWriteResult.getUpserts().size()));
            BsonDocument upserts = new BsonDocument();
            for (BulkWriteUpsert bulkWriteUpsert : bulkWriteResult.getUpserts()) {
                upserts.put(String.valueOf(bulkWriteUpsert.getIndex()), bulkWriteUpsert.getId());
            }
            resultDoc.append("upsertedIds", upserts);
        }
        return toResult(resultDoc);
    }

    BsonDocument toResult(@Nullable final BsonValue results) {
        return new BsonDocument("result", results != null ? results : BsonNull.VALUE);
    }

    BsonDocument getRunCommandResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                                     @Nullable final ClientSession clientSession) {
        BsonDocument command = arguments.getDocument("command");
        ReadPreference readPreference = arguments.containsKey("readPreference") ? getReadPreference(arguments) : null;

        FutureResultCallback<BsonDocument> futureResultCallback = new FutureResultCallback<BsonDocument>();

        if (clientSession == null) {
            if (readPreference == null) {
                database.runCommand(command, BsonDocument.class, futureResultCallback);
            } else {
                database.runCommand(command, readPreference, BsonDocument.class, futureResultCallback);
            }
        } else {
            if (readPreference == null) {
                database.runCommand(clientSession, command, BsonDocument.class, futureResultCallback);
            } else {
                database.runCommand(clientSession, command, readPreference, BsonDocument.class, futureResultCallback);
            }
        }
        BsonDocument response = futureResult(futureResultCallback);
        response.remove("ok");
        response.remove("operationTime");
        response.remove("opTime");
        response.remove("electionId");
        response.remove("$clusterTime");
        return toResult(response);
    }

    BsonDocument getAggregateResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                                    @Nullable final ClientSession clientSession) {
        List<BsonDocument> pipeline = new ArrayList<BsonDocument>();
        for (BsonValue stage : arguments.getArray("pipeline")) {
            pipeline.add(stage.asDocument());
        }

        AggregateIterable<BsonDocument> iterable;
        if (clientSession == null) {
            iterable = getCollection(collectionOptions).aggregate(pipeline);
        } else {
            iterable = getCollection(collectionOptions).aggregate(clientSession, pipeline);
        }

        if (arguments.containsKey("batchSize")) {
            iterable.batchSize(arguments.getNumber("batchSize").intValue());
        }
        if (arguments.containsKey("collation")) {
            iterable.collation(getCollation(arguments.getDocument("collation")));
        }
        return toResult(iterable);
    }

    @SuppressWarnings("deprecation")
    BsonDocument getCountResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                                @Nullable final ClientSession clientSession) {
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

        FutureResultCallback<Long> futureResultCallback = new FutureResultCallback<Long>();
        BsonDocument filter = arguments.getDocument("filter", new BsonDocument());
        if (clientSession == null) {
            getCollection(collectionOptions).count(filter, options, futureResultCallback);
        } else {
            getCollection(collectionOptions).count(clientSession, filter, options, futureResultCallback);
        }
        return toResult(futureResult(futureResultCallback).intValue());
    }

    BsonDocument getEstimatedDocumentCountResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                                                 @Nullable final ClientSession clientSession) {
        if (!arguments.isEmpty()) {
            throw new UnsupportedOperationException("Unexpected arguments: " + arguments);
        }
        FutureResultCallback<Long> futureResultCallback = new FutureResultCallback<Long>();
        getCollection(collectionOptions).estimatedDocumentCount(futureResultCallback);
        return toResult(futureResult(futureResultCallback).intValue());
    }

    BsonDocument getCountDocumentsResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                                         @Nullable final ClientSession clientSession) {
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
        BsonDocument filter = arguments.getDocument("filter", new BsonDocument());
        FutureResultCallback<Long> futureResultCallback = new FutureResultCallback<Long>();
        if (clientSession == null) {
            getCollection(collectionOptions).countDocuments(filter, options, futureResultCallback);
        } else {
            getCollection(collectionOptions).countDocuments(clientSession, filter, options, futureResultCallback);
        }
        return toResult(futureResult(futureResultCallback).intValue());
    }

    BsonDocument getDistinctResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                                   @Nullable final ClientSession clientSession) {
        DistinctIterable<BsonValue> iterable;
        if (clientSession == null) {
            iterable = getCollection(collectionOptions).distinct(arguments.getString("fieldName").getValue(), BsonValue.class);
        } else {
            iterable = getCollection(collectionOptions).distinct(clientSession, arguments.getString("fieldName").getValue(),
                    BsonValue.class);
        }

        if (arguments.containsKey("filter")) {
            iterable.filter(arguments.getDocument("filter"));
        }
        if (arguments.containsKey("collation")) {
            iterable.collation(getCollation(arguments.getDocument("collation")));
        }

        FutureResultCallback<List<BsonValue>> futureResultCallback = new FutureResultCallback<List<BsonValue>>();
        iterable.into(new BsonArray(), futureResultCallback);
        return toResult(new BsonArray(futureResult(futureResultCallback)));
    }

    @SuppressWarnings("deprecation")
    BsonDocument getFindResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                               @Nullable final ClientSession clientSession) {
        FindIterable<BsonDocument> iterable;
        if (clientSession == null) {
            iterable = getCollection(collectionOptions).find(arguments.getDocument("filter", new BsonDocument()));
        } else {
            iterable = getCollection(collectionOptions).find(clientSession, arguments.getDocument("filter", new BsonDocument()));
        }

        if (arguments.containsKey("skip")) {
            iterable.skip(arguments.getNumber("skip").intValue());
        }
        if (arguments.containsKey("limit")) {
            iterable.limit(arguments.getNumber("limit").intValue());
        }
        if (arguments.containsKey("batchSize")) {
            iterable.batchSize(arguments.getNumber("batchSize").intValue());
        }
        if (arguments.containsKey("sort")) {
            iterable.sort(arguments.getDocument("sort"));
        }
        if (arguments.containsKey("modifiers")) {
            iterable.modifiers(arguments.getDocument("modifiers"));
        }
        if (arguments.containsKey("collation")) {
            iterable.collation(getCollation(arguments.getDocument("collation")));
        }
        return toResult(iterable);
    }

    BsonDocument getDeleteManyResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                                     @Nullable final ClientSession clientSession) {
        DeleteOptions options = new DeleteOptions();
        if (arguments.containsKey("collation")) {
            options.collation(getCollation(arguments.getDocument("collation")));
        }

        FutureResultCallback<DeleteResult> futureResultCallback = new FutureResultCallback<DeleteResult>();
        if (clientSession == null) {
            getCollection(collectionOptions).deleteMany(arguments.getDocument("filter"), options, futureResultCallback);
        } else {
            getCollection(collectionOptions).deleteMany(clientSession, arguments.getDocument("filter"), options, futureResultCallback);
        }
        return toResult("deletedCount", new BsonInt32((int) futureResult(futureResultCallback).getDeletedCount()));
    }

    BsonDocument getDeleteOneResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                                    @Nullable final ClientSession clientSession) {
        DeleteOptions options = new DeleteOptions();
        if (arguments.containsKey("collation")) {
            options.collation(getCollation(arguments.getDocument("collation")));
        }

        FutureResultCallback<DeleteResult> futureResultCallback = new FutureResultCallback<DeleteResult>();
        if (clientSession == null) {
            getCollection(collectionOptions).deleteOne(arguments.getDocument("filter"), options, futureResultCallback);
        } else {
            getCollection(collectionOptions).deleteOne(clientSession, arguments.getDocument("filter"), options, futureResultCallback);
        }
        return toResult("deletedCount", new BsonInt32((int) futureResult(futureResultCallback).getDeletedCount()));
    }

    BsonDocument getFindOneAndDeleteResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                                           @Nullable final ClientSession clientSession) {
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

        FutureResultCallback<BsonDocument> futureResultCallback = new FutureResultCallback<BsonDocument>();
        if (clientSession == null) {
            getCollection(collectionOptions).findOneAndDelete(arguments.getDocument("filter"), options, futureResultCallback);
        } else {
            getCollection(collectionOptions).findOneAndDelete(clientSession, arguments.getDocument("filter"), options,
                    futureResultCallback);
        }
        return toResult(futureResult(futureResultCallback));
    }

    BsonDocument getFindOneAndReplaceResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                                            @Nullable final ClientSession clientSession) {
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

        FutureResultCallback<BsonDocument> futureResultCallback = new FutureResultCallback<BsonDocument>();
        if (clientSession == null) {
            getCollection(collectionOptions).findOneAndReplace(arguments.getDocument("filter"), arguments.getDocument("replacement"),
                    options, futureResultCallback);
        } else {
            getCollection(collectionOptions).findOneAndReplace(clientSession, arguments.getDocument("filter"),
                    arguments.getDocument("replacement"), options, futureResultCallback);
        }
        return toResult(futureResult(futureResultCallback));
    }

    BsonDocument getFindOneAndUpdateResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                                           @Nullable final ClientSession clientSession) {
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

        FutureResultCallback<BsonDocument> futureResultCallback = new FutureResultCallback<BsonDocument>();
        if (clientSession == null) {
            getCollection(collectionOptions).findOneAndUpdate(arguments.getDocument("filter"), arguments.getDocument("update"), options,
                    futureResultCallback);
        } else {
            getCollection(collectionOptions).findOneAndUpdate(clientSession, arguments.getDocument("filter"),
                    arguments.getDocument("update"), options, futureResultCallback);
        }
        return toResult(futureResult(futureResultCallback));
    }

    BsonDocument getInsertOneResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                                    @Nullable final ClientSession clientSession) {
        BsonDocument document = arguments.getDocument("document");

        FutureResultCallback<Void> futureResultCallback = new FutureResultCallback<Void>();
        if (clientSession == null) {
            getCollection(collectionOptions).insertOne(document, futureResultCallback);
        } else {
            getCollection(collectionOptions).insertOne(clientSession, document, futureResultCallback);
        }
        futureResult(futureResultCallback);
        return toResult(new BsonDocument("insertedId", document.get("_id")));
    }

    BsonDocument getInsertManyResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                                     @Nullable final ClientSession clientSession) {
        List<BsonDocument> documents = new ArrayList<BsonDocument>();
        for (BsonValue document : arguments.getArray("documents")) {
            documents.add(document.asDocument());
        }
        FutureResultCallback<Void> futureResultCallback = new FutureResultCallback<Void>();
        if (clientSession == null) {
            getCollection(collectionOptions).insertMany(documents, new InsertManyOptions().ordered(arguments.getBoolean("ordered",
                    BsonBoolean.TRUE).getValue()),
                    futureResultCallback);
        } else {
            getCollection(collectionOptions).insertMany(clientSession, documents,
                    new InsertManyOptions().ordered(arguments.getBoolean("ordered", BsonBoolean.TRUE).getValue()),
                    futureResultCallback);
        }
        futureResult(futureResultCallback);

        BsonDocument insertedIds = new BsonDocument();
        for (int i = 0; i < documents.size(); i++) {
            insertedIds.put(Integer.toString(i), documents.get(i).get("_id"));
        }
        return toResult(new BsonDocument("insertedIds", insertedIds));
    }

    BsonDocument getReplaceOneResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                                     @Nullable final ClientSession clientSession) {
        ReplaceOptions options = new ReplaceOptions();
        if (arguments.containsKey("upsert")) {
            options.upsert(arguments.getBoolean("upsert").getValue());
        }
        if (arguments.containsKey("collation")) {
            options.collation(getCollation(arguments.getDocument("collation")));
        }

        FutureResultCallback<UpdateResult> futureResultCallback = new FutureResultCallback<UpdateResult>();
        if (clientSession == null) {
            getCollection(collectionOptions).replaceOne(arguments.getDocument("filter"), arguments.getDocument("replacement"), options,
                    futureResultCallback);
        } else {
            getCollection(collectionOptions).replaceOne(clientSession, arguments.getDocument("filter"),
                    arguments.getDocument("replacement"), options, futureResultCallback);
        }
        return toResult(futureResult(futureResultCallback));
    }

    BsonDocument getUpdateManyResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                                     @Nullable final ClientSession clientSession) {
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
        FutureResultCallback<UpdateResult> futureResultCallback = new FutureResultCallback<UpdateResult>();
        if (clientSession == null) {
            getCollection(collectionOptions).updateMany(arguments.getDocument("filter"), arguments.getDocument("update"), options,
                    futureResultCallback);
        } else {
            getCollection(collectionOptions).updateMany(clientSession, arguments.getDocument("filter"), arguments.getDocument("update"),
                    options, futureResultCallback);
        }
        return toResult(futureResult(futureResultCallback));
    }

    @SuppressWarnings("unchecked")
    BsonDocument getUpdateOneResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                                    @Nullable final ClientSession clientSession) {
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
        FutureResultCallback<UpdateResult> futureResultCallback = new FutureResultCallback<UpdateResult>();
        if (clientSession == null) {
            getCollection(collectionOptions).updateOne(arguments.getDocument("filter"), arguments.getDocument("update"), options,
                    futureResultCallback);
        } else {
            getCollection(collectionOptions).updateOne(clientSession, arguments.getDocument("filter"), arguments.getDocument("update"),
                    options, futureResultCallback);
        }
        return toResult(futureResult(futureResultCallback));
    }

    BsonDocument getBulkWriteResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                                    @Nullable final ClientSession clientSession) {
        WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;
        if (arguments.containsKey("writeConcern")) {
            if (arguments.getDocument("writeConcern").size() > 1) {
                throw new UnsupportedOperationException("Write concern document contains unexpected keys: "
                        + arguments.getDocument("writeConcern").keySet());
            }
            writeConcern = new WriteConcern(arguments.getDocument("writeConcern").getInt32("w").intValue());
        }

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
                } else if (name.equals("deleteOne")) {
                    writeModels.add(new DeleteOneModel<BsonDocument>(requestArguments.getDocument("filter")));
                } else if (name.equals("deleteMany")) {
                    writeModels.add(new DeleteManyModel<BsonDocument>(requestArguments.getDocument("filter")));
                } else if (name.equals("replaceOne")) {
                    writeModels.add(new ReplaceOneModel<BsonDocument>(requestArguments.getDocument("filter"),
                            requestArguments.getDocument("replacement")));
                } else {
                    throw new UnsupportedOperationException(format("Unsupported write request type: %s", name));
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
                    throw new UnsupportedOperationException(format("Unsupported write request type: %s", cur.toJson()));
                }
            }
        }

        FutureResultCallback<BulkWriteResult> futureResultCallback = new FutureResultCallback<BulkWriteResult>();
        if (clientSession == null) {
            getCollection(collectionOptions).withWriteConcern(writeConcern).bulkWrite(writeModels,
                    new BulkWriteOptions().ordered(arguments.getBoolean("ordered", BsonBoolean.TRUE).getValue()),
                    futureResultCallback);
        } else {
            getCollection(collectionOptions).withWriteConcern(writeConcern).bulkWrite(clientSession, writeModels,
                    new BulkWriteOptions().ordered(arguments.getBoolean("ordered", BsonBoolean.TRUE).getValue()),
                    futureResultCallback);
        }
        return toResult(futureResult(futureResultCallback), writeModels);
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
        UpdateOptions options = new UpdateOptions();
        if (requestArguments.containsKey("upsert")) {
            options.upsert(true);
        }
        if (requestArguments.containsKey("arrayFilters")) {
            options.arrayFilters(getArrayFilters(requestArguments.getArray("arrayFilters")));
        }
        return options;
    }

    @Nullable
    private List<BsonDocument> getArrayFilters(@Nullable final BsonArray bsonArray) {
        if (bsonArray == null) {
            return null;
        }
        List<BsonDocument> arrayFilters = new ArrayList<BsonDocument>(bsonArray.size());
        for (BsonValue cur : bsonArray) {
            arrayFilters.add(cur.asDocument());
        }
        return arrayFilters;
    }

    private MongoCollection<BsonDocument> getCollection(final BsonDocument collectionOptions) {
        MongoCollection<BsonDocument> retVal = baseCollection;
        if (collectionOptions.containsKey("readPreference")) {
            retVal = retVal.withReadPreference(getReadPreference(collectionOptions));
        }

        if (collectionOptions.containsKey("writeConcern")) {
            WriteConcern writeConcern = getWriteConcern(collectionOptions);
            retVal = retVal.withWriteConcern(writeConcern);
        }

        if (collectionOptions.containsKey("readConcern")) {
            ReadConcern readConcern = getReadConcern(collectionOptions);
            retVal = retVal.withReadConcern(readConcern);
        }

        return retVal;
    }

    ReadPreference getReadPreference(final BsonDocument arguments) {
        return ReadPreference.valueOf(
                arguments.getDocument("readPreference").getString("mode").getValue());
    }

    WriteConcern getWriteConcern(final BsonDocument arguments) {
        BsonDocument writeConcernDocument = arguments.getDocument("writeConcern");
        if (writeConcernDocument.size() > 1) {
            throw new UnsupportedOperationException("Write concern document contains unexpected keys: " + writeConcernDocument.keySet());
        }
        if (writeConcernDocument.isNumber("w")) {
            return new WriteConcern(writeConcernDocument.getNumber("w").intValue());
        } else {
            return new WriteConcern(writeConcernDocument.getString("w").getValue());
        }
    }

    ReadConcern getReadConcern(final BsonDocument arguments) {
        return new ReadConcern(ReadConcernLevel.fromString(arguments.getDocument("readConcern").getString("level").getValue()));
    }
}
