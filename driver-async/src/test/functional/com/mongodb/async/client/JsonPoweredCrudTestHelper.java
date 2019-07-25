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


import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.MongoGridFSException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.async.client.gridfs.GridFSBucket;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.gridfs.model.GridFSDownloadOptions;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
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
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.AssumptionViolatedException;
import util.Hex;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.async.client.gridfs.helpers.AsyncStreamHelper.toAsyncInputStream;
import static com.mongodb.async.client.gridfs.helpers.AsyncStreamHelper.toAsyncOutputStream;
import static java.lang.String.format;

public class JsonPoweredCrudTestHelper {
    private final String description;
    private final MongoDatabase database;
    private final MongoCollection<BsonDocument> baseCollection;
    private final GridFSBucket gridFSBucket;
    private final MongoClient mongoClient;

    public JsonPoweredCrudTestHelper(final String description, final MongoDatabase database,
                                     final MongoCollection<BsonDocument> collection) {
        this(description, database, collection, null, null);
    }

    public JsonPoweredCrudTestHelper(final String description, final MongoDatabase database,
                                     final MongoCollection<BsonDocument> collection, final GridFSBucket gridFSBucket,
                                     final MongoClient mongoClient) {
        this.description = description;
        this.database = database;
        this.baseCollection = collection;
        this.gridFSBucket = gridFSBucket;
        this.mongoClient = mongoClient;
    }

    BsonDocument getOperationResults(final BsonDocument operation) {
        return getOperationResults(operation, null);
    }

    BsonDocument getOperationResults(final BsonDocument operation, @Nullable final ClientSession clientSession) {
        BsonDocument collectionOptions = operation.getDocument("collectionOptions", new BsonDocument());
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());

        String methodName = createMethodName(operation.getString("name").getValue(),
                operation.getString("object", new BsonString("")).getValue());
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

    private String createMethodName(final String name, final String object) {
        StringBuilder builder = new StringBuilder();
        builder.append("get");
        if (!object.isEmpty() && !object.equals("collection") && !object.equals("gridfsbucket")) {
            appendInitCapToBuilder(builder, object);
        }
        if (name.indexOf('_') >= 0) {
            String[] nameParts = name.split("_");
            for (String part : nameParts) {
                appendInitCapToBuilder(builder, part);
            }
        } else {
            appendInitCapToBuilder(builder, name);
        }
        builder.append("Result");
        return builder.toString();
    }

    private void appendInitCapToBuilder(final StringBuilder builder, final String object) {
        builder.append(object.substring(0, 1).toUpperCase());
        builder.append(object.substring(1));
    }

    <T> T futureResult(final FutureResultCallback<T> callback) {
        try {
            return callback.get();
        } catch (Throwable t) {
            throw MongoException.fromThrowable(t);
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
        resultDoc.append("modifiedCount", new BsonInt32((int) updateResult.getModifiedCount()));
        // If the upsertedId is an ObjectId that means it came from the server and can't be verified.
        // This check is to handle the "ReplaceOne with upsert when no documents match without an id specified" test
        // in replaceOne-pre_2.6
        if (updateResult.getUpsertedId() != null && !updateResult.getUpsertedId().isObjectId()) {
            resultDoc.append("upsertedId", updateResult.getUpsertedId());
        }
        resultDoc.append("upsertedCount", updateResult.getUpsertedId() == null ? new BsonInt32(0) : new BsonInt32(1));

        return toResult(resultDoc);
    }

    BsonDocument toResult(final BulkWriteResult bulkWriteResult, final List<? extends WriteModel<BsonDocument>> writeModels,
                          final List<BulkWriteError> writeErrors) {

        BsonDocument resultDoc = new BsonDocument();
        if (bulkWriteResult.wasAcknowledged()) {
            resultDoc.append("deletedCount", new BsonInt32(bulkWriteResult.getDeletedCount()));

            // Determine insertedIds
            BsonDocument insertedIds = new BsonDocument();
            for (int i = 0; i < writeModels.size(); i++) {
                WriteModel<BsonDocument> cur = writeModels.get(i);
                if (cur instanceof InsertOneModel && writeSuccessful(i, writeErrors)) {
                    InsertOneModel<BsonDocument> insertOneModel = (InsertOneModel<BsonDocument>) cur;
                    insertedIds.put(Integer.toString(i), insertOneModel.getDocument().get("_id"));
                }
            }
            resultDoc.append("insertedIds", insertedIds);
            resultDoc.append("insertedCount", new BsonInt32(insertedIds.size()));

            resultDoc.append("matchedCount", new BsonInt32(bulkWriteResult.getMatchedCount()));
            resultDoc.append("modifiedCount", new BsonInt32(bulkWriteResult.getModifiedCount()));
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

    private boolean writeSuccessful(final int index, final List<BulkWriteError> writeErrors) {
        for (BulkWriteError cur : writeErrors) {
            if (cur.getIndex() == index) {
                return false;
            }
        }
        return true;
    }

    BsonDocument toResult(@Nullable final BsonValue results) {
        return new BsonDocument("result", results != null ? results : BsonNull.VALUE);
    }

    BsonDocument getDatabaseRunCommandResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                                             @Nullable final ClientSession clientSession) {
        return getRunCommandResult(collectionOptions, arguments, clientSession);
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
        if (arguments.containsKey("maxTimeMS")) {
            iterable.maxTime(arguments.getNumber("maxTimeMS").longValue(), TimeUnit.MILLISECONDS);
        }
        return toResult(iterable);
    }

    BsonDocument getDatabaseAggregateResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                                            @Nullable final ClientSession clientSession) {
        List<BsonDocument> pipeline = new ArrayList<BsonDocument>();
        for (BsonValue stage : arguments.getArray("pipeline")) {
            pipeline.add(stage.asDocument());
        }

        AggregateIterable<BsonDocument> iterable;
        if (clientSession == null) {
            iterable = database.aggregate(pipeline, BsonDocument.class);
        } else {
            iterable = database.aggregate(clientSession, pipeline, BsonDocument.class);
        }

        if (arguments.containsKey("allowDiskUse")) {
            iterable.allowDiskUse(arguments.getBoolean("allowDiskUse").getValue());
        }
        if (arguments.containsKey("batchSize")) {
            iterable.batchSize(arguments.getNumber("batchSize").intValue());
        }
        if (arguments.containsKey("collation")) {
            iterable.collation(getCollation(arguments.getDocument("collation")));
        }

        BsonDocument results = toResult(iterable);
        for (BsonValue result : results.getArray("result", new BsonArray())) {
            if (result.isDocument()) {
                BsonDocument command = result.asDocument().getDocument("command", new BsonDocument());
                command.remove("$readPreference");
                command.remove("$clusterTime");
                command.remove("signature");
                command.remove("keyId");
            }
        }
        return results;
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

    BsonDocument getClientListDatabasesResult(final BsonDocument clientOptions, final BsonDocument arguments,
                                              @Nullable final ClientSession clientSession) {
        ListDatabasesIterable<BsonDocument> iterable;
        if (clientSession == null) {
            iterable = mongoClient.listDatabases(BsonDocument.class);
        } else {
            iterable = mongoClient.listDatabases(clientSession, BsonDocument.class);
        }
        return toResult(iterable);
    }

    BsonDocument getClientListDatabaseObjectsResult(final BsonDocument databaseOptions, final BsonDocument arguments,
                                                    @Nullable final ClientSession clientSession) {
        return getClientListDatabasesResult(databaseOptions, arguments, clientSession);
    }

    BsonDocument getClientListDatabaseNamesResult(final BsonDocument databaseOptions, final BsonDocument arguments,
                                                  @Nullable final ClientSession clientSession) {
        return getClientListDatabasesResult(databaseOptions, arguments, clientSession);
    }

    BsonDocument getDatabaseListCollectionObjectsResult(final BsonDocument databaseOptions, final BsonDocument arguments,
                                                        @Nullable final ClientSession clientSession) {
        return getDatabaseListCollectionsResult(databaseOptions, arguments, clientSession);
    }

    BsonDocument getDatabaseListCollectionNamesResult(final BsonDocument databaseOptions, final BsonDocument arguments,
                                                      @Nullable final ClientSession clientSession) {
        return getDatabaseListCollectionsResult(databaseOptions, arguments, clientSession);
    }

    BsonDocument getDatabaseListCollectionsResult(final BsonDocument databaseOptions, final BsonDocument arguments,
                                                  @Nullable final ClientSession clientSession) {
        ListCollectionsIterable<BsonDocument> iterable;
        if (clientSession == null) {
            iterable = database.listCollections(BsonDocument.class);
        } else {
            iterable = database.listCollections(clientSession, BsonDocument.class);
        }
        return toResult(iterable);
    }

    BsonDocument getListIndexesResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                                      @Nullable final ClientSession clientSession) {
        ListIndexesIterable<BsonDocument> iterable;
        if (clientSession == null) {
            iterable = getCollection(collectionOptions).listIndexes(BsonDocument.class);
        } else {
            iterable = getCollection(collectionOptions).listIndexes(clientSession, BsonDocument.class);
        }
        return toResult(iterable);
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
    BsonDocument getFindOneResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                                  @Nullable final ClientSession clientSession) {
        return getFindResult(collectionOptions, arguments, clientSession);
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

    BsonDocument getMapReduceResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                                    @Nullable final ClientSession clientSession) {
        MapReduceIterable<BsonDocument> iterable;
        if (clientSession == null) {
            iterable = getCollection(collectionOptions).mapReduce(arguments.get("map").asJavaScript().getCode(),
                    arguments.get("reduce").asJavaScript().getCode());
        } else {
            iterable = getCollection(collectionOptions).mapReduce(clientSession, arguments.get("map").asJavaScript().getCode(),
                    arguments.get("reduce").asJavaScript().getCode());
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
            options.arrayFilters((getListOfDocuments(arguments.getArray("arrayFilters"))));
        }

        FutureResultCallback<BsonDocument> futureResultCallback = new FutureResultCallback<BsonDocument>();
        if (clientSession == null) {
            if (arguments.isDocument("update")) {
                getCollection(collectionOptions).findOneAndUpdate(arguments.getDocument("filter"), arguments.getDocument("update"), options,
                        futureResultCallback);
            } else {
                getCollection(collectionOptions).findOneAndUpdate(arguments.getDocument("filter"),
                        getListOfDocuments(arguments.getArray("update")), options,
                        futureResultCallback);
            }
        } else {
            getCollection(collectionOptions).findOneAndUpdate(clientSession, arguments.getDocument("filter"),
                    arguments.getDocument("update"), options, futureResultCallback);
        }
        return toResult(futureResult(futureResultCallback));
    }

    BsonDocument getInsertOneResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                                    @Nullable final ClientSession clientSession) {
        BsonDocument document = arguments.getDocument("document");
        InsertOneOptions options = new InsertOneOptions();
        if (arguments.containsKey("bypassDocumentValidation")) {
            options.bypassDocumentValidation(arguments.getBoolean("bypassDocumentValidation").getValue());
        }
        FutureResultCallback<Void> futureResultCallback = new FutureResultCallback<Void>();
        if (clientSession == null) {
            getCollection(collectionOptions).insertOne(document, options, futureResultCallback);
        } else {
            getCollection(collectionOptions).insertOne(clientSession, document, options, futureResultCallback);
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

        try {
            InsertManyOptions options = new InsertManyOptions().ordered(arguments.getDocument("options", new BsonDocument())
                    .getBoolean("ordered", BsonBoolean.TRUE).getValue());
            if (arguments.containsKey("bypassDocumentValidation")) {
                options.bypassDocumentValidation(arguments.getBoolean("bypassDocumentValidation").getValue());
            }

            if (clientSession == null) {
                getCollection(collectionOptions).insertMany(documents, options, futureResultCallback);
            } else {
                getCollection(collectionOptions).insertMany(clientSession, documents, options, futureResultCallback);
            }
            futureResult(futureResultCallback);

            BsonDocument insertedIds = new BsonDocument();
            for (int i = 0; i < documents.size(); i++) {
                insertedIds.put(Integer.toString(i), documents.get(i).get("_id"));
            }
            return toResult(new BsonDocument("insertedIds", insertedIds));
        } catch (MongoBulkWriteException e) {
            // For transaction tests, the exception is expected to be returned.
            if (clientSession != null && clientSession.hasActiveTransaction()) {
                throw e;
            }
                // Test results are expecting this to look just like bulkWrite error, so translate to InsertOneModel so the result
                // translation code can be reused.
                List<InsertOneModel<BsonDocument>> writeModels = new ArrayList<InsertOneModel<BsonDocument>>();
                for (BsonValue document : arguments.getArray("documents")) {
                    writeModels.add(new InsertOneModel<BsonDocument>(document.asDocument()));
                }
                BsonDocument result = toResult(e.getWriteResult(), writeModels, e.getWriteErrors());
                result.put("error", BsonBoolean.TRUE);
                return result;
        }
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
        if (arguments.containsKey("bypassDocumentValidation")) {
            options.bypassDocumentValidation(arguments.getBoolean("bypassDocumentValidation").getValue());
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
            options.arrayFilters((getListOfDocuments(arguments.getArray("arrayFilters"))));
        }
        if (arguments.containsKey("bypassDocumentValidation")) {
            options.bypassDocumentValidation(arguments.getBoolean("bypassDocumentValidation").getValue());
        }

        FutureResultCallback<UpdateResult> futureResultCallback = new FutureResultCallback<UpdateResult>();
        if (clientSession == null) {
            if (arguments.isDocument("update")) {
                getCollection(collectionOptions).updateMany(arguments.getDocument("filter"), arguments.getDocument("update"), options,
                        futureResultCallback);
            } else {
                getCollection(collectionOptions).updateMany(arguments.getDocument("filter"),
                        getListOfDocuments(arguments.getArray("update")), options, futureResultCallback);
            }
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
            options.arrayFilters((getListOfDocuments(arguments.getArray("arrayFilters"))));
        }
        if (arguments.containsKey("bypassDocumentValidation")) {
            options.bypassDocumentValidation(arguments.getBoolean("bypassDocumentValidation").getValue());
        }

        FutureResultCallback<UpdateResult> futureResultCallback = new FutureResultCallback<UpdateResult>();
        if (clientSession == null) {
            if (arguments.isDocument("update")) {
                getCollection(collectionOptions).updateOne(arguments.getDocument("filter"), arguments.getDocument("update"), options,
                        futureResultCallback);
            } else {
                getCollection(collectionOptions).updateOne(arguments.getDocument("filter"),
                        getListOfDocuments(arguments.getArray("update")), options, futureResultCallback);
            }
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
                writeModels.add(new DeleteOneModel<BsonDocument>(requestArguments.getDocument("filter"),
                        getDeleteOptions(requestArguments)));
            } else if (name.equals("deleteMany")) {
                writeModels.add(new DeleteManyModel<BsonDocument>(requestArguments.getDocument("filter"),
                        getDeleteOptions(requestArguments)));
            } else if (name.equals("replaceOne")) {
                writeModels.add(new ReplaceOneModel<BsonDocument>(requestArguments.getDocument("filter"),
                        requestArguments.getDocument("replacement"), getReplaceOptions(requestArguments)));
            } else {
                throw new UnsupportedOperationException(format("Unsupported write request type: %s", name));
            }
        }

        try {
            FutureResultCallback<BulkWriteResult> futureResultCallback = new FutureResultCallback<BulkWriteResult>();
            BsonDocument optionsDocument = arguments.getDocument("options", new BsonDocument());
            BulkWriteOptions options = new BulkWriteOptions()
                    .ordered(optionsDocument.getBoolean("ordered", BsonBoolean.TRUE).getValue());
            if (optionsDocument.containsKey("bypassDocumentValidation")) {
                options.bypassDocumentValidation(optionsDocument.getBoolean("bypassDocumentValidation").getValue());
            }

            if (clientSession == null) {
                getCollection(collectionOptions).bulkWrite(writeModels, options, futureResultCallback);
            } else {
                getCollection(collectionOptions).bulkWrite(clientSession, writeModels, options, futureResultCallback);
            }
            return toResult(futureResult(futureResultCallback), writeModels, Collections.<BulkWriteError>emptyList());
        } catch (MongoBulkWriteException e) {
            BsonDocument result = toResult(e.getWriteResult(), writeModels, e.getWriteErrors());
            result.put("error", BsonBoolean.TRUE);
            return result;
        }
    }


    BsonDocument getRenameResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                                 @Nullable final ClientSession clientSession) {

        FutureResultCallback<Void> futureResultCallback = new FutureResultCallback<Void>();

        MongoNamespace toNamespace = new MongoNamespace(database.getName(), arguments.getString("to").getValue());
        if (clientSession == null) {
            getCollection(collectionOptions).renameCollection(toNamespace, futureResultCallback);
        } else {
            getCollection(collectionOptions).renameCollection(clientSession, toNamespace, futureResultCallback);
        }
        futureResult(futureResultCallback);
        return new BsonDocument("ok", new BsonInt32(1));
    }

    BsonDocument getDropResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                               @Nullable final ClientSession clientSession) {

        FutureResultCallback<Void> futureResultCallback = new FutureResultCallback<Void>();

        if (clientSession == null) {
            getCollection(collectionOptions).drop(futureResultCallback);
        } else {
            getCollection(collectionOptions).drop(clientSession, futureResultCallback);
        }
        futureResult(futureResultCallback);
        return new BsonDocument("ok", new BsonInt32(1));
    }

    // GridFSBucket operations

    BsonDocument getDownloadByNameResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                                         @Nullable final ClientSession clientSession) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {
            final GridFSDownloadOptions downloadOptions = new GridFSDownloadOptions();
            if (arguments.containsKey("options")) {
                int revision = arguments.getDocument("options").getInt32("revision").getValue();
                downloadOptions.revision(revision);
            }
            new MongoOperation<Long>() {
                @Override
                public void execute() {
                    gridFSBucket.downloadToStream(arguments.getString("filename").getValue(), toAsyncOutputStream(outputStream),
                            downloadOptions, getCallback());
                }
            }.get();

            return toResult("result", new BsonString(Hex.encode(outputStream.toByteArray()).toLowerCase()));
        } finally {
            outputStream.close();
        }
    }

    BsonDocument getDeleteResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                                 @Nullable final ClientSession clientSession) {
        try {
            new MongoOperation<Void>() {
                @Override
                public void execute() {
                    gridFSBucket.delete(arguments.getObjectId("id").getValue(), getCallback());
                }
            }.get();

            return new BsonDocument("ok", new BsonInt32(1));
        } catch (MongoGridFSException e) {
            BsonDocument result = toResult("message", new BsonString(e.getMessage()));
            result.put("error", BsonBoolean.TRUE);
            return result;
        }
    }

    BsonDocument getDownloadResult(final BsonDocument collectionOptions, final BsonDocument arguments,
                                   @Nullable final ClientSession clientSession) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {
            new MongoOperation<Long>() {
                @Override
                public void execute() {
                    gridFSBucket.downloadToStream(arguments.getObjectId("id").getValue(), toAsyncOutputStream(outputStream),
                            getCallback());
                }
            }.get();
        } finally {
            outputStream.close();
        }
        return toResult("result", new BsonString(Hex.encode(outputStream.toByteArray()).toLowerCase()));
    }

    BsonDocument getUploadResult(final BsonDocument collectionOptions, final BsonDocument rawArguments,
                                 @Nullable final ClientSession clientSession) {
        ObjectId objectId = null;
        BsonDocument arguments = parseHexDocument(rawArguments, "source");

        final GridFSBucket gridFSUploadBucket = gridFSBucket;
        final String filename = arguments.getString("filename").getValue();
        final InputStream inputStream = new ByteArrayInputStream(arguments.getBinary("source").getData());
        final GridFSUploadOptions options = new GridFSUploadOptions();
        BsonDocument rawOptions = arguments.getDocument("options", new BsonDocument());
        if (rawOptions.containsKey("chunkSizeBytes")) {
            options.chunkSizeBytes(rawOptions.getInt32("chunkSizeBytes").getValue());
        }
        if (rawOptions.containsKey("metadata")) {
            options.metadata(Document.parse(rawOptions.getDocument("metadata").toJson()));
        }
        if (rawOptions.containsKey("disableMD5")) {
            gridFSUploadBucket.withDisableMD5(rawOptions.getBoolean("disableMD5").getValue());
        }

        objectId = new MongoOperation<ObjectId>() {
            @Override
            public void execute() {
                gridFSUploadBucket.uploadFromStream(filename, toAsyncInputStream(inputStream), options, getCallback());
            }
        }.get();

        return new BsonDocument("objectId", new BsonObjectId(objectId));
    }

    // Change streams operations

    BsonDocument getClientWatchResult(final BsonDocument collectionOptions, final BsonDocument rawArguments,
                                      @Nullable final ClientSession clientSession) {
        new MongoOperation<AsyncBatchCursor<ChangeStreamDocument<Document>>>() {
            @Override
            public void execute() {
                mongoClient.watch().batchCursor(getCallback());
            }
        }.get();
        return new BsonDocument("ok", new BsonInt32(1));
    }

    BsonDocument getWatchResult(final BsonDocument collectionOptions, final BsonDocument rawArguments,
                                @Nullable final ClientSession clientSession) {
        new MongoOperation<AsyncBatchCursor<ChangeStreamDocument<BsonDocument>>>() {
            @Override
            public void execute() {
                baseCollection.watch().batchCursor(getCallback());
            }
        }.get();
        return new BsonDocument("ok", new BsonInt32(1));
    }

    BsonDocument getDatabaseWatchResult(final BsonDocument collectionOptions, final BsonDocument rawArguments,
                                        @Nullable final ClientSession clientSession) {
        new MongoOperation<AsyncBatchCursor<ChangeStreamDocument<Document>>>() {
            @Override
            public void execute() {
                database.watch().batchCursor(getCallback());
            }
        }.get();
        return new BsonDocument("ok", new BsonInt32(1));
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
            options.arrayFilters(getListOfDocuments(requestArguments.getArray("arrayFilters")));
        }
        if (requestArguments.containsKey("collation")) {
            options.collation(getCollation(requestArguments.getDocument("collation")));
        }
        return options;
    }

    private DeleteOptions getDeleteOptions(final BsonDocument requestArguments) {
        DeleteOptions options = new DeleteOptions();
        if (requestArguments.containsKey("collation")) {
            options.collation(getCollation(requestArguments.getDocument("collation")));
        }
        return options;
    }

    private ReplaceOptions getReplaceOptions(final BsonDocument requestArguments) {
        ReplaceOptions options = new ReplaceOptions();
        if (requestArguments.containsKey("upsert")) {
            options.upsert(true);
        }
        if (requestArguments.containsKey("collation")) {
            options.collation(getCollation(requestArguments.getDocument("collation")));
        }
        return options;
    }

    @Nullable
    private List<BsonDocument> getListOfDocuments(@Nullable final BsonArray bsonArray) {
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
        if (!writeConcernDocument.containsKey("w")) {
            throw new UnsupportedOperationException("Write concern document contains unexpected keys: " + writeConcernDocument.keySet());
        }

        WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;
        if (writeConcernDocument.isNumber("w")) {
            writeConcern = writeConcern.withW(writeConcernDocument.getNumber("w").intValue());
        } else {
            writeConcern = writeConcern.withW(writeConcernDocument.getString("w").getValue());
        }

        if (writeConcernDocument.containsKey("wtimeout")) {
            writeConcern = writeConcern.withWTimeout(writeConcernDocument.getNumber("wtimeout").longValue(), TimeUnit.MILLISECONDS);
        }
        if (writeConcernDocument.containsKey("j")) {
            writeConcern = writeConcern.withJ(writeConcernDocument.getBoolean("j").getValue());
        }
        return writeConcern;
    }

    ReadConcern getReadConcern(final BsonDocument arguments) {
        return new ReadConcern(ReadConcernLevel.fromString(arguments.getDocument("readConcern").getString("level").getValue()));
    }

    private BsonDocument parseHexDocument(final BsonDocument document, final String hexDocument) {
        if (document.containsKey(hexDocument) && document.get(hexDocument).isDocument()) {
            byte[] bytes = Hex.decode(document.getDocument(hexDocument).getString("$hex").getValue());
            document.put(hexDocument, new BsonBinary(bytes));
        }
        return document;
    }
}
