/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.unified;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
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
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

final class UnifiedCrudHelper {
    private final Entities entities;

    UnifiedCrudHelper(final Entities entities) {
        this.entities = entities;
    }

    OperationResult executeListDatabases(final BsonDocument operation) {
        MongoClient client = entities.getClient(operation.getString("object").getValue());

        if (operation.containsKey("arguments")) {
            throw new UnsupportedOperationException("Unexpected arguments");
        }

        try {
            return OperationResult.of(new BsonArray(client.listDatabases(BsonDocument.class).into(new ArrayList<>())));
        } catch (Exception e) {
            return OperationResult.of(e);
        }
    }

    OperationResult executeFind(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");

        BsonDocument filter = arguments.getDocument("filter");
        FindIterable<BsonDocument> iterable = collection.find(filter);
        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "filter":
                    break;
                case "sort":
                    iterable.sort(cur.getValue().asDocument());
                    break;
                case "batchSize":
                    iterable.batchSize(cur.getValue().asInt32().intValue());
                    break;
                case "limit":
                    iterable.limit(cur.getValue().asInt32().intValue());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }
        try {
            return OperationResult.of(new BsonArray(iterable.into(new ArrayList<>())));
        } catch (Exception e) {
            return OperationResult.of(e);
        }
    }

    OperationResult executeFindOneAndUpdate(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");

        BsonDocument filter = null;
        BsonDocument update = null;
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "filter":
                    filter = cur.getValue().asDocument();
                    break;
                case "update":
                    update = cur.getValue().asDocument();
                    break;
                case "returnDocument":
                    switch (cur.getValue().asString().getValue()) {
                        case "Before":
                            options.returnDocument(ReturnDocument.BEFORE);
                            break;
                        case "After":
                            options.returnDocument(ReturnDocument.AFTER);
                            break;
                        default:
                            throw new UnsupportedOperationException("Can't happen");
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }
        requireNonNull(filter);
        requireNonNull(update);

        try {
            BsonDocument result = collection.findOneAndUpdate(filter, update, options);
            return OperationResult.of(result);
        } catch (Exception e) {
            return OperationResult.of(e);
        }
    }

    OperationResult executeAggregate(final BsonDocument operation) {
        String entityName = operation.getString("object").getValue();

        BsonDocument arguments = operation.getDocument("arguments");
        List<BsonDocument> pipeline = arguments.getArray("pipeline").stream().map(BsonValue::asDocument).collect(toList());
        AggregateIterable<BsonDocument> iterable;
        if (entities.hasDatabase(entityName)) {
            iterable = entities.getDatabase(entityName).aggregate(requireNonNull(pipeline), BsonDocument.class);
        } else if (entities.hasCollection(entityName)) {
            iterable = entities.getCollection(entityName).aggregate(requireNonNull(pipeline));
        } else {
            throw new UnsupportedOperationException("Unsupported entity type with name: " + entityName);
        }
        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            //noinspection SwitchStatementWithTooFewBranches
            switch (cur.getKey()) {
                case "pipeline":
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }
        try {
            if (pipeline.get(pipeline.size() - 1).getFirstKey().equals("$out")) {
                iterable.toCollection();
                return OperationResult.NONE;
            } else {
                return OperationResult.of(new BsonArray(iterable.into(new ArrayList<>())));
            }
        } catch (Exception e) {
            return OperationResult.of(e);
        }
    }

    OperationResult executeDeleteOne(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");
        BsonDocument filter = arguments.getDocument("filter");

        if (operation.getDocument("arguments").size() > 1) {
            throw new UnsupportedOperationException("Unexpected arguments");
        }

        try {
            DeleteResult result = collection.deleteOne(filter);
            return toExpected(result);
        } catch (Exception e) {
            return OperationResult.of(e);
        }
    }

    private OperationResult toExpected(final DeleteResult result) {
        return OperationResult.of(new BsonDocument()
                .append("deletedCount", new BsonInt32((int) result.getDeletedCount()))
        );
    }

    OperationResult executeReplaceOne(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");
        BsonDocument filter = arguments.getDocument("filter");
        BsonDocument replacement = arguments.getDocument("replacement");
        ReplaceOptions options = getReplaceOptions(arguments);

        if (operation.getDocument("arguments").size() > 2) {
            throw new UnsupportedOperationException("Unexpected arguments");
        }

        try {
            UpdateResult result = collection.replaceOne(filter, replacement, options);
            return toExpected(result);
        } catch (Exception e) {
            return OperationResult.of(e);
        }
    }

    private OperationResult toExpected(final UpdateResult result) {
        return OperationResult.of(new BsonDocument()
                .append("matchedCount", new BsonInt32((int) result.getMatchedCount()))
                .append("modifiedCount", new BsonInt32((int) result.getModifiedCount()))
                .append("upsertedId", result.getUpsertedId())
        );
    }


    OperationResult executeInsertOne(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        ClientSession session = null;
        BsonDocument document = null;
        InsertOneOptions options = new InsertOneOptions();

        for (Map.Entry<String, BsonValue> cur : operation.getDocument("arguments").entrySet()) {
            switch (cur.getKey()) {
                case "session":
                    session = entities.getSession(cur.getValue().asString().getValue());
                    break;
                case "document":
                    document = cur.getValue().asDocument();
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }
        requireNonNull(document);

        try {
            InsertOneResult result = session == null
                    ? collection.insertOne(document, options)
                    : collection.insertOne(session, document, options);
            return toExpected(result);
        } catch (Exception e) {
            return OperationResult.of(e);
        }
    }

    private OperationResult toExpected(final InsertOneResult result) {
        return OperationResult.of(new BsonDocument()
                .append("insertedId", result.getInsertedId()));
    }

    OperationResult executeInsertMany(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        List<BsonDocument> documents = null;
        InsertManyOptions options = new InsertManyOptions();

        for (Map.Entry<String, BsonValue> cur : operation.getDocument("arguments").entrySet()) {
            switch (cur.getKey()) {
                case "documents":
                    documents = cur.getValue().asArray().stream().map(BsonValue::asDocument).collect(toList());
                    break;
                case "ordered":
                    options.ordered(cur.getValue().asBoolean().getValue());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }
        requireNonNull(documents);

        try {
            InsertManyResult result = collection.insertMany(documents, options);
            return toExpected(result);
        } catch (Exception e) {
            return OperationResult.of(e);
        }
    }

    private OperationResult toExpected(final InsertManyResult result) {
        return OperationResult.of(new BsonDocument()
                .append("insertedIds", new BsonDocument(result.getInsertedIds().entrySet().stream()
                        .map(value -> new BsonElement(value.getKey().toString(), value.getValue())).collect(toList())))
        );
    }

    OperationResult executeBulkWrite(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        List<WriteModel<BsonDocument>> requests = null;
        BulkWriteOptions options = new BulkWriteOptions();
        for (Map.Entry<String, BsonValue> cur : operation.getDocument("arguments").entrySet()) {
            switch (cur.getKey()) {
                case "requests":
                    requests = cur.getValue().asArray().stream().map(value -> toWriteModel(value.asDocument())).collect(toList());
                    break;
                case "ordered":
                    options.ordered(cur.getValue().asBoolean().getValue());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        requireNonNull(requests);

        try {
            BulkWriteResult result = collection.bulkWrite(requests, options);
            return toExpected(result);
        } catch (Exception e) {
           return OperationResult.of(e);
        }
    }

    private OperationResult toExpected(final BulkWriteResult result) {
        return OperationResult.of(new BsonDocument()
                .append("deletedCount", new BsonInt32(result.getDeletedCount()))
                .append("insertedCount", new BsonInt32(result.getInsertedCount()))
                .append("matchedCount", new BsonInt32(result.getMatchedCount()))
                .append("modifiedCount", new BsonInt32(result.getModifiedCount()))
                .append("upsertedCount", new BsonInt32(result.getUpserts().size()))
                .append("insertedIds", new BsonDocument(result.getInserts().stream()
                        .map(value -> new BsonElement(Integer.toString(value.getIndex()), value.getId())).collect(toList())))
                .append("upsertedIds", new BsonDocument(result.getUpserts().stream()
                        .map(value -> new BsonElement(Integer.toString(value.getIndex()), value.getId())).collect(toList()))));
    }

    private WriteModel<BsonDocument> toWriteModel(final BsonDocument document) {

        String requestType = document.getFirstKey();
        BsonDocument arguments = document.getDocument(requestType);
        switch (requestType) {
            case "insertOne":
                return new InsertOneModel<>(arguments.getDocument("document"));
            case "updateOne":
                return new UpdateOneModel<>(arguments.getDocument("filter"), arguments.getDocument("update"),
                        getUpdateOptions());
            case "updateMany":
                return new UpdateManyModel<>(arguments.getDocument("filter"), arguments.getDocument("update"),
                        getUpdateOptions());
            case "deleteOne":
                return new DeleteOneModel<>(arguments.getDocument("filter"), getDeleteOptions());
            case "deleteMany":
                return new DeleteManyModel<>(arguments.getDocument("filter"), getDeleteOptions());
            case "replaceOne":
                return new ReplaceOneModel<>(arguments.getDocument("filter"), arguments.getDocument("replacement"),
                        getReplaceOptions(arguments));
            default:
                throw new UnsupportedOperationException("Unsupported write model type: " + requestType);
        }
    }

    @NotNull
    private DeleteOptions getDeleteOptions() {
        return new DeleteOptions();
    }

    private UpdateOptions getUpdateOptions() {
        return new UpdateOptions();
    }

    private ReplaceOptions getReplaceOptions(final BsonDocument arguments) {
        ReplaceOptions options = new ReplaceOptions();
        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "filter":
                case "replacement":
                    break;
                case "upsert":
                    options.upsert(cur.getValue().asBoolean().getValue());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }
        return options;
    }

    OperationResult executeStartTransaction(final BsonDocument operation) {
        ClientSession session = entities.getSession(operation.getString("object").getValue());

        if (operation.containsKey("arguments")) {
            throw new UnsupportedOperationException("Unexpected arguments");
        }

        try {
            session.startTransaction();
            return OperationResult.NONE;
        } catch (Exception e) {
            return OperationResult.of(e);
        }
    }

    OperationResult executeCommitTransaction(final BsonDocument operation) {
        ClientSession session = entities.getSession(operation.getString("object").getValue());

        if (operation.containsKey("arguments")) {
            throw new UnsupportedOperationException("Unexpected arguments");
        }

        try {
            session.commitTransaction();
            return OperationResult.NONE;
        } catch (Exception e) {
            return OperationResult.of(e);
        }
    }

    OperationResult executeAbortTransaction(final BsonDocument operation) {
        ClientSession session = entities.getSession(operation.getString("object").getValue());

        if (operation.containsKey("arguments")) {
            throw new UnsupportedOperationException("Unexpected arguments");
        }

        try {
            session.abortTransaction();
            return OperationResult.NONE;
        } catch (Exception e) {
            return OperationResult.of(e);
        }
    }

    public OperationResult executeDropCollection(final BsonDocument operation) {
        MongoDatabase database = entities.getDatabase(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");
        String collectionName = arguments.getString("collection").getValue();

        if (operation.getDocument("arguments").size() > 1) {
            throw new UnsupportedOperationException("Unexpected arguments");
        }

        try {
            database.getCollection(collectionName).drop();
            return OperationResult.NONE;
        } catch (Exception e) {
            return OperationResult.of(e);
        }
    }

    public OperationResult executeCreateCollection(final BsonDocument operation) {
        MongoDatabase database = entities.getDatabase(operation.getString("object").getValue());
        String collectionName = null;
        ClientSession session = null;

        for (Map.Entry<String, BsonValue> cur : operation.getDocument("arguments").entrySet()) {
            switch (cur.getKey()) {
                case "collection":
                    collectionName = cur.getValue().asString().getValue();
                    break;
                case "session":
                    session = entities.getSession(cur.getValue().asString().getValue());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        requireNonNull(collectionName);

        try {
            if (session == null) {
                database.createCollection(collectionName);
            } else {
                database.createCollection(session, collectionName);
            }
            return OperationResult.NONE;
        } catch (Exception e) {
            return OperationResult.of(e);
        }
    }

    public OperationResult executeCreateIndex(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument keys = null;
        ClientSession session = null;
        IndexOptions options = new IndexOptions();

        for (Map.Entry<String, BsonValue> cur : operation.getDocument("arguments").entrySet()) {
            switch (cur.getKey()) {
                case "name":
                    options.name(cur.getValue().asString().getValue());
                    break;
                case "keys":
                    keys = cur.getValue().asDocument();
                    break;
                case "session":
                    session = entities.getSession(cur.getValue().asString().getValue());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        requireNonNull(keys);

        try {
            if (session == null) {
                collection.createIndex(keys, options);
            } else {
                collection.createIndex(session, keys, options);
            }
            return OperationResult.NONE;
        } catch (Exception e) {
            return OperationResult.of(e);
        }
    }

    public OperationResult executeChangeStream(final BsonDocument operation) {
        String entityName = operation.getString("object").getValue();
        ChangeStreamIterable<BsonDocument> iterable;
        if (entities.hasCollection(entityName)) {
            iterable = entities.getCollection(entityName).watch();
        } else if (entities.hasDatabase(entityName)) {
            iterable = entities.getDatabase(entityName).watch(BsonDocument.class);
        } else if (entities.hasClient(entityName)) {
            iterable = entities.getClient(entityName).watch(BsonDocument.class);
        } else {
            throw new UnsupportedOperationException("No entity found for id: " + entityName);
        }

        for (Map.Entry<String, BsonValue> cur : operation.getDocument("arguments", new BsonDocument()).entrySet()) {
            //noinspection SwitchStatementWithTooFewBranches
            switch (cur.getKey()) {
                case "batchSize":
                    iterable.batchSize(cur.getValue().asNumber().intValue());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        try {
            MongoCursor<BsonDocument> cursor = iterable.withDocumentClass(BsonDocument.class).cursor();
            entities.addChangeStream(operation.getString("saveResultAsEntity").getValue(), cursor);
            return OperationResult.NONE;
        } catch (Exception e) {
            return OperationResult.of(e);
        }
    }

    public OperationResult executeIterateUntilDocumentOrError(final BsonDocument operation) {
        MongoCursor<BsonDocument> cursor = entities.getChangeStream(operation.getString("object").getValue());

        if (operation.containsKey("arguments")) {
            throw new UnsupportedOperationException("Unexpected arguments");
        }

        try {
            return OperationResult.of(cursor.next());
        } catch (Exception e) {
            return OperationResult.of(e);
        }
    }
}
