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

import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
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
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonElement;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

final class UnifiedCrudHelper {
    private final Entities entities;
    private final Codec<ChangeStreamDocument<BsonDocument>> changeStreamDocumentCodec = ChangeStreamDocument.createCodec(
            BsonDocument.class,
            CodecRegistries.fromProviders(asList(new BsonCodecProvider(), new ValueCodecProvider())));

    UnifiedCrudHelper(final Entities entities) {
        this.entities = entities;
    }

    static ReadConcern asReadConcern(final BsonDocument readConcernDocument) {
        if (readConcernDocument.size() > 1) {
            throw new UnsupportedOperationException("Unsupported read concern properties");
        }
        return new ReadConcern(ReadConcernLevel.fromString(readConcernDocument.getString("level").getValue()));
    }

    static WriteConcern asWriteConcern(final BsonDocument writeConcernDocument) {
        if (writeConcernDocument.size() > 1) {
            throw new UnsupportedOperationException("Unsupported write concern properties");
        }
        return new WriteConcern(writeConcernDocument.getInt32("w").intValue());
    }

    public static ReadPreference asReadPreference(final BsonDocument readPreferenceDocument) {
        if (readPreferenceDocument.size() > 1) {
            throw new UnsupportedOperationException("Unsupported read preference properties");
        }

        return ReadPreference.valueOf(readPreferenceDocument.getString("mode").getValue());
    }

    private OperationResult resultOf(final Supplier<BsonValue> operationResult) {
        try {
            return OperationResult.of(operationResult.get());
        } catch (Exception e) {
            return OperationResult.of(e);
        }
    }

    private ClientSession getSession(final BsonDocument arguments) {
        if (arguments.containsKey("session")) {
            return entities.getSession(arguments.getString("session").asString().getValue());
        } else {
            return null;
        }
    }


    OperationResult executeListDatabases(final BsonDocument operation) {
        MongoClient client = entities.getClient(operation.getString("object").getValue());

        if (operation.containsKey("arguments")) {
            throw new UnsupportedOperationException("Unexpected arguments");
        }

        return resultOf(() ->
                new BsonArray(client.listDatabases(BsonDocument.class).into(new ArrayList<>())));
    }

    OperationResult executeListCollections(final BsonDocument operation) {
        MongoDatabase database = entities.getDatabase(operation.getString("object").getValue());

        BsonDocument arguments = operation.getDocument("arguments");
        ListCollectionsIterable<BsonDocument> iterable = database.listCollections(BsonDocument.class);
        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "filter":
                    iterable.filter(cur.getValue().asDocument());
                    break;
                case "batchSize":
                    iterable.batchSize(cur.getValue().asNumber().intValue());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() ->
                new BsonArray(iterable.into(new ArrayList<>())));
    }

    OperationResult executeListIndexes(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");
        ListIndexesIterable<BsonDocument> iterable = collection.listIndexes(BsonDocument.class);
        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            //noinspection SwitchStatementWithTooFewBranches
            switch (cur.getKey()) {
                case "batchSize":
                    iterable.batchSize(cur.getValue().asNumber().intValue());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() ->
                new BsonArray(iterable.into(new ArrayList<>())));
    }

    OperationResult executeFind(final BsonDocument operation) {
        FindIterable<BsonDocument> iterable = createFindIterable(operation);
        return resultOf(() ->
                new BsonArray(iterable.into(new ArrayList<>())));
    }

    OperationResult createFindCursor(final BsonDocument operation) {
        FindIterable<BsonDocument> iterable = createFindIterable(operation);
        return resultOf(() -> {
            entities.addCursor(operation.getString("saveResultAsEntity").getValue(), iterable.cursor());
            return null;
        });
    }

    @NotNull
    private FindIterable<BsonDocument> createFindIterable(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");
        ClientSession session = getSession(arguments);
        BsonDocument filter = arguments.getDocument("filter");
        FindIterable<BsonDocument> iterable = session == null ? collection.find(filter) : collection.find(session, filter);
        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "session":
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
        return iterable;
    }

    OperationResult executeDistinct(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");

        BsonString fieldName = arguments.getString("fieldName");
        DistinctIterable<BsonValue> iterable = collection.distinct(fieldName.getValue(), BsonValue.class);

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "fieldName":
                    break;
                case "filter":
                    iterable.filter(cur.getValue().asDocument());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() ->
                new BsonArray(iterable.into(new ArrayList<>())));
    }

    OperationResult executeFindOneAndUpdate(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");

        BsonDocument filter = arguments.getDocument("filter").asDocument();
        BsonDocument update = arguments.getDocument("update").asDocument();
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "filter":
                case "update":
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

        return resultOf(() ->
                collection.findOneAndUpdate(filter, update, options));
    }

    OperationResult executeFindOneAndReplace(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");

        BsonDocument filter = arguments.getDocument("filter").asDocument();
        BsonDocument replacement = arguments.getDocument("replacement").asDocument();
        FindOneAndReplaceOptions options = new FindOneAndReplaceOptions();

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "filter":
                case "replacement":
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

        return resultOf(() ->
                collection.findOneAndReplace(filter, replacement, options));
    }

    OperationResult executeFindOneAndDelete(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");

        BsonDocument filter = arguments.getDocument("filter").asDocument();
        FindOneAndDeleteOptions options = new FindOneAndDeleteOptions();

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            //noinspection SwitchStatementWithTooFewBranches
            switch (cur.getKey()) {
                case "filter":
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() ->
                collection.findOneAndDelete(filter, options));
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
            switch (cur.getKey()) {
                case "pipeline":
                    break;
                case "batchSize":
                    iterable.batchSize(cur.getValue().asNumber().intValue());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }
        return resultOf(() -> {
            if (!pipeline.isEmpty() && pipeline.get(pipeline.size() - 1).getFirstKey().equals("$out")) {
                iterable.toCollection();
                return null;
            } else {
                return new BsonArray(iterable.into(new ArrayList<>()));
            }
        });
    }

    OperationResult executeDeleteOne(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");
        BsonDocument filter = arguments.getDocument("filter");

        if (operation.getDocument("arguments").size() > 1) {
            throw new UnsupportedOperationException("Unexpected arguments");
        }

        return resultOf(() ->
                toExpected(collection.deleteOne(filter)));
    }

    OperationResult executeDeleteMany(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");
        BsonDocument filter = arguments.getDocument("filter");

        if (operation.getDocument("arguments").size() > 1) {
            throw new UnsupportedOperationException("Unexpected arguments");
        }

        return resultOf(() ->
                toExpected(collection.deleteMany(filter)));
    }

    private BsonDocument toExpected(final DeleteResult result) {
        return new BsonDocument("deletedCount", new BsonInt32((int) result.getDeletedCount()));
    }

    OperationResult executeUpdateOne(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");
        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "session":
                case "filter":
                case "update":
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }
        ClientSession session = getSession(arguments);
        BsonDocument filter = arguments.getDocument("filter");
        BsonValue update = arguments.get("update");
        UpdateOptions options = getUpdateOptions(arguments);

        return resultOf(() -> {
            UpdateResult updateResult;
            if (session == null) {
                updateResult = update.isArray()
                        ? collection.updateOne(filter, update.asArray().stream().map(BsonValue::asDocument).collect(toList()), options)
                        : collection.updateOne(filter, update.asDocument(), options);
            } else {
                updateResult = update.isArray()
                        ? collection.updateOne(session, filter, update.asArray().stream().map(BsonValue::asDocument).collect(toList()),
                        options)
                        : collection.updateOne(session, filter, update.asDocument(), options);
            }
            return toExpected(updateResult);
        });
    }

    OperationResult executeUpdateMany(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");
        BsonDocument filter = arguments.getDocument("filter");
        BsonDocument update = arguments.getDocument("update");
        UpdateOptions options = getUpdateOptions(arguments);

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "filter":
                case "update":
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }
        return resultOf(() ->
                toExpected(collection.updateMany(filter, update, options)));
    }

    OperationResult executeReplaceOne(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");
        BsonDocument filter = arguments.getDocument("filter");
        BsonDocument replacement = arguments.getDocument("replacement");
        ReplaceOptions options = getReplaceOptions(arguments);

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "filter":
                case "replacement":
                case "upsert":
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }
        return resultOf(() ->
                toExpected(collection.replaceOne(filter, replacement, options)));
    }

    private BsonDocument toExpected(final UpdateResult result) {
        BsonDocument expectedDocument = new BsonDocument()
                .append("matchedCount", new BsonInt32((int) result.getMatchedCount()))
                .append("modifiedCount", new BsonInt32((int) result.getModifiedCount()));
        if (result.getUpsertedId() != null) {
            expectedDocument.append("upsertedId", result.getUpsertedId());
        }
        return expectedDocument;
    }


    OperationResult executeInsertOne(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");
        ClientSession session = getSession(arguments);
        BsonDocument document = arguments.getDocument("document").asDocument();
        InsertOneOptions options = new InsertOneOptions();

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "session":
                case "document":
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() ->
                toExpected(session == null
                        ? collection.insertOne(document, options)
                        : collection.insertOne(session, document, options)));
    }

    private BsonDocument toExpected(final InsertOneResult result) {
        return new BsonDocument("insertedId", result.getInsertedId());
    }

    OperationResult executeInsertMany(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");
        List<BsonDocument> documents = arguments.getArray("documents").stream().map(BsonValue::asDocument).collect(toList());
        InsertManyOptions options = new InsertManyOptions();

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "documents":
                    break;
                case "ordered":
                    options.ordered(cur.getValue().asBoolean().getValue());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() ->
                toExpected(collection.insertMany(documents, options)));
    }

    private BsonDocument toExpected(final InsertManyResult result) {
        return new BsonDocument("insertedIds", new BsonDocument(result.getInsertedIds().entrySet().stream()
                .map(value -> new BsonElement(value.getKey().toString(), value.getValue())).collect(toList())));
    }

    OperationResult executeBulkWrite(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");
        List<WriteModel<BsonDocument>> requests = arguments.getArray("requests").stream()
                .map(value -> toWriteModel(value.asDocument())).collect(toList());
        BulkWriteOptions options = new BulkWriteOptions();
        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "requests":
                    break;
                case "ordered":
                    options.ordered(cur.getValue().asBoolean().getValue());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() ->
                toExpected(collection.bulkWrite(requests, options)));
    }

    private BsonDocument toExpected(final BulkWriteResult result) {
        return new BsonDocument()
                .append("deletedCount", new BsonInt32(result.getDeletedCount()))
                .append("insertedCount", new BsonInt32(result.getInsertedCount()))
                .append("matchedCount", new BsonInt32(result.getMatchedCount()))
                .append("modifiedCount", new BsonInt32(result.getModifiedCount()))
                .append("upsertedCount", new BsonInt32(result.getUpserts().size()))
                .append("insertedIds", new BsonDocument(result.getInserts().stream()
                        .map(value -> new BsonElement(Integer.toString(value.getIndex()), value.getId())).collect(toList())))
                .append("upsertedIds", new BsonDocument(result.getUpserts().stream()
                        .map(value -> new BsonElement(Integer.toString(value.getIndex()), value.getId())).collect(toList())));
    }

    private WriteModel<BsonDocument> toWriteModel(final BsonDocument document) {

        String requestType = document.getFirstKey();
        BsonDocument arguments = document.getDocument(requestType);
        switch (requestType) {
            case "insertOne":
                return new InsertOneModel<>(arguments.getDocument("document"));
            case "updateOne":
                return new UpdateOneModel<>(arguments.getDocument("filter"), arguments.getDocument("update"),
                        getUpdateOptions(arguments));
            case "updateMany":
                return new UpdateManyModel<>(arguments.getDocument("filter"), arguments.getDocument("update"),
                        getUpdateOptions(arguments));
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

    private UpdateOptions getUpdateOptions(final BsonDocument arguments) {
        UpdateOptions options = new UpdateOptions();

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "session":
                case "filter":
                case "update":
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

        return resultOf(() -> {
            session.startTransaction();
            return null;
        });
    }

    OperationResult executeCommitTransaction(final BsonDocument operation) {
        ClientSession session = entities.getSession(operation.getString("object").getValue());

        if (operation.containsKey("arguments")) {
            throw new UnsupportedOperationException("Unexpected arguments");
        }

        return resultOf(() -> {
            session.commitTransaction();
            return null;
        });
    }

    OperationResult executeAbortTransaction(final BsonDocument operation) {
        ClientSession session = entities.getSession(operation.getString("object").getValue());

        if (operation.containsKey("arguments")) {
            throw new UnsupportedOperationException("Unexpected arguments");
        }

        return resultOf(() -> {
            session.abortTransaction();
            return null;
        });
    }

    OperationResult executeWithTransaction(final BsonDocument operation, final OperationAsserter operationAsserter) {
        ClientSession session = entities.getSession(operation.getString("object").getValue());
        BsonArray callback = operation.getDocument("arguments").getArray("callback");
        TransactionOptions.Builder optionsBuilder = TransactionOptions.builder();
        for (Map.Entry<String, BsonValue> entry : operation.getDocument("arguments").entrySet()) {
            switch (entry.getKey()) {
                case "callback":
                    break;
                case "readConcern":
                    optionsBuilder.readConcern(asReadConcern(entry.getValue().asDocument()));
                    break;
                case "writeConcern":
                    optionsBuilder.writeConcern(asWriteConcern(entry.getValue().asDocument()));
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported transaction option: " + entry.getKey());
            }
        }

        return resultOf(() -> {
            session.withTransaction(() -> {
                for (BsonValue cur : callback) {
                    operationAsserter.assertOperation(cur.asDocument());
                }
                //noinspection ConstantConditions
                return null;
            }, optionsBuilder.build());
            return null;
        });
    }

    public OperationResult executeDropCollection(final BsonDocument operation) {
        MongoDatabase database = entities.getDatabase(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");
        String collectionName = arguments.getString("collection").getValue();

        if (operation.getDocument("arguments").size() > 1) {
            throw new UnsupportedOperationException("Unexpected arguments");
        }

        return resultOf(() -> {
            database.getCollection(collectionName).drop();
            return null;
        });
    }

    public OperationResult executeCreateCollection(final BsonDocument operation) {
        MongoDatabase database = entities.getDatabase(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");
        String collectionName = arguments.getString("collection").getValue();
        ClientSession session = getSession(arguments);

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "collection":
                case "session":
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() -> {
            if (session == null) {
                database.createCollection(collectionName);
            } else {
                database.createCollection(session, collectionName);
            }
            return null;
        });
    }

    public OperationResult executeCreateIndex(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");
        BsonDocument keys = arguments.getDocument("keys").asDocument();
        ClientSession session = getSession(arguments);
        IndexOptions options = new IndexOptions();

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "keys":
                case "session":
                    break;
                case "name":
                    options.name(cur.getValue().asString().getValue());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() -> {
            if (session == null) {
                collection.createIndex(keys, options);
            } else {
                collection.createIndex(session, keys, options);
            }
            return null;
        });
    }

    public OperationResult createChangeStreamCursor(final BsonDocument operation) {
        String entityName = operation.getString("object").getValue();
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        List<BsonDocument> pipeline = arguments.getArray("pipeline").stream().map(BsonValue::asDocument).collect(toList());
        ChangeStreamIterable<BsonDocument> iterable;
        if (entities.hasCollection(entityName)) {
            iterable = entities.getCollection(entityName).watch(pipeline);
        } else if (entities.hasDatabase(entityName)) {
            iterable = entities.getDatabase(entityName).watch(pipeline, BsonDocument.class);
        } else if (entities.hasClient(entityName)) {
            iterable = entities.getClient(entityName).watch(pipeline, BsonDocument.class);
        } else {
            throw new UnsupportedOperationException("No entity found for id: " + entityName);
        }

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "batchSize":
                    iterable.batchSize(cur.getValue().asNumber().intValue());
                    break;
                case "pipeline":
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() -> {
            entities.addChangeStreamCursor(operation.getString("saveResultAsEntity").getValue(), iterable.cursor());
            return null;
        });
    }

    public OperationResult executeIterateUntilDocumentOrError(final BsonDocument operation) {
        String id = operation.getString("object").getValue();
        if (entities.hasCursor(id)) {
            MongoCursor<BsonDocument> cursor = entities.getCursor(id);

            if (operation.containsKey("arguments")) {
                throw new UnsupportedOperationException("Unexpected arguments");
            }

            return resultOf(cursor::next);
        } else {
            MongoCursor<ChangeStreamDocument<BsonDocument>> cursor = entities.getChangeStreamCursor(id);

            if (operation.containsKey("arguments")) {
                throw new UnsupportedOperationException("Unexpected arguments");
            }

            return resultOf(() -> {
                BsonDocumentWriter bsonDocumentWriter = new BsonDocumentWriter(new BsonDocument());
                changeStreamDocumentCodec.encode(bsonDocumentWriter, cursor.next(), EncoderContext.builder().build());
                return bsonDocumentWriter.getDocument();
            });
        }
    }

    public OperationResult close(final BsonDocument operation) {
        String id = operation.getString("object").getValue();
        if (entities.hasCursor(id)) {
            MongoCursor<BsonDocument> cursor = entities.getCursor(id);
            cursor.close();
        } else {
            MongoCursor<ChangeStreamDocument<BsonDocument>> cursor = entities.getChangeStreamCursor(id);
            cursor.close();
        }
        return OperationResult.NONE;
    }

    public OperationResult executeRunCommand(final BsonDocument operation) {
        MongoDatabase database = entities.getDatabase(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        BsonDocument command = arguments.getDocument("command");
        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "command":
                case "commandName":
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() ->
                database.runCommand(command, BsonDocument.class));
    }

    public OperationResult executeCountDocuments(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");
        BsonDocument filter = arguments.getDocument("filter");
        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            //noinspection SwitchStatementWithTooFewBranches
            switch (cur.getKey()) {
                case "filter":
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() ->
                new BsonInt64(collection.countDocuments(filter)));
    }

    public OperationResult executeEstimatedDocumentCount(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());

        EstimatedDocumentCountOptions options = new EstimatedDocumentCountOptions();
        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            //noinspection SwitchStatementWithTooFewBranches
            switch (cur.getKey()) {
                case "maxTimeMS":
                    options.maxTime(cur.getValue().asNumber().intValue(), TimeUnit.MILLISECONDS);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() ->
                new BsonInt64(collection.estimatedDocumentCount(options)));
    }
}
