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

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.ChangeStreamPreAndPostImagesOptions;
import com.mongodb.client.model.ClusteredIndexOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateCollectionOptions;
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
import com.mongodb.client.model.TimeSeriesGranularity;
import com.mongodb.client.model.TimeSeriesOptions;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.lang.NonNull;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

final class UnifiedCrudHelper {
    private final Entities entities;
    private final String testDescription;
    private final AtomicInteger uniqueIdGenerator = new AtomicInteger();

    private final Codec<ChangeStreamDocument<BsonDocument>> changeStreamDocumentCodec = ChangeStreamDocument.createCodec(
            BsonDocument.class,
            CodecRegistries.fromProviders(asList(new BsonCodecProvider(), new ValueCodecProvider())));

    UnifiedCrudHelper(final Entities entities, final String testDescription) {
        this.entities = entities;
        this.testDescription = testDescription;
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
        if (writeConcernDocument.isString("w")) {
            return new WriteConcern(writeConcernDocument.getString("w").getValue());
        } else {
            return new WriteConcern(writeConcernDocument.getInt32("w").intValue());
        }
    }

    public static ReadPreference asReadPreference(final BsonDocument readPreferenceDocument) {
        if (readPreferenceDocument.size() == 1) {
            return ReadPreference.valueOf(readPreferenceDocument.getString("mode").getValue());
        } else if (readPreferenceDocument.size() == 2) {
            return ReadPreference.valueOf(readPreferenceDocument.getString("mode").getValue(),
                    Collections.emptyList(), readPreferenceDocument.getNumber("maxStalenessSeconds").intValue(), TimeUnit.SECONDS);
        } else {
            throw new UnsupportedOperationException("Unsupported read preference properties: " + readPreferenceDocument.toJson());
        }
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

        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        ClientSession session = getSession(arguments);
        ListDatabasesIterable<BsonDocument> iterable = session == null
                ? client.listDatabases(BsonDocument.class)
                : client.listDatabases(session, BsonDocument.class);

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            //noinspection SwitchStatementWithTooFewBranches
            switch (cur.getKey()) {
                case "session":
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() ->
                new BsonArray(iterable.into(new ArrayList<>())));
    }

    OperationResult executeListCollections(final BsonDocument operation) {
        MongoDatabase database = entities.getDatabase(operation.getString("object").getValue());

        BsonDocument arguments = operation.getDocument("arguments");
        ClientSession session = getSession(arguments);
        ListCollectionsIterable<BsonDocument> iterable = session == null
                ? database.listCollections(BsonDocument.class)
                : database.listCollections(session, BsonDocument.class);
        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "session":
                    break;
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
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        ClientSession session = getSession(arguments);
        ListIndexesIterable<BsonDocument> iterable = session == null
                ? collection.listIndexes(BsonDocument.class)
                : collection.listIndexes(session, BsonDocument.class);
        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "session":
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

    OperationResult executeFind(final BsonDocument operation) {
        FindIterable<BsonDocument> iterable = createFindIterable(operation);
        return resultOf(() ->
                new BsonArray(iterable.into(new ArrayList<>())));
    }

    OperationResult createFindCursor(final BsonDocument operation) {
        FindIterable<BsonDocument> iterable = createFindIterable(operation);
        return resultOf(() -> {
            entities.addCursor(operation.getString("saveResultAsEntity", new BsonString(createRandomEntityId())).getValue(),
                    iterable.cursor());
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
                case "projection":
                    iterable.projection(cur.getValue().asDocument());
                    break;
                case "sort":
                    iterable.sort(cur.getValue().asDocument());
                    break;
                case "batchSize":
                    iterable.batchSize(cur.getValue().asInt32().intValue());
                    break;
                case "maxTimeMS":
                    iterable.maxTime(cur.getValue().asInt32().longValue(), TimeUnit.MILLISECONDS);
                    break;
                case "skip":
                    iterable.skip(cur.getValue().asInt32().intValue());
                    break;
                case "limit":
                    iterable.limit(cur.getValue().asInt32().intValue());
                    break;
                case "allowDiskUse":
                    iterable.allowDiskUse(cur.getValue().asBoolean().getValue());
                    break;
                case "hint":
                    if (cur.getValue().isString()) {
                        iterable.hintString(cur.getValue().asString().getValue());
                    } else {
                        iterable.hint(cur.getValue().asDocument());
                    }
                    break;
                case "comment":
                    iterable.comment(cur.getValue());
                    break;
                case "let":
                    iterable.let(cur.getValue().asDocument());
                    break;
                case "min":
                    iterable.min(cur.getValue().asDocument());
                    break;
                case "max":
                    iterable.max(cur.getValue().asDocument());
                    break;
                case "returnKey":
                    iterable.returnKey(cur.getValue().asBoolean().getValue());
                    break;
                case "showRecordId":
                    iterable.showRecordId(cur.getValue().asBoolean().getValue());
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
        ClientSession session = getSession(arguments);

        BsonString fieldName = arguments.getString("fieldName");
        DistinctIterable<BsonValue> iterable = session == null
                ? collection.distinct(fieldName.getValue(), BsonValue.class)
                : collection.distinct(session, fieldName.getValue(), BsonValue.class);

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "fieldName":
                case "session":
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
        BsonValue update = arguments.get("update");
        ClientSession session = getSession(arguments);
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "filter":
                case "update":
                case "session":
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
                case "hint":
                    if (cur.getValue().isString()) {
                        options.hintString(cur.getValue().asString().getValue());
                    } else {
                        options.hint(cur.getValue().asDocument());
                    }
                    break;
                case "comment":
                    options.comment(cur.getValue());
                    break;
                case "let":
                    options.let(cur.getValue().asDocument());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() -> {
            if (session == null) {
                return update.isArray()
                        ? collection.findOneAndUpdate(filter, update.asArray().stream().map(BsonValue::asDocument).collect(toList()),
                        options)
                        : collection.findOneAndUpdate(filter, update.asDocument(), options);
            } else {
                return update.isArray()
                        ? collection.findOneAndUpdate(session, filter,
                        update.asArray().stream().map(BsonValue::asDocument).collect(toList()), options)
                        : collection.findOneAndUpdate(session, filter, update.asDocument(), options);

            }
        });
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
                case "hint":
                    if (cur.getValue().isString()) {
                        options.hintString(cur.getValue().asString().getValue());
                    } else {
                        options.hint(cur.getValue().asDocument());
                    }
                    break;
                case "comment":
                    options.comment(cur.getValue());
                    break;
                case "let":
                    options.let(cur.getValue().asDocument());
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
            switch (cur.getKey()) {
                case "filter":
                    break;
                case "hint":
                    if (cur.getValue().isString()) {
                        options.hintString(cur.getValue().asString().getValue());
                    } else {
                        options.hint(cur.getValue().asDocument());
                    }
                    break;
                case "comment":
                    options.comment(cur.getValue());
                    break;
                case "let":
                    options.let(cur.getValue().asDocument());
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
        ClientSession session = getSession(arguments);
        List<BsonDocument> pipeline = arguments.getArray("pipeline").stream().map(BsonValue::asDocument).collect(toList());
        AggregateIterable<BsonDocument> iterable;
        if (entities.hasDatabase(entityName)) {
            iterable = session == null
                    ? entities.getDatabase(entityName).aggregate(requireNonNull(pipeline), BsonDocument.class)
                    : entities.getDatabase(entityName).aggregate(session, requireNonNull(pipeline), BsonDocument.class);
        } else if (entities.hasCollection(entityName)) {
            iterable = session == null
                    ? entities.getCollection(entityName).aggregate(requireNonNull(pipeline))
                    : entities.getCollection(entityName).aggregate(session, requireNonNull(pipeline));
        } else {
            throw new UnsupportedOperationException("Unsupported entity type with name: " + entityName);
        }
        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "pipeline":
                case "session":
                    break;
                case "batchSize":
                    iterable.batchSize(cur.getValue().asNumber().intValue());
                    break;
                case "allowDiskUse":
                    iterable.allowDiskUse(cur.getValue().asBoolean().getValue());
                    break;
                case "let":
                    iterable.let(cur.getValue().asDocument());
                    break;
                case "comment":
                    iterable.comment(cur.getValue());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }
        String lastStageName = pipeline.isEmpty() ? null : pipeline.get(pipeline.size() - 1).getFirstKey();
        boolean useToCollection = Objects.equals(lastStageName, "$out") || Objects.equals(lastStageName, "$merge");

        return resultOf(() -> {
            if (!pipeline.isEmpty() && useToCollection) {
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
        ClientSession session = getSession(arguments);
        DeleteOptions options = getDeleteOptions(arguments);

        return resultOf(() -> {
            if (session == null) {
                return toExpected(collection.deleteOne(filter, options));
            } else {
                return toExpected(collection.deleteOne(session, filter, options));
            }
        });
    }

    OperationResult executeDeleteMany(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");
        BsonDocument filter = arguments.getDocument("filter");
        ClientSession session = getSession(arguments);
        DeleteOptions options = getDeleteOptions(arguments);

        return resultOf(() -> {
            if (session == null) {
                return toExpected(collection.deleteMany(filter, options));
            } else {
                return toExpected(collection.deleteMany(session, filter, options));
            }
        });
    }

    private BsonDocument toExpected(final DeleteResult result) {
        if (result.wasAcknowledged()) {
            return new BsonDocument("deletedCount", new BsonInt32((int) result.getDeletedCount()));
        } else {
            return new BsonDocument();
        }
    }

    OperationResult executeUpdateOne(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");
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
        BsonValue update = arguments.get("update");
        ClientSession session = getSession(arguments);
        UpdateOptions options = getUpdateOptions(arguments);

        return resultOf(() -> {
            if (session == null) {
                return update.isArray()
                        ? toExpected(collection.updateMany(filter, update.asArray().stream().map(BsonValue::asDocument).collect(toList()),
                        options))
                        : toExpected(collection.updateMany(filter, update.asDocument(), options));
            } else {
                return update.isArray()
                        ? toExpected(collection.updateMany(session, filter,
                        update.asArray().stream().map(BsonValue::asDocument).collect(toList()), options))
                        : toExpected(collection.updateMany(session, filter, update.asDocument(), options));
            }
        });
    }

    OperationResult executeReplaceOne(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");
        BsonDocument filter = arguments.getDocument("filter");
        BsonDocument replacement = arguments.getDocument("replacement");
        ReplaceOptions options = getReplaceOptions(arguments);

        return resultOf(() ->
                toExpected(collection.replaceOne(filter, replacement, options)));
    }

    private BsonDocument toExpected(final UpdateResult result) {
        if (result.wasAcknowledged()) {
            BsonDocument expectedDocument = new BsonDocument()
                    .append("matchedCount", new BsonInt32((int) result.getMatchedCount()))
                    .append("modifiedCount", new BsonInt32((int) result.getModifiedCount()))
                    .append("upsertedCount", new BsonInt32(result.getUpsertedId() == null ? 0 : 1));
            if (result.getUpsertedId() != null) {
                expectedDocument.append("upsertedId", result.getUpsertedId());
            }
            return expectedDocument;
        } else {
            return new BsonDocument();
        }
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
                case "comment":
                    options.comment(cur.getValue());
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
        if (result.wasAcknowledged()) {
            return new BsonDocument("insertedId", result.getInsertedId());
        } else {
            return new BsonDocument();
        }
    }

    OperationResult executeInsertMany(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");
        List<BsonDocument> documents = arguments.getArray("documents").stream().map(BsonValue::asDocument).collect(toList());
        ClientSession session = getSession(arguments);
        InsertManyOptions options = new InsertManyOptions();

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "documents":
                case "session":
                    break;
                case "ordered":
                    options.ordered(cur.getValue().asBoolean().getValue());
                    break;
                case "comment":
                    options.comment(cur.getValue());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() -> {
            if (session == null) {
                return toExpected(collection.insertMany(documents, options));
            } else {
                return toExpected(collection.insertMany(session, documents, options));
            }
        });
    }

    private BsonDocument toExpected(final InsertManyResult result) {
        if (result.wasAcknowledged()) {
            return new BsonDocument("insertedIds", new BsonDocument(result.getInsertedIds().entrySet().stream()
                    .map(value -> new BsonElement(value.getKey().toString(), value.getValue())).collect(toList())));
        } else {
            return new BsonDocument();
        }
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
                case "comment":
                    options.comment(cur.getValue());
                    break;
                case "let":
                    options.let(cur.getValue().asDocument());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() ->
                toExpected(collection.bulkWrite(requests, options)));
    }

    private BsonDocument toExpected(final BulkWriteResult result) {
        if (result.wasAcknowledged()) {
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
        } else {
            return new BsonDocument();
        }
    }

    private WriteModel<BsonDocument> toWriteModel(final BsonDocument document) {

        String requestType = document.getFirstKey();
        BsonDocument arguments = document.getDocument(requestType);
        switch (requestType) {
            case "insertOne":
                return new InsertOneModel<>(arguments.getDocument("document"));
            case "updateOne":
                return arguments.isArray("update")
                        ? new UpdateOneModel<>(arguments.getDocument("filter"),
                        arguments.getArray("update").stream().map(BsonValue::asDocument).collect(toList()),
                        getUpdateOptions(arguments))
                        : new UpdateOneModel<>(arguments.getDocument("filter"), arguments.getDocument("update"),
                        getUpdateOptions(arguments));
            case "updateMany":
                return arguments.isArray("update")
                        ? new UpdateManyModel<>(arguments.getDocument("filter"),
                        arguments.getArray("update").stream().map(BsonValue::asDocument).collect(toList()),
                        getUpdateOptions(arguments))
                        : new UpdateManyModel<>(arguments.getDocument("filter"), arguments.getDocument("update"),
                        getUpdateOptions(arguments));
            case "deleteOne":
                return new DeleteOneModel<>(arguments.getDocument("filter"), getDeleteOptions(arguments));
            case "deleteMany":
                return new DeleteManyModel<>(arguments.getDocument("filter"), getDeleteOptions(arguments));
            case "replaceOne":
                return new ReplaceOneModel<>(arguments.getDocument("filter"), arguments.getDocument("replacement"),
                        getReplaceOptions(arguments));
            default:
                throw new UnsupportedOperationException("Unsupported write model type: " + requestType);
        }
    }

    @NotNull
    private DeleteOptions getDeleteOptions(final BsonDocument arguments) {
        DeleteOptions options = new DeleteOptions();

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "session":
                case "filter":
                    break;
                case "hint":
                    if (cur.getValue().isString()) {
                        options.hintString(cur.getValue().asString().getValue());
                    } else {
                        options.hint(cur.getValue().asDocument());
                    }
                    break;
                case "comment":
                    options.comment(cur.getValue());
                    break;
                case "let":
                    options.let(cur.getValue().asDocument());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }
        return options;
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
                case "arrayFilters":
                    options.arrayFilters(cur.getValue().asArray().stream().map(BsonValue::asDocument).collect(toList()));
                    break;
                case "hint":
                    if (cur.getValue().isString()) {
                        options.hintString(cur.getValue().asString().getValue());
                    } else {
                        options.hint(cur.getValue().asDocument());
                    }
                    break;
                case "comment":
                    options.comment(cur.getValue());
                    break;
                case "let":
                    options.let(cur.getValue().asDocument());
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
                case "hint":
                    if (cur.getValue().isString()) {
                        options.hintString(cur.getValue().asString().getValue());
                    } else {
                        options.hint(cur.getValue().asDocument());
                    }
                    break;
                case "comment":
                    options.comment(cur.getValue());
                    break;
                case "let":
                    options.let(cur.getValue().asDocument());
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
                for (int i = 0; i < callback.size(); i++) {
                    BsonValue cur = callback.get(i);
                    operationAsserter.assertOperation(cur.asDocument(), i);
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

        // In Java driver there is a separate method for creating a view, but in the unified test CRUD format
        // views and collections are both created with the createCollection operation. We branch off the "viewOn" key's
        // existence to determine which path to follow here
        if (arguments.containsKey("viewOn")) {
            String viewOn = arguments.getString("viewOn").getValue();
            List<BsonDocument> pipeline =
                    arguments.getArray("pipeline").stream().map(BsonValue::asDocument).collect(toList());

            for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
                switch (cur.getKey()) {
                    case "collection":
                    case "session":
                    case "viewOn":
                    case "pipeline":
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
                }
            }

            return resultOf(() -> {
                if (session == null) {
                    database.createView(collectionName, viewOn, pipeline);
                } else {
                    database.createView(session, collectionName, viewOn, pipeline);
                }
                return null;
            });
        } else {
            CreateCollectionOptions options = new CreateCollectionOptions();

            for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
                switch (cur.getKey()) {
                    case "collection":
                    case "session":
                        break;
                    case "expireAfterSeconds":
                        options.expireAfter(cur.getValue().asNumber().longValue(), TimeUnit.SECONDS);
                        break;
                    case "timeseries":
                        options.timeSeriesOptions(createTimeSeriesOptions(cur.getValue().asDocument()));
                        break;
                    case "changeStreamPreAndPostImages":
                        options.changeStreamPreAndPostImagesOptions(createChangeStreamPreAndPostImagesOptions(cur.getValue().asDocument()));
                        break;
                    case "clusteredIndex":
                        options.clusteredIndexOptions(createClusteredIndexOptions(cur.getValue().asDocument()));
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
                }
            }
            return resultOf(() -> {
                if (session == null) {
                    database.createCollection(collectionName, options);
                } else {
                    database.createCollection(session, collectionName, options);
                }
                return null;
            });
        }
    }

    public OperationResult executeRenameCollection(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");
        String newCollectionName = arguments.getString("to").getValue();
        ClientSession session = getSession(arguments);

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "to":
                case "session":
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() -> {
            MongoNamespace newCollectionNamespace = new MongoNamespace(collection.getNamespace().getDatabaseName(), newCollectionName);
            if (session == null) {
                collection.renameCollection(newCollectionNamespace);
            } else {
                collection.renameCollection(session, newCollectionNamespace);
            }
            return null;
        });
    }

    private TimeSeriesOptions createTimeSeriesOptions(final BsonDocument timeSeriesDocument) {
        TimeSeriesOptions options = new TimeSeriesOptions(timeSeriesDocument.getString("timeField").getValue());

        for (Map.Entry<String, BsonValue> cur : timeSeriesDocument.entrySet()) {
            switch (cur.getKey()) {
                case "timeField":
                    break;
                case "metaField":
                    options.metaField(cur.getValue().asString().getValue());
                    break;
                case "granularity":
                    options.granularity(createTimeSeriesGranularity(cur.getValue().asString().getValue()));
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }
        return options;
    }

    private ClusteredIndexOptions createClusteredIndexOptions(final BsonDocument clusteredIndexDocument) {
        ClusteredIndexOptions options = new ClusteredIndexOptions(clusteredIndexDocument.getDocument("key"),
                clusteredIndexDocument.getBoolean("unique").getValue());

        for (Map.Entry<String, BsonValue> cur : clusteredIndexDocument.entrySet()) {
            switch (cur.getKey()) {
                case "key":
                case "unique":
                    break;
                case "name":
                    options.name(cur.getValue().asString().getValue());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }
        return options;
    }

    private ChangeStreamPreAndPostImagesOptions createChangeStreamPreAndPostImagesOptions(
            final BsonDocument changeStreamPreAndPostImagesDocument) {
        ChangeStreamPreAndPostImagesOptions changeStreamPreAndPostImagesOptions =
                new ChangeStreamPreAndPostImagesOptions(changeStreamPreAndPostImagesDocument.getBoolean("enabled").getValue());

        return changeStreamPreAndPostImagesOptions;
    }

    private TimeSeriesGranularity createTimeSeriesGranularity(final String value) {
        switch (value) {
            case "seconds":
                return TimeSeriesGranularity.SECONDS;
            case "minutes":
                return TimeSeriesGranularity.MINUTES;
            case "hours":
                return TimeSeriesGranularity.HOURS;
            default:
                throw new UnsupportedOperationException("Unsupported time series granularity: " + value);
        }
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
                case "comment":
                    iterable.comment(cur.getValue());
                    break;
                case "fullDocument":
                    iterable.fullDocument(FullDocument.fromString(cur.getValue().asString().getValue()));
                    break;
                case "fullDocumentBeforeChange":
                    iterable.fullDocumentBeforeChange(FullDocumentBeforeChange.fromString(cur.getValue().asString().getValue()));
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() -> {
            entities.addCursor(operation.getString("saveResultAsEntity",
                    new BsonString(createRandomEntityId())).getValue(), createChangeStreamWrappingCursor(iterable));
            return null;
        });
    }

    public OperationResult executeIterateUntilDocumentOrError(final BsonDocument operation) {
        String id = operation.getString("object").getValue();
        MongoCursor<BsonDocument> cursor = entities.getCursor(id);

        if (operation.containsKey("arguments")) {
            throw new UnsupportedOperationException("Unexpected arguments");
        }

        return resultOf(cursor::next);
    }

    public OperationResult close(final BsonDocument operation) {
        String id = operation.getString("object").getValue();
        MongoCursor<BsonDocument> cursor = entities.getCursor(id);
        cursor.close();
        return OperationResult.NONE;
    }

    public OperationResult executeRunCommand(final BsonDocument operation) {
        MongoDatabase database = entities.getDatabase(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        ClientSession session = getSession(arguments);
        BsonDocument command = arguments.getDocument("command");
        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "command":
                case "commandName":
                case "session":
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() -> {
            if (session == null) {
                return database.runCommand(command, BsonDocument.class);
            } else {
                return database.runCommand(session, command, BsonDocument.class);
            }
        });
    }

    public OperationResult executeCountDocuments(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");
        BsonDocument filter = arguments.getDocument("filter");
        ClientSession session = getSession(arguments);
        CountOptions options = new CountOptions();

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "filter":
                case "session":
                    break;
                case "comment":
                    options.comment(cur.getValue());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() -> {
            if (session == null) {
                return new BsonInt64(collection.countDocuments(filter, options));
            } else {
                return new BsonInt64(collection.countDocuments(session, filter, options));
            }
        });
    }

    public OperationResult executeEstimatedDocumentCount(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());

        EstimatedDocumentCountOptions options = new EstimatedDocumentCountOptions();
        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "maxTimeMS":
                    options.maxTime(cur.getValue().asNumber().intValue(), TimeUnit.MILLISECONDS);
                    break;
                case "comment":
                    options.comment(cur.getValue());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() ->
                new BsonInt64(collection.estimatedDocumentCount(options)));
    }

    @NotNull
    private String createRandomEntityId() {
        return "random-entity-id" + uniqueIdGenerator.getAndIncrement();
    }

    /**
     * The tests in this list can not currently pass when using {@link ChangeStreamDocument} because there is information loss when
     * decoding into an instance of that class.  So for these tests, we just decode directly into {@link BsonDocument}.  For all
     * others, we decode into {@link ChangeStreamDocument} and from there to {@link BsonDocument} so that there is some integration test
     * coverage of {@link ChangeStreamDocument}.
     */
    private static final List<String> BSON_DOCUMENT_CHANGE_STREAM_TESTS = asList(
            "Test newField added in response MUST NOT err",
            "Test projection in change stream returns expected fields",
            "fullDocument:whenAvailable with changeStreamPreAndPostImages disabled",
            "fullDocumentBeforeChange:whenAvailable with changeStreamPreAndPostImages disabled");

    @NotNull
    private MongoCursor<BsonDocument> createChangeStreamWrappingCursor(final ChangeStreamIterable<BsonDocument> iterable) {
        if (BSON_DOCUMENT_CHANGE_STREAM_TESTS.contains(testDescription)) {
            return iterable.withDocumentClass(BsonDocument.class).cursor();
        } else {
            MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> wrappedCursor = iterable.cursor();
            return new MongoCursor<BsonDocument>() {
                @Override
                public void close() {
                    wrappedCursor.close();
                }

                @Override
                public boolean hasNext() {
                    return wrappedCursor.hasNext();
                }

                @NonNull
                @Override
                public BsonDocument next() {
                    return encodeChangeStreamDocumentToBsonDocument(wrappedCursor.next());
                }

                @Override
                public int available() {
                    return wrappedCursor.available();
                }

                @Override
                public BsonDocument tryNext() {
                    ChangeStreamDocument<BsonDocument> next = wrappedCursor.tryNext();
                    if (next == null) {
                        return null;
                    } else {
                        return encodeChangeStreamDocumentToBsonDocument(next);
                    }
                }

                @Override
                public ServerCursor getServerCursor() {
                    return wrappedCursor.getServerCursor();
                }

                @NonNull
                @Override
                public ServerAddress getServerAddress() {
                    return wrappedCursor.getServerAddress();
                }

                private BsonDocument encodeChangeStreamDocumentToBsonDocument(final ChangeStreamDocument<BsonDocument> next) {
                    BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
                    changeStreamDocumentCodec.encode(writer, next, EncoderContext.builder().build());
                    return writer.getDocument();
                }
            };
        }
    }
}
