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

import com.mongodb.CursorType;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.assertions.Assertions;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.ListSearchIndexesIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCluster;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.ChangeStreamPreAndPostImagesOptions;
import com.mongodb.client.model.ClusteredIndexOptions;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CollationStrength;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.DropIndexOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.SearchIndexModel;
import com.mongodb.client.model.SearchIndexType;
import com.mongodb.client.model.TimeSeriesGranularity;
import com.mongodb.client.model.TimeSeriesOptions;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.bulk.ClientBulkWriteOptions;
import com.mongodb.client.model.bulk.ClientBulkWriteResult;
import com.mongodb.client.model.bulk.ClientDeleteManyOptions;
import com.mongodb.client.model.bulk.ClientDeleteOneOptions;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;
import com.mongodb.client.model.bulk.ClientReplaceOneOptions;
import com.mongodb.client.model.bulk.ClientUpdateManyOptions;
import com.mongodb.client.model.bulk.ClientUpdateOneOptions;
import com.mongodb.client.model.bulk.ClientUpdateResult;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.internal.client.model.bulk.AbstractClientDeleteOptions;
import com.mongodb.internal.client.model.bulk.AbstractClientUpdateOptions;
import com.mongodb.internal.client.model.bulk.ConcreteClientDeleteManyOptions;
import com.mongodb.internal.client.model.bulk.ConcreteClientDeleteOneOptions;
import com.mongodb.internal.client.model.bulk.ConcreteClientUpdateManyOptions;
import com.mongodb.internal.client.model.bulk.ConcreteClientUpdateOneOptions;
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;
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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.mongodb.client.model.bulk.ClientBulkWriteOptions.clientBulkWriteOptions;
import static com.mongodb.client.model.bulk.ClientReplaceOneOptions.clientReplaceOneOptions;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;

@SuppressWarnings("deprecation")
final class UnifiedCrudHelper extends UnifiedHelper {
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
        WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;

        for (Map.Entry<String, BsonValue> entry: writeConcernDocument.entrySet()) {
            switch (entry.getKey()) {
                case "w":
                    writeConcern = writeConcernDocument.isString("w")
                            ? writeConcern.withW(writeConcernDocument.getString("w").getValue())
                            : writeConcern.withW(writeConcernDocument.getInt32("w").intValue());
                    break;
                case "journal":
                    writeConcern = writeConcern.withJournal(entry.getValue().asBoolean().getValue());
                    break;
                case "wtimeoutMS":
                    writeConcern = writeConcern.withWTimeout(entry.getValue().asNumber().longValue(), TimeUnit.MILLISECONDS);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + entry.getKey());
            }
        }

        return writeConcern;
    }

    public static ReadPreference asReadPreference(final BsonDocument readPreferenceDocument) {
        List<String> supportedKeys = asList("mode", "tagSets", "maxStalenessSeconds");
        List<String> unsupportedKeys = readPreferenceDocument.keySet().stream().filter(key -> !supportedKeys.contains(key)).collect(toList());
        if (!unsupportedKeys.isEmpty()) {
            throw new UnsupportedOperationException("Unsupported read preference keys: " + unsupportedKeys + " in " + readPreferenceDocument);
        }
        String mode = readPreferenceDocument.getString("mode").getValue();
        if (readPreferenceDocument.size() == 1) {
            return ReadPreference.valueOf(mode);
        }
        List<TagSet> tagSets = tagSets(readPreferenceDocument.getArray("tagSets", new BsonArray()));
        BsonValue maxStalenessSecondsBson = readPreferenceDocument.get("maxStalenessSeconds");
        Integer maxStalenessSeconds = maxStalenessSecondsBson == null ? null : maxStalenessSecondsBson.asInt32().intValue();
        if (maxStalenessSecondsBson == null) {
            return ReadPreference.valueOf(mode, tagSets);
        }
        return ReadPreference.valueOf(mode, tagSets, maxStalenessSeconds, TimeUnit.SECONDS);
    }

    private static List<TagSet> tagSets(final BsonArray tagSetsBson) {
        return tagSetsBson.stream()
                .map(tagSetBson -> new TagSet(tagSetBson.asDocument()
                        .entrySet()
                        .stream()
                        .map(entry -> new Tag(entry.getKey(), entry.getValue().asString().getValue()))
                        .collect(toList())))
                .collect(toList());
    }

    private static Collation asCollation(final BsonDocument collationDocument) {
        Collation.Builder builder = Collation.builder();

        for (Map.Entry<String, BsonValue> entry: collationDocument.entrySet()) {
            switch (entry.getKey()) {
                case "locale":
                    builder.locale(entry.getValue().asString().getValue());
                    break;
                case "strength":
                    builder.collationStrength(CollationStrength.fromInt(entry.getValue().asNumber().intValue()));
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + entry.getKey());
            }
        }

        return builder.build();
    }

    private OperationResult resultOf(final Supplier<BsonValue> operationResult) {
        try {
            return OperationResult.of(operationResult.get());
        } catch (Exception e) {
            return OperationResult.of(e);
        }
    }

    @Nullable
    private ClientSession getSession(final BsonDocument arguments) {
        if (arguments.containsKey("session")) {
            return entities.getSession(arguments.getString("session").asString().getValue());
        } else {
            return null;
        }
    }


    OperationResult executeListDatabases(final BsonDocument operation) {
        MongoCluster mongoCluster = getMongoCluster(operation);

        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        ClientSession session = getSession(arguments);
        ListDatabasesIterable<BsonDocument> iterable = session == null
                ? mongoCluster.listDatabases(BsonDocument.class)
                : mongoCluster.listDatabases(session, BsonDocument.class);

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
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

    OperationResult executeListDatabaseNames(final BsonDocument operation) {
        MongoCluster mongoCluster = getMongoCluster(operation);

        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        ClientSession session = getSession(arguments);
        MongoIterable<String> iterable = session == null
                ? mongoCluster.listDatabaseNames()
                : mongoCluster.listDatabaseNames(session);

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
                new BsonArray(iterable.into(new ArrayList<>()).stream().map(BsonString::new).collect(toList())));
    }

    OperationResult executeListCollections(final BsonDocument operation) {
        MongoDatabase database = getMongoDatabase(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        ClientSession session = getSession(arguments);
        ListCollectionsIterable<BsonDocument> iterable = session == null
                ? database.listCollections(BsonDocument.class)
                : database.listCollections(session, BsonDocument.class);
        return resultOf(() -> {
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
                    case "timeoutMode":
                        setTimeoutMode(iterable, cur);
                        break;
                    case "maxTimeMS":
                        iterable.maxTime(cur.getValue().asNumber().longValue(), TimeUnit.MILLISECONDS);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
                }
            }

            return new BsonArray(iterable.into(new ArrayList<>()));
        });
    }

    OperationResult executeListCollectionNames(final BsonDocument operation) {
        MongoDatabase database = getMongoDatabase(operation);

        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        ClientSession session = getSession(arguments);
        MongoIterable<String> iterable = session == null
                ? database.listCollectionNames()
                : database.listCollectionNames(session);
        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "session":
                    break;
                case "filter":
                    BsonDocument filter = cur.getValue().asDocument();
                    if (!filter.isEmpty()) {
                        throw new UnsupportedOperationException("The driver does not support filtering of collection names");
                    }
                    break;
                case "batchSize":
                    iterable.batchSize(cur.getValue().asNumber().intValue());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() ->
                new BsonArray(iterable.into(new ArrayList<>()).stream().map(BsonString::new).collect(toList())));
    }

    OperationResult executeListIndexes(final BsonDocument operation) {
        return resultOf(() -> {
            ListIndexesIterable<BsonDocument> iterable = createListIndexesIterable(operation);
            return new BsonArray(iterable.into(new ArrayList<>()));
        });
    }

    OperationResult executeListIndexNames(final BsonDocument operation) {
        return resultOf(() -> {
            ListIndexesIterable<BsonDocument> iterable = createListIndexesIterable(operation);
            return new BsonArray(iterable.into(new ArrayList<>()).stream().map(document -> document.getString("name")).collect(toList()));
        });
    }

    private ListIndexesIterable<BsonDocument> createListIndexesIterable(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = getMongoCollection(operation);
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
                case "timeoutMode":
                    setTimeoutMode(iterable, cur);
                    break;
                case "maxTimeMS":
                    iterable.maxTime(cur.getValue().asNumber().longValue(), TimeUnit.MILLISECONDS);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }
        return iterable;
    }

    OperationResult executeFind(final BsonDocument operation) {
        return resultOf(() -> {
            FindIterable<BsonDocument> iterable = createFindIterable(operation);
            return new BsonArray(iterable.into(new ArrayList<>()));
        });
    }

    OperationResult executeFindOne(final BsonDocument operation) {
        return resultOf(() ->  createFindIterable(operation).first());
    }

    OperationResult createFindCursor(final BsonDocument operation) {
        return resultOf(() -> {
            FindIterable<BsonDocument> iterable = createFindIterable(operation);
            entities.addCursor(operation.getString("saveResultAsEntity", new BsonString(createRandomEntityId())).getValue(),
                    iterable.cursor());
            return null;
        });
    }

    @NonNull
    private FindIterable<BsonDocument> createFindIterable(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = getMongoCollection(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
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
                case "maxAwaitTimeMS":
                    iterable.maxAwaitTime(cur.getValue().asInt32().longValue(), TimeUnit.MILLISECONDS);
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
                case "collation":
                    iterable.collation(asCollation(cur.getValue().asDocument()));
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
                case "cursorType":
                    setCursorType(iterable, cur);
                    break;
                case "timeoutMode":
                    setTimeoutMode(iterable, cur);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }
        return iterable;
    }

    @SuppressWarnings("deprecation") //maxTimeMS
    OperationResult executeDistinct(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = getMongoCollection(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
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
                case "comment":
                    iterable.comment(cur.getValue());
                    break;
                case "hint":
                    if (cur.getValue().isString()) {
                        iterable.hintString(cur.getValue().asString().getValue());
                    } else {
                        iterable.hint(cur.getValue().asDocument());
                    }
                    break;
                case "filter":
                    iterable.filter(cur.getValue().asDocument());
                    break;
                case "maxTimeMS":
                    iterable.maxTime(cur.getValue().asInt32().intValue(), TimeUnit.MILLISECONDS);
                    break;
                case "collation":
                    iterable.collation(asCollation(cur.getValue().asDocument()));
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() ->
                new BsonArray(iterable.into(new ArrayList<>())));
    }

    @SuppressWarnings("deprecation")
    OperationResult executeMapReduce(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = getMongoCollection(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        ClientSession session = getSession(arguments);

        String mapFunction = arguments.get("map").asJavaScript().getCode();
        String reduceFunction = arguments.get("reduce").asJavaScript().getCode();
        com.mongodb.client.MapReduceIterable<BsonDocument> iterable = session == null
                ? collection.mapReduce(mapFunction, reduceFunction)
                : collection.mapReduce(session, mapFunction, reduceFunction);

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "map":
                case "reduce":
                case "session":
                    break;
                case "out":
                    if (!cur.getValue().asDocument().equals(new BsonDocument("inline", new BsonInt32(1)))) {
                        throw new UnsupportedOperationException("Unsupported value for out argument: " + cur.getValue());
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() ->
                new BsonArray(iterable.into(new ArrayList<>())));
    }

    @SuppressWarnings("deprecation") //maxTimeMS
    OperationResult executeFindOneAndUpdate(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = getMongoCollection(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());

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
                case "upsert":
                    options.upsert(cur.getValue().asBoolean().getValue());
                    break;
                case "sort":
                    options.sort(cur.getValue().asDocument());
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
                case "projection":
                    options.projection(cur.getValue().asDocument());
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
                case "maxTimeMS":
                    options.maxTime(cur.getValue().asInt32().intValue(), TimeUnit.MILLISECONDS);
                    break;
                case "collation":
                    options.collation(asCollation(cur.getValue().asDocument()));
                    break;
                case "arrayFilters":
                    options.arrayFilters(cur.getValue().asArray().stream().map(BsonValue::asDocument).collect(toList()));
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

    @SuppressWarnings("deprecation")
    OperationResult executeFindOneAndReplace(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = getMongoCollection(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        ClientSession session = getSession(arguments);
        BsonDocument filter = arguments.getDocument("filter").asDocument();
        BsonDocument replacement = arguments.getDocument("replacement").asDocument();
        FindOneAndReplaceOptions options = new FindOneAndReplaceOptions();

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "filter":
                case "replacement":
                case "session":
                    break;
                case "upsert":
                    options.upsert(cur.getValue().asBoolean().getValue());
                    break;
                case "sort":
                    options.sort(cur.getValue().asDocument());
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
                case "projection":
                    options.projection(cur.getValue().asDocument());
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
                case "maxTimeMS":
                    options.maxTime(cur.getValue().asInt32().intValue(), TimeUnit.MILLISECONDS);
                    break;
                case "collation":
                    options.collation(asCollation(cur.getValue().asDocument()));
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() -> {
            if (session == null) {
                return collection.findOneAndReplace(filter, replacement, options);
            } else {
                return collection.findOneAndReplace(session, filter, replacement, options);
            }
        });
    }

    @SuppressWarnings("deprecation") //maxTimeMS
    OperationResult executeFindOneAndDelete(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = getMongoCollection(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        ClientSession session = getSession(arguments);
        BsonDocument filter = arguments.getDocument("filter").asDocument();
        FindOneAndDeleteOptions options = new FindOneAndDeleteOptions();

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "filter":
                case "session":
                    break;
                case "projection":
                    options.projection(cur.getValue().asDocument());
                    break;
                case "sort":
                    options.sort(cur.getValue().asDocument());
                    break;
                case "hint":
                    if (cur.getValue().isString()) {
                        options.hintString(cur.getValue().asString().getValue());
                    } else {
                        options.hint(cur.getValue().asDocument());
                    }
                    break;
                case "collation":
                    options.collation(asCollation(cur.getValue().asDocument()));
                    break;
                case "comment":
                    options.comment(cur.getValue());
                    break;
                case "let":
                    options.let(cur.getValue().asDocument());
                    break;
                case "maxTimeMS":
                    options.maxTime(cur.getValue().asInt32().intValue(), TimeUnit.MILLISECONDS);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() -> {
            if (session == null) {
                return collection.findOneAndDelete(filter, options);
            } else {
                return collection.findOneAndDelete(session, filter, options);
            }
        });
    }

    OperationResult executeAggregate(final BsonDocument operation) {
        String entityName = operation.getString("object").getValue();
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        ClientSession session = getSession(arguments);
        List<BsonDocument> pipeline = arguments.getArray("pipeline").stream().map(BsonValue::asDocument).collect(toList());
        AggregateIterable<BsonDocument> iterable;
        if (entities.hasDatabase(entityName)) {
            Long timeoutMS = getAndRemoveTimeoutMS(operation.getDocument("arguments"));
            MongoDatabase database = entities.getDatabaseWithTimeoutMS(entityName, timeoutMS);
            iterable = session == null
                    ? database.aggregate(requireNonNull(pipeline), BsonDocument.class)
                    : database.aggregate(session, requireNonNull(pipeline), BsonDocument.class);
        } else if (entities.hasCollection(entityName)) {
            Long timeoutMS = getAndRemoveTimeoutMS(operation.getDocument("arguments"));
            MongoCollection<BsonDocument> collection = entities.getCollectionWithTimeoutMS(entityName, timeoutMS);
            iterable = session == null
                    ? collection.aggregate(requireNonNull(pipeline))
                    : collection.aggregate(session, requireNonNull(pipeline));
        } else {
            throw new UnsupportedOperationException("Unsupported entity type with name: " + entityName);
        }
        return resultOf(() -> {
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
                    case "collation":
                        iterable.collation(asCollation(cur.getValue().asDocument()));
                        break;
                    case "comment":
                        iterable.comment(cur.getValue());
                        break;
                    case "timeoutMode":
                        setTimeoutMode(iterable, cur);
                        break;
                    case "maxTimeMS":
                        iterable.maxTime(cur.getValue().asNumber().longValue(), TimeUnit.MILLISECONDS);
                        break;
                    case "maxAwaitTimeMS":
                        iterable.maxAwaitTime(cur.getValue().asNumber().longValue(), TimeUnit.MILLISECONDS);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
                }
            }
            String lastStageName = pipeline.isEmpty() ? null : pipeline.get(pipeline.size() - 1).getFirstKey();
            boolean useToCollection = Objects.equals(lastStageName, "$out") || Objects.equals(lastStageName, "$merge");
            if (!pipeline.isEmpty() && useToCollection) {
                iterable.toCollection();
                return null;
            } else {
                return new BsonArray(iterable.into(new ArrayList<>()));
            }
        });
    }

    OperationResult executeDeleteOne(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = getMongoCollection(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
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
        MongoCollection<BsonDocument> collection = getMongoCollection(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
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
        MongoCollection<BsonDocument> collection = getMongoCollection(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
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
        MongoCollection<BsonDocument> collection = getMongoCollection(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
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
        MongoCollection<BsonDocument> collection = getMongoCollection(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        ClientSession session = getSession(arguments);
        BsonDocument filter = arguments.getDocument("filter");
        BsonDocument replacement = arguments.getDocument("replacement");
        ReplaceOptions options = getReplaceOptions(arguments);

        return resultOf(() -> {
            if (session == null) {
                return toExpected(collection.replaceOne(filter, replacement, options));
            } else {
                return toExpected(collection.replaceOne(session, filter, replacement, options));
            }
        });
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
        MongoCollection<BsonDocument> collection = getMongoCollection(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
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
        MongoCollection<BsonDocument> collection = getMongoCollection(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
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
        MongoCollection<BsonDocument> collection = getMongoCollection(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        ClientSession session = getSession(arguments);
        List<WriteModel<BsonDocument>> requests = arguments.getArray("requests").stream()
                .map(value -> toWriteModel(value.asDocument())).collect(toList());
        BulkWriteOptions options = new BulkWriteOptions();
        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "requests":
                case "session":
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

        return resultOf(() -> {
            if (session == null) {
                return toExpected(collection.bulkWrite(requests, options));
            } else {
                return toExpected(collection.bulkWrite(session, requests, options));
            }
        });
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

    @NonNull
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
                case "collation":
                    options.collation(asCollation(cur.getValue().asDocument()));
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
                case "collation":
                    options.collation(asCollation(cur.getValue().asDocument()));
                    break;
                case "sort":
                    options.sort(cur.getValue().asDocument());
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
                case "session":
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
                case "collation":
                    options.collation(asCollation(cur.getValue().asDocument()));
                    break;
                case "sort":
                    options.sort(cur.getValue().asDocument());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }
        return options;
    }

    OperationResult executeStartTransaction(final BsonDocument operation) {
        ClientSession session = entities.getSession(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        TransactionOptions.Builder optionsBuilder = TransactionOptions.builder();

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "writeConcern":
                    optionsBuilder.writeConcern(asWriteConcern(cur.getValue().asDocument()));
                    break;
                case "readPreference":
                    optionsBuilder.readPreference(asReadPreference(cur.getValue().asDocument()));
                    break;
                case "readConcern":
                    optionsBuilder.readConcern(asReadConcern(cur.getValue().asDocument()));
                    break;
                case "timeoutMS":
                    optionsBuilder.timeout(cur.getValue().asInt32().longValue(), TimeUnit.MILLISECONDS);
                    break;
                case "maxCommitTimeMS":
                    optionsBuilder.maxCommitTime(cur.getValue().asNumber().longValue(), TimeUnit.MILLISECONDS);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() -> {
            session.startTransaction(optionsBuilder.build());
            return null;
        });
    }

    OperationResult executeCommitTransaction(final BsonDocument operation) {
        ClientSession session = entities.getSession(operation.getString("object").getValue());

        if (operation.containsKey("arguments")) {
            throw new UnsupportedOperationException("Unexpected arguments " + operation.get("arguments"));
        }

        return resultOf(() -> {
            session.commitTransaction();
            return null;
        });
    }

    OperationResult executeAbortTransaction(final BsonDocument operation) {
        ClientSession session = entities.getSession(operation.getString("object").getValue());

        if (operation.containsKey("arguments")) {
            throw new UnsupportedOperationException("Unexpected arguments: " + operation.get("arguments"));
        }

        return resultOf(() -> {
            session.abortTransaction();
            return null;
        });
    }

    OperationResult executeWithTransaction(final BsonDocument operation, final OperationAsserter operationAsserter) {
        ClientSession session = entities.getSession(operation.getString("object").getValue());
        BsonArray callback = operation.getDocument("arguments", new BsonDocument()).getArray("callback");
        TransactionOptions.Builder optionsBuilder = TransactionOptions.builder();
        for (Map.Entry<String, BsonValue> entry : operation.getDocument("arguments", new BsonDocument()).entrySet()) {
            switch (entry.getKey()) {
                case "callback":
                    break;
                case "readConcern":
                    optionsBuilder.readConcern(asReadConcern(entry.getValue().asDocument()));
                    break;
                case "writeConcern":
                    optionsBuilder.writeConcern(asWriteConcern(entry.getValue().asDocument()));
                    break;
                case "timeoutMS":
                    optionsBuilder.timeout(entry.getValue().asNumber().longValue(), TimeUnit.MILLISECONDS);
                    break;
                case "maxCommitTimeMS":
                    optionsBuilder.maxCommitTime(entry.getValue().asNumber().longValue(), TimeUnit.MILLISECONDS);
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
        MongoDatabase database = getMongoDatabase(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        String collectionName = arguments.getString("collection").getValue();

        if (operation.getDocument("arguments").size() > 1) {
            throw new UnsupportedOperationException("Unexpected arguments " + operation.get("arguments"));
        }

        return resultOf(() -> {
            database.getCollection(collectionName).drop();
            return null;
        });
    }

    public OperationResult executeCreateCollection(final BsonDocument operation) {
        MongoDatabase database = getMongoDatabase(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        String collectionName = arguments.getString("collection").getValue();
        ClientSession session = getSession(arguments);

        // In Java driver there is a separate method for creating a view, but in the unified test CRUD format
        // views and collections are both created with the createCollection operation. We use the createView
        // method if the requisite arguments are present.
        if (arguments.containsKey("viewOn") && arguments.containsKey("pipeline")) {
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
                    case "viewOn":
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

    public OperationResult executeModifyCollection(final BsonDocument operation) {
        MongoDatabase database = getMongoDatabase(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        String collectionName = arguments.getString("collection").getValue();
        ClientSession session = getSession(arguments);

        BsonDocument collModCommandDocument = new BsonDocument("collMod", new BsonString(collectionName));

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "collection":
                case "session":
                    break;
                case "validator":
                    collModCommandDocument.append("validator", cur.getValue());
                    break;
                case "index":
                    collModCommandDocument.append("index", cur.getValue());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }
        return resultOf(() -> {
            if (session == null) {
                database.runCommand(collModCommandDocument);
            } else {
                database.runCommand(session, collModCommandDocument);
            }
            return null;
        });
    }

    public OperationResult executeRenameCollection(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = getMongoCollection(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        String newCollectionName = arguments.getString("to").getValue();
        ClientSession session = getSession(arguments);
        RenameCollectionOptions options = new RenameCollectionOptions();

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "to":
                case "session":
                    break;
                case "dropTarget":
                    options.dropTarget(cur.getValue().asBoolean().getValue());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() -> {
            MongoNamespace newCollectionNamespace = new MongoNamespace(collection.getNamespace().getDatabaseName(), newCollectionName);
            if (session == null) {
                collection.renameCollection(newCollectionNamespace, options);
            } else {
                collection.renameCollection(session, newCollectionNamespace, options);
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
                case "bucketMaxSpanSeconds":
                    options.bucketMaxSpan(cur.getValue().asInt32().longValue(), TimeUnit.SECONDS);
                    break;
                case "bucketRoundingSeconds":
                    options.bucketRounding(cur.getValue().asInt32().longValue(), TimeUnit.SECONDS);
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


    OperationResult executeCreateSearchIndex(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = getMongoCollection(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        BsonDocument model = arguments.getDocument("model");

        return resultOf(() -> {
            collection.createSearchIndexes(Collections.singletonList(toIndexSearchModel(model)));
            return null;
        });
    }

    private static SearchIndexType  getSearchIndexType(final BsonString type) {
        switch (type.getValue()) {
            case "search":
                return SearchIndexType.search();
            case "vectorSearch":
                return SearchIndexType.vectorSearch();
            default:
                throw new UnsupportedOperationException("Unsupported search index type: " + type.getValue());
        }
    }

    OperationResult executeCreateSearchIndexes(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = getMongoCollection(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        BsonArray models = arguments.getArray("models");

        List<SearchIndexModel> searchIndexModels = models.stream()
                .map(UnifiedCrudHelper::toIndexSearchModel).collect(toList());

        return resultOf(() -> {
            collection.createSearchIndexes(searchIndexModels);
            return null;
        });
    }


    OperationResult executeUpdateSearchIndex(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = getMongoCollection(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        BsonDocument definition = arguments.getDocument("definition");
        String name = arguments.getString("name").getValue();

        return resultOf(() -> {
            collection.updateSearchIndex(name, definition);
            return null;
        });
    }

    OperationResult executeDropSearchIndex(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = getMongoCollection(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        String name = arguments.getString("name").getValue();

        return resultOf(() -> {
            collection.dropSearchIndex(name);
            return null;
        });
    }

    private static SearchIndexModel toIndexSearchModel(final BsonValue bsonValue) {
        BsonDocument model = bsonValue.asDocument();
        BsonDocument definition = model.getDocument("definition");
        SearchIndexType type = model.containsKey("type") ? getSearchIndexType(model.getString("type")) : null;
        String name = ofNullable(model.getString("name", null))
                .map(BsonString::getValue).
                orElse(null);
        return new SearchIndexModel(name, definition, type);
    }


    OperationResult executeListSearchIndexes(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = getMongoCollection(operation);
        Optional<BsonDocument> arguments = ofNullable(operation.getOrDefault("arguments", null)).map(BsonValue::asDocument);

        if (arguments.isPresent()) {
            ListSearchIndexesIterable<BsonDocument> iterable = createListSearchIndexesIterable(collection, arguments.get());
            return resultOf(() -> {
                iterable.into(new ArrayList<>());
                return null;
            });
        }

        return resultOf(() -> {
            collection.listSearchIndexes().into(new ArrayList<>());
            return null;
        });
    }

    private ListSearchIndexesIterable<BsonDocument> createListSearchIndexesIterable(final MongoCollection<BsonDocument> collection,
            final BsonDocument arguments) {
        Optional<String> name = ofNullable(arguments.getOrDefault("name", null))
                .map(BsonValue::asString).map(BsonString::getValue);

        ListSearchIndexesIterable<BsonDocument> iterable = collection.listSearchIndexes(BsonDocument.class);

        if (arguments.containsKey("aggregationOptions")) {
            for (Map.Entry<String, BsonValue> option : arguments.getDocument("aggregationOptions").entrySet()) {
                switch (option.getKey()) {
                    case "batchSize":
                        iterable.batchSize(option.getValue().asNumber().intValue());
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported argument: " + option.getKey());
                }
            }
        }
        return iterable.name(name.get());
    }

    public OperationResult executeCreateIndex(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = getMongoCollection(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
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
                case "unique":
                    options.unique(cur.getValue().asBoolean().getValue());
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

    public OperationResult executeDropIndex(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = getMongoCollection(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        ClientSession session = getSession(arguments);
        String indexName = arguments.get("name").asString().getValue();

        if (!arguments.containsKey("name")) {
            throw new UnsupportedOperationException("Drop index without name is not supported");
        }

        DropIndexOptions options = getDropIndexOptions(arguments);
        return resultOf(() -> {
            if (session == null) {
                collection.dropIndex(indexName, options);
            } else {
                collection.dropIndex(session, indexName, options);
            }
            return null;
        });
    }

    public OperationResult executeDropIndexes(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = getMongoCollection(operation);

        if (operation.containsKey("arguments")) {
            BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
            ClientSession session = getSession(arguments);
            DropIndexOptions options = getDropIndexOptions(arguments);
            return resultOf(() -> {
                if (session == null) {
                    collection.dropIndexes(options);
                } else {
                    collection.dropIndexes(session, options);
                }
                return null;
            });
        }
        return resultOf(() -> {
            collection.dropIndexes();
            return null;
        });
    }

    private static DropIndexOptions getDropIndexOptions(final BsonDocument arguments) {
        DropIndexOptions options = new DropIndexOptions();
        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "session":
                case "name":
                    break;
                case "maxTimeMS":
                    options.maxTime(cur.getValue().asNumber().intValue(), TimeUnit.MILLISECONDS);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }
        return options;
    }

    public OperationResult createChangeStreamCursor(final BsonDocument operation) {
        String entityName = operation.getString("object").getValue();
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        List<BsonDocument> pipeline = arguments.getArray("pipeline").stream().map(BsonValue::asDocument).collect(toList());
        ChangeStreamIterable<BsonDocument> iterable;

        Long timeoutMS = arguments.containsKey("timeoutMS") ? arguments.remove("timeoutMS").asNumber().longValue() : null;
        if (entities.hasCollection(entityName)) {
            iterable = entities.getCollectionWithTimeoutMS(entityName, timeoutMS).watch(pipeline);
        } else if (entities.hasDatabase(entityName)) {
            iterable = entities.getDatabaseWithTimeoutMS(entityName, timeoutMS).watch(pipeline, BsonDocument.class);
        } else if (entities.hasClient(entityName)) {
            iterable = entities.getMongoClusterWithTimeoutMS(entityName, timeoutMS).watch(pipeline, BsonDocument.class);
        } else {
            throw new UnsupportedOperationException("No entity found for id: " + entityName);
        }

        return resultOf(() -> {
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
                    case "maxAwaitTimeMS":
                        iterable.maxAwaitTime(cur.getValue().asNumber().longValue(), TimeUnit.MILLISECONDS);
                        break;
                    case "showExpandedEvents":
                        iterable.showExpandedEvents(cur.getValue().asBoolean().getValue());
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
                }
            }
            MongoCursor<BsonDocument> changeStreamWrappingCursor = createChangeStreamWrappingCursor(iterable);
            entities.addCursor(operation.getString("saveResultAsEntity",
                    new BsonString(createRandomEntityId())).getValue(), changeStreamWrappingCursor);
            return null;
        });
    }

    public OperationResult clientBulkWrite(final BsonDocument operation) {
        Set<String> unexpectedOperationKeys = singleton("saveResultAsEntity");
        if (operation.keySet().stream().anyMatch(unexpectedOperationKeys::contains)) {
            throw new UnsupportedOperationException("Unexpected field in operation. One of " + unexpectedOperationKeys);
        }
        String clientId = operation.getString("object").getValue();
        MongoCluster cluster = entities.getClient(clientId);
        BsonDocument arguments = operation.getDocument("arguments");
        ClientSession session = getSession(arguments);
        List<ClientNamespacedWriteModel> models = arguments.getArray("models").stream()
                .map(BsonValue::asDocument)
                .map(UnifiedCrudHelper::toClientNamespacedWriteModel)
                .collect(toList());
        ClientBulkWriteOptions options = clientBulkWriteOptions();
        for (Map.Entry<String, BsonValue> entry : arguments.entrySet()) {
            String key = entry.getKey();
            BsonValue argument = entry.getValue();
            switch (key) {
                case "models":
                case "session":
                    break;
                case "writeConcern":
                    cluster = cluster.withWriteConcern(asWriteConcern(argument.asDocument()));
                    break;
                case "ordered":
                    options.ordered(argument.asBoolean().getValue());
                    break;
                case "bypassDocumentValidation":
                    options.bypassDocumentValidation(argument.asBoolean().getValue());
                    break;
                case "let":
                    options.let(argument.asDocument());
                    break;
                case "comment":
                    options.comment(argument);
                    break;
                case "verboseResults":
                    options.verboseResults(argument.asBoolean().getValue());
                    break;
                default:
                    throw new UnsupportedOperationException(format("Unsupported argument: key=%s, argument=%s", key, argument));
            }
        }
        MongoCluster clusterWithWriteConcern = cluster;
        return resultOf(() -> {
            if (session == null) {
                return toMatchableValue(clusterWithWriteConcern.bulkWrite(models, options));
            } else {
                return toMatchableValue(clusterWithWriteConcern.bulkWrite(session, models, options));
            }
        });
    }

    private static ClientNamespacedWriteModel toClientNamespacedWriteModel(final BsonDocument model) {
        String modelType = model.getFirstKey();
        BsonDocument arguments = model.getDocument(modelType);
        MongoNamespace namespace = new MongoNamespace(arguments.getString("namespace").getValue());
        switch (modelType) {
            case "insertOne":
                Set<String> expectedArguments = new HashSet<>(asList("namespace", "document"));
                if (!expectedArguments.containsAll(arguments.keySet())) {
                    // for other `modelType`s a conceptually similar check is done when creating their options objects
                    throw new UnsupportedOperationException("Unsupported argument, one of: " + arguments.keySet());
                }
                return ClientNamespacedWriteModel.insertOne(
                        namespace,
                        arguments.getDocument("document"));
            case "replaceOne":
                return ClientNamespacedWriteModel.replaceOne(
                        namespace,
                        arguments.getDocument("filter"),
                        arguments.getDocument("replacement"),
                        getClientReplaceOneOptions(arguments));
            case "updateOne":
                return arguments.isDocument("update")
                        ? ClientNamespacedWriteModel.updateOne(
                                namespace,
                                arguments.getDocument("filter"),
                                arguments.getDocument("update"),
                                getClientUpdateOneOptions(arguments))
                        : ClientNamespacedWriteModel.updateOne(
                                namespace,
                                arguments.getDocument("filter"),
                                arguments.getArray("update").stream().map(BsonValue::asDocument).collect(toList()),
                                getClientUpdateOneOptions(arguments));
            case "updateMany":
                return arguments.isDocument("update")
                        ? ClientNamespacedWriteModel.updateMany(
                                namespace,
                                arguments.getDocument("filter"),
                                arguments.getDocument("update"),
                                getClientUpdateManyOptions(arguments))
                        : ClientNamespacedWriteModel.updateMany(
                                namespace,
                                arguments.getDocument("filter"),
                                arguments.getArray("update").stream().map(BsonValue::asDocument).collect(toList()),
                                getClientUpdateManyOptions(arguments));
            case "deleteOne":
                return ClientNamespacedWriteModel.deleteOne(
                        namespace,
                        arguments.getDocument("filter"),
                        getClientDeleteOneOptions(arguments));
            case "deleteMany":
                return ClientNamespacedWriteModel.deleteMany(
                        namespace,
                        arguments.getDocument("filter"),
                        getClientDeleteManyOptions(arguments));
            default:
                throw new UnsupportedOperationException("Unsupported client write model type: " + modelType);
        }
    }

    private static ClientReplaceOneOptions getClientReplaceOneOptions(final BsonDocument arguments) {
        ClientReplaceOneOptions options = clientReplaceOneOptions();
        arguments.forEach((key, argument) -> {
            switch (key) {
                case "namespace":
                case "filter":
                case "replacement":
                    break;
                case "collation":
                    options.collation(asCollation(argument.asDocument()));
                    break;
                case "hint":
                    if (argument.isDocument()) {
                        options.hint(argument.asDocument());
                    } else {
                        options.hintString(argument.asString().getValue());
                    }
                    break;
                case "upsert":
                    options.upsert(argument.asBoolean().getValue());
                    break;
                case "sort":
                    options.sort(argument.asDocument());
                    break;
                default:
                    throw new UnsupportedOperationException(format("Unsupported argument: key=%s, argument=%s", key, argument));
            }
        });
        return options;
    }

    private static ClientUpdateOneOptions getClientUpdateOneOptions(final BsonDocument arguments) {
        ConcreteClientUpdateOneOptions options = new ConcreteClientUpdateOneOptions();

        if (arguments.containsKey("sort")) {
            BsonDocument sort = arguments
                    .remove("sort")
                    .asDocument();
            options.sort(sort);
        }

        return fillAbstractClientUpdateOptions(options, arguments);
    }

    private static ClientUpdateManyOptions getClientUpdateManyOptions(final BsonDocument arguments) {
        return fillAbstractClientUpdateOptions(new ConcreteClientUpdateManyOptions(), arguments);
    }

    private static <T extends AbstractClientUpdateOptions> T fillAbstractClientUpdateOptions(
            final T options,
            final BsonDocument arguments) {
        arguments.forEach((key, argument) -> {
            switch (key) {
                case "namespace":
                case "filter":
                case "update":
                    break;
                case "arrayFilters":
                    options.arrayFilters(argument.asArray().stream().map(BsonValue::asDocument).collect(toList()));
                    break;
                case "collation":
                    options.collation(asCollation(argument.asDocument()));
                    break;
                case "hint":
                    if (argument.isDocument()) {
                        options.hint(argument.asDocument());
                    } else {
                        options.hintString(argument.asString().getValue());
                    }
                    break;
                case "upsert":
                    options.upsert(argument.asBoolean().getValue());
                    break;
                default:
                    throw new UnsupportedOperationException(format("Unsupported argument: key=%s, argument=%s", key, argument));
            }
        });
        return options;
    }

    private static ClientDeleteOneOptions getClientDeleteOneOptions(final BsonDocument arguments) {
        return fillAbstractClientDeleteOptions(new ConcreteClientDeleteOneOptions(), arguments);
    }

    private static ClientDeleteManyOptions getClientDeleteManyOptions(final BsonDocument arguments) {
        return fillAbstractClientDeleteOptions(new ConcreteClientDeleteManyOptions(), arguments);
    }

    private static <T extends AbstractClientDeleteOptions> T fillAbstractClientDeleteOptions(
            final T options,
            final BsonDocument arguments) {
        arguments.forEach((key, argument) -> {
            switch (key) {
                case "namespace":
                case "filter":
                    break;
                case "collation":
                    options.collation(asCollation(argument.asDocument()));
                    break;
                case "hint":
                    if (argument.isDocument()) {
                        options.hint(argument.asDocument());
                    } else {
                        options.hintString(argument.asString().getValue());
                    }
                    break;
                default:
                    throw new UnsupportedOperationException(format("Unsupported argument: key=%s, argument=%s", key, argument));
            }
        });
        return options;
    }

    static BsonDocument toMatchableValue(final ClientBulkWriteResult result) {
        BsonDocument expected = new BsonDocument();
        if (result.isAcknowledged()) {
            expected.append("insertedCount", new BsonInt64(result.getInsertedCount()))
                    .append("upsertedCount", new BsonInt64(result.getUpsertedCount()))
                    .append("matchedCount", new BsonInt64(result.getMatchedCount()))
                    .append("modifiedCount", new BsonInt64(result.getModifiedCount()))
                    .append("deletedCount", new BsonInt64(result.getDeletedCount()));
            result.getVerboseResults().ifPresent(verbose ->
                expected.append("insertResults", new BsonDocument(verbose.getInsertResults().entrySet().stream()
                                .map(entry -> new BsonElement(
                                        entry.getKey().toString(),
                                        new BsonDocument("insertedId", entry.getValue().getInsertedId().orElseThrow(Assertions::fail))))
                                .collect(toList())))
                        .append("updateResults", new BsonDocument(verbose.getUpdateResults().entrySet().stream()
                                .map(entry -> {
                                    ClientUpdateResult updateResult = entry.getValue();
                                    BsonDocument updateResultDocument = new BsonDocument(
                                            "matchedCount", new BsonInt64(updateResult.getMatchedCount()))
                                            .append("modifiedCount", new BsonInt64(updateResult.getModifiedCount()));
                                    updateResult.getUpsertedId().ifPresent(upsertedId -> updateResultDocument.append("upsertedId", upsertedId));
                                    return new BsonElement(entry.getKey().toString(), updateResultDocument);
                                })
                                .collect(toList())))
                        .append("deleteResults", new BsonDocument(verbose.getDeleteResults().entrySet().stream()
                                .map(entry -> new BsonElement(
                                        entry.getKey().toString(),
                                        new BsonDocument("deletedCount", new BsonInt64(entry.getValue().getDeletedCount()))))
                                .collect(toList()))));
        }
        return expected;
    }

    public OperationResult executeIterateUntilDocumentOrError(final BsonDocument operation) {
        String id = operation.getString("object").getValue();
        MongoCursor<BsonDocument> cursor = entities.getCursor(id);

        if (operation.containsKey("arguments")) {
            throw new UnsupportedOperationException("Unexpected arguments " + operation.get("arguments"));
        }

        return resultOf(cursor::next);
    }


    public OperationResult executeIterateOnce(final BsonDocument operation) {
        String id = operation.getString("object").getValue();
        MongoCursor<BsonDocument> cursor = entities.getCursor(id);

        if (operation.containsKey("arguments")) {
            throw new UnsupportedOperationException("Unexpected arguments " + operation.get("arguments"));
        }

        return resultOf(cursor::tryNext);
    }

    public OperationResult close(final BsonDocument operation) {
        String id = operation.getString("object").getValue();

        if (entities.hasClient(id)) {
            entities.getClient(id).close();
        } else {
            MongoCursor<BsonDocument> cursor = entities.getCursor(id);
            cursor.close();
        }

        return OperationResult.NONE;
    }

    public OperationResult executeRunCommand(final BsonDocument operation) {
        MongoDatabase database = getMongoDatabase(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        ClientSession session = getSession(arguments);
        BsonDocument command = arguments.getDocument("command");
        ReadPreference readPreference = arguments.containsKey("readPreference")
                ? asReadPreference(arguments.getDocument("readPreference")) : null;
        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "command":
                case "commandName":
                case "session":
                case "readPreference":
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }

        return resultOf(() -> {
            if (session == null) {
                if (readPreference == null) {
                    return database.runCommand(command, BsonDocument.class);
                } else {
                    return database.runCommand(command, readPreference, BsonDocument.class);
                }
            } else {
                if (readPreference == null) {
                    return database.runCommand(session, command, BsonDocument.class);
                } else {
                    return database.runCommand(session, command, readPreference, BsonDocument.class);
                }
            }
        });
    }

    public OperationResult executeCountDocuments(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = getMongoCollection(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        BsonDocument filter = arguments.getDocument("filter");
        ClientSession session = getSession(arguments);
        CountOptions options = new CountOptions();

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "filter":
                case "session":
                    break;
                case "skip":
                    options.skip(cur.getValue().asNumber().intValue());
                    break;
                case "limit":
                    options.limit(cur.getValue().asNumber().intValue());
                    break;
                case "comment":
                    options.comment(cur.getValue());
                    break;
                case "collation":
                    options.collation(asCollation(cur.getValue().asDocument()));
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
        MongoCollection<BsonDocument> collection = getMongoCollection(operation);
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

    @NonNull
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

    @NonNull
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

    private MongoCollection<BsonDocument> getMongoCollection(final BsonDocument operation) {
        MongoCollection<BsonDocument> collection = entities.getCollection(operation.getString("object").getValue());
        Long timeoutMS = getAndRemoveTimeoutMS(operation.getDocument("arguments", new BsonDocument()));
        if (timeoutMS != null) {
            collection = collection.withTimeout(timeoutMS, TimeUnit.MILLISECONDS);
        }
        return collection;
    }
    private MongoDatabase getMongoDatabase(final BsonDocument operation) {
        MongoDatabase database = entities.getDatabase(operation.getString("object").getValue());
        if (operation.containsKey("arguments")) {
            BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
            Long timeoutMS = getAndRemoveTimeoutMS(arguments);
            if (timeoutMS != null) {
                database = database.withTimeout(timeoutMS, TimeUnit.MILLISECONDS);
                arguments.remove("timeoutMS");
            }
        }
        return database;
    }

    private MongoCluster getMongoCluster(final BsonDocument operation) {
        MongoCluster mongoCluster = entities.getClient(operation.getString("object").getValue());
        if (operation.containsKey("arguments")) {
            BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
            Long timeoutMS = getAndRemoveTimeoutMS(arguments);
            if (timeoutMS != null) {
                mongoCluster = mongoCluster.withTimeout(timeoutMS, TimeUnit.MILLISECONDS);
                arguments.remove("timeoutMS");
            }
        }
        return mongoCluster;
    }

    private static void setCursorType(final FindIterable<BsonDocument> iterable, final Map.Entry<String, BsonValue> cur) {
        switch (cur.getValue().asString().getValue()) {
            case "tailable":
                iterable.cursorType(CursorType.Tailable);
                break;
            case "nonTailable":
                iterable.cursorType(CursorType.NonTailable);
                break;
            case "tailableAwait":
                iterable.cursorType(CursorType.TailableAwait);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported cursorType: " + cur.getValue());
        }
    }

    private static void setTimeoutMode(final MongoIterable<BsonDocument> iterable, final Map.Entry<String, BsonValue> cur) {
         switch (cur.getValue().asString().getValue()) {
            case "cursorLifetime":
                invokeTimeoutMode(iterable, TimeoutMode.CURSOR_LIFETIME);
                break;
            case "iteration":
                invokeTimeoutMode(iterable, TimeoutMode.ITERATION);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported timeoutMode: " + cur.getValue());
        }
    }

    private static void invokeTimeoutMode(final MongoIterable<BsonDocument> iterable, final TimeoutMode timeoutMode) {
        try {
            Method timeoutModeMethod = iterable.getClass().getDeclaredMethod("timeoutMode", TimeoutMode.class);
            timeoutModeMethod.setAccessible(true);
            timeoutModeMethod.invoke(iterable, timeoutMode);
        } catch (NoSuchMethodException e) {
            throw new UnsupportedOperationException("Unsupported timeoutMode method for class: " + iterable.getClass(), e);
        } catch (IllegalAccessException e) {
            throw new UnsupportedOperationException("Unable to set timeoutMode method for class: " + iterable.getClass(), e);
        } catch (InvocationTargetException e) {
            Throwable targetException = e.getTargetException();
            if (targetException instanceof IllegalArgumentException) {
                throw (IllegalArgumentException) targetException;
            }
            throw new UnsupportedOperationException("Unable to set timeoutMode method for class: " + iterable.getClass(), targetException);
        }
    }
}
