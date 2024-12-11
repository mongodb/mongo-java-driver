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

package com.mongodb.internal.operation;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.ClusteredIndexOptions;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.CreateViewOptions;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.DropCollectionOptions;
import com.mongodb.client.model.DropIndexOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptionDefaults;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.SearchIndexModel;
import com.mongodb.client.model.SearchIndexType;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mongodb.internal.bulk.DeleteRequest;
import com.mongodb.internal.bulk.IndexRequest;
import com.mongodb.internal.bulk.InsertRequest;
import com.mongodb.internal.bulk.UpdateRequest;
import com.mongodb.internal.bulk.WriteRequest;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.client.model.FindOptions;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonJavaScript;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.codecs.Codec;
import org.bson.codecs.CollectibleCodec;
import org.bson.codecs.Decoder;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;
import static java.util.Collections.singletonList;

final class Operations<TDocument> {
    private final MongoNamespace namespace;
    private final Class<TDocument> documentClass;
    private final ReadPreference readPreference;
    private final CodecRegistry codecRegistry;
    private final ReadConcern readConcern;
    private final WriteConcern writeConcern;
    private final boolean retryWrites;
    private final boolean retryReads;

    Operations(@Nullable final MongoNamespace namespace, final Class<TDocument> documentClass, final ReadPreference readPreference,
            final CodecRegistry codecRegistry, final ReadConcern readConcern, final WriteConcern writeConcern, final boolean retryWrites,
            final boolean retryReads) {
        this.namespace = namespace;
        this.documentClass = documentClass;
        this.readPreference = readPreference;
        this.codecRegistry = codecRegistry;
        this.readConcern = readConcern;
        this.writeConcern = writeConcern;
        this.retryWrites = retryWrites;
        this.retryReads = retryReads;
    }

    @Nullable
    MongoNamespace getNamespace() {
        return namespace;
    }

    Class<TDocument> getDocumentClass() {
        return documentClass;
    }

    ReadPreference getReadPreference() {
        return readPreference;
    }

    CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    ReadConcern getReadConcern() {
        return readConcern;
    }

    WriteConcern getWriteConcern() {
        return writeConcern;
    }

    boolean isRetryWrites() {
        return retryWrites;
    }

    boolean isRetryReads() {
        return retryReads;
    }

    CountDocumentsOperation countDocuments(final Bson filter, final CountOptions options) {
        CountDocumentsOperation operation = new CountDocumentsOperation(
                assertNotNull(namespace))
                .retryReads(retryReads)
                .filter(toBsonDocument(filter))
                .skip(options.getSkip())
                .limit(options.getLimit())
                .collation(options.getCollation())
                .comment(options.getComment());
        if (options.getHint() != null) {
            operation.hint(toBsonDocument(options.getHint()));
        } else if (options.getHintString() != null) {
            operation.hint(new BsonString(options.getHintString()));
        }
        return operation;
    }

    EstimatedDocumentCountOperation estimatedDocumentCount(final EstimatedDocumentCountOptions options) {
        return new EstimatedDocumentCountOperation(
                assertNotNull(namespace))
                .retryReads(retryReads)
                .comment(options.getComment());
    }

    <TResult> FindOperation<TResult> findFirst(final Bson filter, final Class<TResult> resultClass,
                                                      final FindOptions options) {
        return createFindOperation(assertNotNull(namespace), filter, resultClass, options).batchSize(0).limit(-1);
    }

    <TResult> FindOperation<TResult> find(final Bson filter, final Class<TResult> resultClass,
                                                 final FindOptions options) {
        return createFindOperation(assertNotNull(namespace), filter, resultClass, options);
    }

    <TResult> FindOperation<TResult> find(final MongoNamespace findNamespace, @Nullable final Bson filter,
                                                 final Class<TResult> resultClass, final FindOptions options) {
        return createFindOperation(findNamespace, filter, resultClass, options);
    }

    private <TResult> FindOperation<TResult> createFindOperation(final MongoNamespace findNamespace, @Nullable final Bson filter,
                                                                 final Class<TResult> resultClass, final FindOptions options) {
        FindOperation<TResult> operation = new FindOperation<>(
                findNamespace, codecRegistry.get(resultClass))
                .retryReads(retryReads)
                .filter(filter == null ? new BsonDocument() : filter.toBsonDocument(documentClass, codecRegistry))
                .batchSize(options.getBatchSize())
                .skip(options.getSkip())
                .limit(options.getLimit())
                .projection(toBsonDocument(options.getProjection()))
                .sort(toBsonDocument(options.getSort()))
                .cursorType(options.getCursorType())
                .noCursorTimeout(options.isNoCursorTimeout())
                .partial(options.isPartial())
                .collation(options.getCollation())
                .comment(options.getComment())
                .let(toBsonDocument(options.getLet()))
                .min(toBsonDocument(options.getMin()))
                .max(toBsonDocument(options.getMax()))
                .returnKey(options.isReturnKey())
                .showRecordId(options.isShowRecordId())
                .allowDiskUse(options.isAllowDiskUse())
                .timeoutMode(options.getTimeoutMode());

        if (options.getHint() != null) {
            operation.hint(toBsonDocument(options.getHint()));
        } else if (options.getHintString() != null) {
            operation.hint(new BsonString(options.getHintString()));
        }
        return operation;
    }

    <TResult> DistinctOperation<TResult> distinct(final String fieldName, @Nullable final Bson filter, final Class<TResult> resultClass,
            final Collation collation, final BsonValue comment, @Nullable final Bson hint, @Nullable final String hintString) {
        return new DistinctOperation<>(assertNotNull(namespace),
                fieldName, codecRegistry.get(resultClass))
                .retryReads(retryReads)
                .filter(filter == null ? null : filter.toBsonDocument(documentClass, codecRegistry))
                .collation(collation)
                .comment(comment)
                .hint(hint != null ? toBsonDocument(hint) : (hintString != null ? new BsonString(hintString) : null));
    }

    <TResult> AggregateOperation<TResult> aggregate(final List<? extends Bson> pipeline, final Class<TResult> resultClass,
            @Nullable final TimeoutMode timeoutMode, @Nullable final Integer batchSize,
            final Collation collation, @Nullable final Bson hint, @Nullable final String hintString,
            final BsonValue comment, final Bson variables, final Boolean allowDiskUse, final AggregationLevel aggregationLevel) {
        return new AggregateOperation<>(assertNotNull(namespace),
                assertNotNull(toBsonDocumentList(pipeline)), codecRegistry.get(resultClass), aggregationLevel)
                .retryReads(retryReads)
                .allowDiskUse(allowDiskUse)
                .batchSize(batchSize)
                .collation(collation)
                .hint(hint != null ? toBsonDocument(hint) : (hintString != null ? new BsonString(hintString) : null))
                .comment(comment)
                .let(toBsonDocument(variables))
                .timeoutMode(timeoutMode);
    }

    AggregateToCollectionOperation aggregateToCollection(final List<? extends Bson> pipeline, @Nullable final TimeoutMode timeoutMode,
            final Boolean allowDiskUse, final Boolean bypassDocumentValidation, final Collation collation, @Nullable final Bson hint,
            @Nullable final String hintString, final BsonValue comment, final Bson variables, final AggregationLevel aggregationLevel) {
        return new AggregateToCollectionOperation(assertNotNull(namespace),
                assertNotNull(toBsonDocumentList(pipeline)), readConcern, writeConcern, aggregationLevel)
                .allowDiskUse(allowDiskUse)
                .bypassDocumentValidation(bypassDocumentValidation)
                .collation(collation)
                .hint(hint != null ? toBsonDocument(hint) : (hintString != null ? new BsonString(hintString) : null))
                .comment(comment)
                .let(toBsonDocument(variables))
                .timeoutMode(timeoutMode);
    }

    @SuppressWarnings("deprecation")
    MapReduceToCollectionOperation mapReduceToCollection(final String databaseName, final String collectionName,
                                                                final String mapFunction, final String reduceFunction,
                                                                @Nullable final String finalizeFunction, final Bson filter,
                                                                final int limit, final boolean jsMode,
                                                                final Bson scope, final Bson sort, final boolean verbose,
                                                                final com.mongodb.client.model.MapReduceAction action,
                                                                final Boolean bypassDocumentValidation, final Collation collation) {
        MapReduceToCollectionOperation operation = new MapReduceToCollectionOperation(
                assertNotNull(namespace), new BsonJavaScript(mapFunction),
                new BsonJavaScript(reduceFunction), collectionName, writeConcern)
                .filter(toBsonDocument(filter))
                .limit(limit)
                .jsMode(jsMode)
                .scope(toBsonDocument(scope))
                .sort(toBsonDocument(sort))
                .verbose(verbose)
                .action(action.getValue())
                .databaseName(databaseName)
                .bypassDocumentValidation(bypassDocumentValidation)
                .collation(collation);

        if (finalizeFunction != null) {
            operation.finalizeFunction(new BsonJavaScript(finalizeFunction));
        }
        return operation;
    }

    <TResult> MapReduceWithInlineResultsOperation<TResult> mapReduce(final String mapFunction, final String reduceFunction,
            @Nullable final String finalizeFunction, final Class<TResult> resultClass, final Bson filter, final int limit,
            final boolean jsMode, final Bson scope, final Bson sort, final boolean verbose,
            final Collation collation) {
        MapReduceWithInlineResultsOperation<TResult> operation =
                new MapReduceWithInlineResultsOperation<>(
                        assertNotNull(namespace), new BsonJavaScript(mapFunction), new BsonJavaScript(reduceFunction),
                        codecRegistry.get(resultClass))
                        .filter(toBsonDocument(filter))
                        .limit(limit)
                        .jsMode(jsMode)
                        .scope(toBsonDocument(scope))
                        .sort(toBsonDocument(sort))
                        .verbose(verbose)
                        .collation(collation);
        if (finalizeFunction != null) {
            operation.finalizeFunction(new BsonJavaScript(finalizeFunction));
        }
        return operation;
    }

    FindAndDeleteOperation<TDocument> findOneAndDelete(final Bson filter, final FindOneAndDeleteOptions options) {
        return new FindAndDeleteOperation<>(
                assertNotNull(namespace), writeConcern, retryWrites, getCodec())
                .filter(toBsonDocument(filter))
                .projection(toBsonDocument(options.getProjection()))
                .sort(toBsonDocument(options.getSort()))
                .collation(options.getCollation())
                .hint(toBsonDocument(options.getHint()))
                .hintString(options.getHintString())
                .comment(options.getComment())
                .let(toBsonDocument(options.getLet()));
    }

    FindAndReplaceOperation<TDocument> findOneAndReplace(final Bson filter, final TDocument replacement,
                                                                final FindOneAndReplaceOptions options) {
        return new FindAndReplaceOperation<>(
                assertNotNull(namespace), writeConcern, retryWrites, getCodec(), documentToBsonDocument(replacement))
                .filter(toBsonDocument(filter))
                .projection(toBsonDocument(options.getProjection()))
                .sort(toBsonDocument(options.getSort()))
                .returnOriginal(options.getReturnDocument() == ReturnDocument.BEFORE)
                .upsert(options.isUpsert())
                .bypassDocumentValidation(options.getBypassDocumentValidation())
                .collation(options.getCollation())
                .hint(toBsonDocument(options.getHint()))
                .hintString(options.getHintString())
                .comment(options.getComment())
                .let(toBsonDocument(options.getLet()));
    }

    FindAndUpdateOperation<TDocument> findOneAndUpdate(final Bson filter, final Bson update, final FindOneAndUpdateOptions options) {
        return new FindAndUpdateOperation<>(
                assertNotNull(namespace), writeConcern, retryWrites, getCodec(), assertNotNull(toBsonDocument(update)))
                .filter(toBsonDocument(filter))
                .projection(toBsonDocument(options.getProjection()))
                .sort(toBsonDocument(options.getSort()))
                .returnOriginal(options.getReturnDocument() == ReturnDocument.BEFORE)
                .upsert(options.isUpsert())
                .bypassDocumentValidation(options.getBypassDocumentValidation())
                .collation(options.getCollation())
                .arrayFilters(toBsonDocumentList(options.getArrayFilters()))
                .hint(toBsonDocument(options.getHint()))
                .hintString(options.getHintString())
                .comment(options.getComment())
                .let(toBsonDocument(options.getLet()));
    }

    FindAndUpdateOperation<TDocument> findOneAndUpdate(final Bson filter, final List<? extends Bson> update,
                                                       final FindOneAndUpdateOptions options) {
        return new FindAndUpdateOperation<>(
                assertNotNull(namespace), writeConcern, retryWrites, getCodec(), assertNotNull(toBsonDocumentList(update)))
                .filter(toBsonDocument(filter))
                .projection(toBsonDocument(options.getProjection()))
                .sort(toBsonDocument(options.getSort()))
                .returnOriginal(options.getReturnDocument() == ReturnDocument.BEFORE)
                .upsert(options.isUpsert())
                .bypassDocumentValidation(options.getBypassDocumentValidation())
                .collation(options.getCollation())
                .arrayFilters(toBsonDocumentList(options.getArrayFilters()))
                .hint(toBsonDocument(options.getHint()))
                .hintString(options.getHintString())
                .comment(options.getComment())
                .let(toBsonDocument(options.getLet()));
    }


    MixedBulkWriteOperation insertOne(final TDocument document, final InsertOneOptions options) {
        return bulkWrite(singletonList(new InsertOneModel<>(document)),
                new BulkWriteOptions().bypassDocumentValidation(options.getBypassDocumentValidation()).comment(options.getComment()));
    }


    MixedBulkWriteOperation replaceOne(final Bson filter, final TDocument replacement, final ReplaceOptions options) {
        return bulkWrite(singletonList(new ReplaceOneModel<>(filter, replacement, options)),
                new BulkWriteOptions().bypassDocumentValidation(options.getBypassDocumentValidation())
                        .comment(options.getComment()).let(options.getLet()));
    }

    MixedBulkWriteOperation deleteOne(final Bson filter, final DeleteOptions options) {
        return bulkWrite(singletonList(new DeleteOneModel<>(filter, options)),
                new BulkWriteOptions().comment(options.getComment()).let(options.getLet()));
    }

    MixedBulkWriteOperation deleteMany(final Bson filter, final DeleteOptions options) {
        return bulkWrite(singletonList(new DeleteManyModel<>(filter, options)),
                new BulkWriteOptions().comment(options.getComment()).let(options.getLet()));
    }

    MixedBulkWriteOperation updateOne(final Bson filter, final Bson update, final UpdateOptions options) {
        return bulkWrite(singletonList(new UpdateOneModel<>(filter, update, options)),
                new BulkWriteOptions().bypassDocumentValidation(options.getBypassDocumentValidation())
                        .comment(options.getComment()).let(options.getLet()));
    }

    MixedBulkWriteOperation updateOne(final Bson filter, final List<? extends Bson> update, final UpdateOptions options) {
        return bulkWrite(singletonList(new UpdateOneModel<>(filter, update, options)),
                new BulkWriteOptions().bypassDocumentValidation(options.getBypassDocumentValidation())
                        .comment(options.getComment()).let(options.getLet()));
    }

    MixedBulkWriteOperation updateMany(final Bson filter, final Bson update, final UpdateOptions options) {
        return bulkWrite(singletonList(new UpdateManyModel<>(filter, update, options)),
                new BulkWriteOptions().bypassDocumentValidation(options.getBypassDocumentValidation())
                        .comment(options.getComment()).let(options.getLet()));
    }

    MixedBulkWriteOperation updateMany(final Bson filter, final List<? extends Bson> update, final UpdateOptions options) {
        return bulkWrite(singletonList(new UpdateManyModel<>(filter, update, options)),
                new BulkWriteOptions().bypassDocumentValidation(options.getBypassDocumentValidation())
                        .comment(options.getComment()).let(options.getLet()));
    }

    MixedBulkWriteOperation insertMany(final List<? extends TDocument> documents, final InsertManyOptions options) {
        notNull("documents", documents);
        List<InsertRequest> requests = new ArrayList<>(documents.size());
        for (TDocument document : documents) {
            if (document == null) {
                throw new IllegalArgumentException("documents can not contain a null value");
            }
            if (getCodec() instanceof CollectibleCodec) {
                document = ((CollectibleCodec<TDocument>) getCodec()).generateIdIfAbsentFromDocument(document);
            }
            requests.add(new InsertRequest(documentToBsonDocument(document)));
        }

        return new MixedBulkWriteOperation(assertNotNull(namespace),
                requests, options.isOrdered(), writeConcern, retryWrites)
                .bypassDocumentValidation(options.getBypassDocumentValidation())
                .comment(options.getComment());
    }

    @SuppressWarnings("unchecked")
    MixedBulkWriteOperation bulkWrite(final List<? extends WriteModel<? extends TDocument>> requests, final BulkWriteOptions options) {
        notNull("requests", requests);
        List<WriteRequest> writeRequests = new ArrayList<>(requests.size());
        for (WriteModel<? extends TDocument> writeModel : requests) {
            WriteRequest writeRequest;
            if (writeModel == null) {
                throw new IllegalArgumentException("requests can not contain a null value");
            } else if (writeModel instanceof InsertOneModel) {
                TDocument document = ((InsertOneModel<TDocument>) writeModel).getDocument();
                if (getCodec() instanceof CollectibleCodec) {
                    document = ((CollectibleCodec<TDocument>) getCodec()).generateIdIfAbsentFromDocument(document);
                }
                writeRequest = new InsertRequest(documentToBsonDocument(document));
            } else if (writeModel instanceof ReplaceOneModel) {
                ReplaceOneModel<TDocument> replaceOneModel = (ReplaceOneModel<TDocument>) writeModel;
                writeRequest = new UpdateRequest(assertNotNull(toBsonDocument(replaceOneModel.getFilter())),
                        documentToBsonDocument(replaceOneModel.getReplacement()), WriteRequest.Type.REPLACE)
                        .upsert(replaceOneModel.getReplaceOptions().isUpsert())
                        .collation(replaceOneModel.getReplaceOptions().getCollation())
                        .hint(toBsonDocument(replaceOneModel.getReplaceOptions().getHint()))
                        .hintString(replaceOneModel.getReplaceOptions().getHintString());
            } else if (writeModel instanceof UpdateOneModel) {
                UpdateOneModel<TDocument> updateOneModel = (UpdateOneModel<TDocument>) writeModel;
                BsonValue update = updateOneModel.getUpdate() != null ? toBsonDocument(updateOneModel.getUpdate())
                        : new BsonArray(toBsonDocumentList(updateOneModel.getUpdatePipeline()));
                writeRequest = new UpdateRequest(assertNotNull(toBsonDocument(updateOneModel.getFilter())), update, WriteRequest.Type.UPDATE)
                        .multi(false)
                        .upsert(updateOneModel.getOptions().isUpsert())
                        .collation(updateOneModel.getOptions().getCollation())
                        .arrayFilters(toBsonDocumentList(updateOneModel.getOptions().getArrayFilters()))
                        .hint(toBsonDocument(updateOneModel.getOptions().getHint()))
                        .hintString(updateOneModel.getOptions().getHintString());
            } else if (writeModel instanceof UpdateManyModel) {
                UpdateManyModel<TDocument> updateManyModel = (UpdateManyModel<TDocument>) writeModel;
                BsonValue update = updateManyModel.getUpdate() != null ? toBsonDocument(updateManyModel.getUpdate())
                        : new BsonArray(toBsonDocumentList(updateManyModel.getUpdatePipeline()));
                writeRequest = new UpdateRequest(assertNotNull(toBsonDocument(updateManyModel.getFilter())), update, WriteRequest.Type.UPDATE)
                        .multi(true)
                        .upsert(updateManyModel.getOptions().isUpsert())
                        .collation(updateManyModel.getOptions().getCollation())
                        .arrayFilters(toBsonDocumentList(updateManyModel.getOptions().getArrayFilters()))
                        .hint(toBsonDocument(updateManyModel.getOptions().getHint()))
                        .hintString(updateManyModel.getOptions().getHintString());
            } else if (writeModel instanceof DeleteOneModel) {
                DeleteOneModel<TDocument> deleteOneModel = (DeleteOneModel<TDocument>) writeModel;
                writeRequest = new DeleteRequest(assertNotNull(toBsonDocument(deleteOneModel.getFilter()))).multi(false)
                        .collation(deleteOneModel.getOptions().getCollation())
                        .hint(toBsonDocument(deleteOneModel.getOptions().getHint()))
                        .hintString(deleteOneModel.getOptions().getHintString());
            } else if (writeModel instanceof DeleteManyModel) {
                DeleteManyModel<TDocument> deleteManyModel = (DeleteManyModel<TDocument>) writeModel;
                writeRequest = new DeleteRequest(assertNotNull(toBsonDocument(deleteManyModel.getFilter()))).multi(true)
                        .collation(deleteManyModel.getOptions().getCollation())
                        .hint(toBsonDocument(deleteManyModel.getOptions().getHint()))
                        .hintString(deleteManyModel.getOptions().getHintString());
            } else {
                throw new UnsupportedOperationException(format("WriteModel of type %s is not supported", writeModel.getClass()));
            }
            writeRequests.add(writeRequest);
        }

        return new MixedBulkWriteOperation(assertNotNull(namespace), writeRequests,
                options.isOrdered(), writeConcern, retryWrites)
                .bypassDocumentValidation(options.getBypassDocumentValidation())
                .comment(options.getComment())
                .let(toBsonDocument(options.getLet()));
    }

    <TResult> CommandReadOperation<TResult> commandRead(final Bson command, final Class<TResult> resultClass) {
        notNull("command", command);
        notNull("resultClass", resultClass);
        return new CommandReadOperation<>(assertNotNull(namespace).getDatabaseName(),
                                          assertNotNull(toBsonDocument(command)), codecRegistry.get(resultClass));
    }


    DropDatabaseOperation dropDatabase() {
        return new DropDatabaseOperation(assertNotNull(namespace).getDatabaseName(),
                getWriteConcern());
    }

    CreateCollectionOperation createCollection(final String collectionName, final CreateCollectionOptions createCollectionOptions,
            @Nullable final AutoEncryptionSettings autoEncryptionSettings) {
        CreateCollectionOperation operation = new CreateCollectionOperation(
                assertNotNull(namespace).getDatabaseName(), collectionName, writeConcern)
                .collation(createCollectionOptions.getCollation())
                .capped(createCollectionOptions.isCapped())
                .sizeInBytes(createCollectionOptions.getSizeInBytes())
                .maxDocuments(createCollectionOptions.getMaxDocuments())
                .storageEngineOptions(toBsonDocument(createCollectionOptions.getStorageEngineOptions()))
                .expireAfter(createCollectionOptions.getExpireAfter(TimeUnit.SECONDS))
                .timeSeriesOptions(createCollectionOptions.getTimeSeriesOptions())
                .changeStreamPreAndPostImagesOptions(createCollectionOptions.getChangeStreamPreAndPostImagesOptions());

        ClusteredIndexOptions clusteredIndexOptions = createCollectionOptions.getClusteredIndexOptions();
        if (clusteredIndexOptions != null) {
            operation.clusteredIndexKey(toBsonDocument(clusteredIndexOptions.getKey()));
            operation.clusteredIndexUnique(clusteredIndexOptions.isUnique());
            operation.clusteredIndexName(clusteredIndexOptions.getName());
        }

        Bson encryptedFields = createCollectionOptions.getEncryptedFields();
        operation.encryptedFields(toBsonDocument(encryptedFields));
        if (encryptedFields == null && autoEncryptionSettings != null) {
            Map<String, BsonDocument> encryptedFieldsMap = autoEncryptionSettings.getEncryptedFieldsMap();
            if (encryptedFieldsMap != null) {
                operation.encryptedFields(encryptedFieldsMap.getOrDefault(namespace.getDatabaseName() + "." + collectionName, null));
            }
        }

        IndexOptionDefaults indexOptionDefaults = createCollectionOptions.getIndexOptionDefaults();
        Bson storageEngine = indexOptionDefaults.getStorageEngine();
        if (storageEngine != null) {
            operation.indexOptionDefaults(new BsonDocument("storageEngine", toBsonDocument(storageEngine)));
        }
        ValidationOptions validationOptions = createCollectionOptions.getValidationOptions();
        Bson validator = validationOptions.getValidator();
        operation.validator(toBsonDocument(validator));
        operation.validationLevel(validationOptions.getValidationLevel());
        operation.validationAction(validationOptions.getValidationAction());
        return operation;
    }

    DropCollectionOperation dropCollection(
            final DropCollectionOptions dropCollectionOptions,
            @Nullable final AutoEncryptionSettings autoEncryptionSettings) {
        DropCollectionOperation operation = new DropCollectionOperation(
                assertNotNull(namespace), writeConcern);
        Bson encryptedFields = dropCollectionOptions.getEncryptedFields();
        if (encryptedFields != null) {
            operation.encryptedFields(assertNotNull(toBsonDocument(encryptedFields)));
        } else if (autoEncryptionSettings != null) {
            Map<String, BsonDocument> encryptedFieldsMap = autoEncryptionSettings.getEncryptedFieldsMap();
            if (encryptedFieldsMap != null) {
                operation.encryptedFields(encryptedFieldsMap.getOrDefault(namespace.getFullName(), null));
                operation.autoEncryptedFields(true);
            }
        }
        return operation;
    }


    RenameCollectionOperation renameCollection(final MongoNamespace newCollectionNamespace,
            final RenameCollectionOptions renameCollectionOptions) {
        return new RenameCollectionOperation(assertNotNull(namespace),
                newCollectionNamespace, writeConcern).dropTarget(renameCollectionOptions.isDropTarget());
    }

    CreateViewOperation createView(final String viewName, final String viewOn, final List<? extends Bson> pipeline,
            final CreateViewOptions createViewOptions) {
        notNull("options", createViewOptions);
        notNull("pipeline", pipeline);
        return new CreateViewOperation(assertNotNull(namespace).getDatabaseName(), viewName,
                viewOn, assertNotNull(toBsonDocumentList(pipeline)), writeConcern).collation(createViewOptions.getCollation());
    }

    CreateIndexesOperation createIndexes(final List<IndexModel> indexes, final CreateIndexOptions createIndexOptions) {
        notNull("indexes", indexes);
        notNull("createIndexOptions", createIndexOptions);
        List<IndexRequest> indexRequests = new ArrayList<>(indexes.size());
        for (IndexModel model : indexes) {
            if (model == null) {
                throw new IllegalArgumentException("indexes can not contain a null value");
            }
            indexRequests.add(new IndexRequest(assertNotNull(toBsonDocument(model.getKeys())))
                    .name(model.getOptions().getName())
                    .background(model.getOptions().isBackground())
                    .unique(model.getOptions().isUnique())
                    .sparse(model.getOptions().isSparse())
                    .expireAfter(model.getOptions().getExpireAfter(TimeUnit.SECONDS), TimeUnit.SECONDS)
                    .version(model.getOptions().getVersion())
                    .weights(toBsonDocument(model.getOptions().getWeights()))
                    .defaultLanguage(model.getOptions().getDefaultLanguage())
                    .languageOverride(model.getOptions().getLanguageOverride())
                    .textVersion(model.getOptions().getTextVersion())
                    .sphereVersion(model.getOptions().getSphereVersion())
                    .bits(model.getOptions().getBits())
                    .min(model.getOptions().getMin())
                    .max(model.getOptions().getMax())
                    .storageEngine(toBsonDocument(model.getOptions().getStorageEngine()))
                    .partialFilterExpression(toBsonDocument(model.getOptions().getPartialFilterExpression()))
                    .collation(model.getOptions().getCollation())
                    .wildcardProjection(toBsonDocument(model.getOptions().getWildcardProjection()))
                    .hidden(model.getOptions().isHidden())
            );
        }
        return new CreateIndexesOperation(
                assertNotNull(namespace), indexRequests, writeConcern)
                .commitQuorum(createIndexOptions.getCommitQuorum());
    }

    CreateSearchIndexesOperation createSearchIndexes(final List<SearchIndexModel> indexes) {
        List<SearchIndexRequest> indexRequests = indexes.stream()
                .map(this::createSearchIndexRequest)
                .collect(Collectors.toList());
        return new CreateSearchIndexesOperation(assertNotNull(namespace), indexRequests);
    }

    UpdateSearchIndexesOperation updateSearchIndex(final String indexName, final Bson definition) {
        BsonDocument definitionDocument = assertNotNull(toBsonDocument(definition));
        SearchIndexRequest searchIndexRequest = new SearchIndexRequest(definitionDocument, indexName);
        return new UpdateSearchIndexesOperation(assertNotNull(namespace), searchIndexRequest);
    }


    DropSearchIndexOperation dropSearchIndex(final String indexName) {
        return new DropSearchIndexOperation(assertNotNull(namespace), indexName);
    }


    <TResult> ListSearchIndexesOperation<TResult> listSearchIndexes(final Class<TResult> resultClass,
            @Nullable final String indexName, @Nullable final Integer batchSize, @Nullable final Collation collation,
            @Nullable final BsonValue comment, @Nullable final Boolean allowDiskUse) {
        return new ListSearchIndexesOperation<>(assertNotNull(namespace),
                codecRegistry.get(resultClass), indexName, batchSize, collation, comment, allowDiskUse, retryReads);
    }

    DropIndexOperation dropIndex(final String indexName, final DropIndexOptions ignoredOptions) {
        return new DropIndexOperation(assertNotNull(namespace), indexName, writeConcern);
    }

    DropIndexOperation dropIndex(final Bson keys, final DropIndexOptions ignoredOptions) {
        return new DropIndexOperation(assertNotNull(namespace), keys.toBsonDocument(BsonDocument.class, codecRegistry), writeConcern);
    }

    <TResult> ListCollectionsOperation<TResult> listCollections(final String databaseName, final Class<TResult> resultClass,
                                                                final Bson filter, final boolean collectionNamesOnly,
                                                                final boolean authorizedCollections,
                                                                @Nullable final Integer batchSize,
                                                                final BsonValue comment, @Nullable final TimeoutMode timeoutMode) {
        return new ListCollectionsOperation<>(databaseName, codecRegistry.get(resultClass))
                .retryReads(retryReads)
                .filter(toBsonDocument(filter))
                .nameOnly(collectionNamesOnly)
                .authorizedCollections(authorizedCollections)
                .batchSize(batchSize == null ? 0 : batchSize)
                .comment(comment)
                .timeoutMode(timeoutMode);
    }

    <TResult> ListDatabasesOperation<TResult> listDatabases(final Class<TResult> resultClass, final Bson filter,
                                                            final Boolean nameOnly,
                                                            final Boolean authorizedDatabasesOnly, final BsonValue comment) {
        return new ListDatabasesOperation<>(codecRegistry.get(resultClass))
                .retryReads(retryReads)
                .filter(toBsonDocument(filter))
                .nameOnly(nameOnly)
                .authorizedDatabasesOnly(authorizedDatabasesOnly)
                .comment(comment);
    }

    <TResult> ListIndexesOperation<TResult> listIndexes(final Class<TResult> resultClass, @Nullable final Integer batchSize,
            final BsonValue comment, @Nullable final TimeoutMode timeoutMode) {
        return new ListIndexesOperation<>(assertNotNull(namespace),
                codecRegistry.get(resultClass))
                .retryReads(retryReads)
                .batchSize(batchSize == null ? 0 : batchSize)
                .comment(comment)
                .timeoutMode(timeoutMode);
    }

    <TResult> ChangeStreamOperation<TResult> changeStream(final FullDocument fullDocument,
            final FullDocumentBeforeChange fullDocumentBeforeChange, final List<? extends Bson> pipeline,
            final Decoder<TResult> decoder, final ChangeStreamLevel changeStreamLevel, @Nullable final Integer batchSize,
            final Collation collation, final BsonValue comment, final BsonDocument resumeToken,
            final BsonTimestamp startAtOperationTime, final BsonDocument startAfter, final boolean showExpandedEvents) {
        return new ChangeStreamOperation<>(
                assertNotNull(namespace),
                fullDocument,
                fullDocumentBeforeChange,
                assertNotNull(toBsonDocumentList(pipeline)), decoder, changeStreamLevel)
                .batchSize(batchSize)
                .collation(collation)
                .comment(comment)
                .resumeAfter(resumeToken)
                .startAtOperationTime(startAtOperationTime)
                .startAfter(startAfter)
                .showExpandedEvents(showExpandedEvents)
                .retryReads(retryReads);
    }

    private Codec<TDocument> getCodec() {
        return codecRegistry.get(documentClass);
    }

    private BsonDocument documentToBsonDocument(final TDocument document) {
        if (document instanceof BsonDocument) {
            return (BsonDocument) document;
        } else {
            return new BsonDocumentWrapper<>(document, getCodec());
        }
    }

    @Nullable
    private BsonDocument toBsonDocument(@Nullable final Bson bson) {
        return bson == null ? null : bson.toBsonDocument(documentClass, codecRegistry);
    }

    @Nullable
    private List<BsonDocument> toBsonDocumentList(@Nullable final List<? extends Bson> bsonList) {
        if (bsonList == null) {
            return null;
        }
        List<BsonDocument> bsonDocumentList = new ArrayList<>(bsonList.size());
        for (Bson cur : bsonList) {
            if (cur == null) {
                throw new IllegalArgumentException("All documents in the list must be non-null");
            }
            bsonDocumentList.add(toBsonDocument(cur));
        }
        return bsonDocumentList;
    }

    private SearchIndexRequest createSearchIndexRequest(final SearchIndexModel model) {
        BsonDocument definition = assertNotNull(toBsonDocument(model.getDefinition()));
        String indexName = model.getName();
        SearchIndexType searchIndexType = model.getType();

        return new SearchIndexRequest(definition, indexName, searchIndexType);
    }
}
