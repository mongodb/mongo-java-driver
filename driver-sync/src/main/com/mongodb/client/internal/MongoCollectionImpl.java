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

package com.mongodb.client.internal;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.MongoWriteException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernResult;
import com.mongodb.WriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.ListSearchIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.DropCollectionOptions;
import com.mongodb.client.model.DropIndexOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.SearchIndexModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.bulk.WriteRequest;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.internal.operation.IndexHelper;
import com.mongodb.internal.operation.Operations;
import com.mongodb.internal.operation.WriteOperation;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.assertions.Assertions.notNullElements;
import static com.mongodb.internal.bulk.WriteRequest.Type.DELETE;
import static com.mongodb.internal.bulk.WriteRequest.Type.INSERT;
import static com.mongodb.internal.bulk.WriteRequest.Type.REPLACE;
import static com.mongodb.internal.bulk.WriteRequest.Type.UPDATE;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.bson.codecs.configuration.CodecRegistries.withUuidRepresentation;

class MongoCollectionImpl<TDocument> implements MongoCollection<TDocument> {
    private final MongoNamespace namespace;
    private final Class<TDocument> documentClass;
    private final ReadPreference readPreference;
    private final CodecRegistry codecRegistry;
    private final WriteConcern writeConcern;
    private final boolean retryWrites;
    private final boolean retryReads;
    private final ReadConcern readConcern;
    private final Operations<TDocument> operations;
    private final UuidRepresentation uuidRepresentation;
    @Nullable
    private final AutoEncryptionSettings autoEncryptionSettings;

    private final TimeoutSettings timeoutSettings;
    private final OperationExecutor executor;

    MongoCollectionImpl(final MongoNamespace namespace, final Class<TDocument> documentClass, final CodecRegistry codecRegistry,
            final ReadPreference readPreference, final WriteConcern writeConcern, final boolean retryWrites,
            final boolean retryReads, final ReadConcern readConcern, final UuidRepresentation uuidRepresentation,
            @Nullable final AutoEncryptionSettings autoEncryptionSettings, final TimeoutSettings timeoutSettings,
            final OperationExecutor executor) {
        this.namespace = notNull("namespace", namespace);
        this.documentClass = notNull("documentClass", documentClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.retryWrites = retryWrites;
        this.retryReads = retryReads;
        this.readConcern = notNull("readConcern", readConcern);
        this.executor = notNull("executor", executor);
        this.uuidRepresentation = notNull("uuidRepresentation", uuidRepresentation);
        this.autoEncryptionSettings = autoEncryptionSettings;
        this.timeoutSettings = timeoutSettings;
        this.operations = new Operations<>(namespace, documentClass, readPreference, codecRegistry, readConcern, writeConcern,
                retryWrites, retryReads, timeoutSettings);
    }

    @Override
    public MongoNamespace getNamespace() {
        return namespace;
    }

    @Override
    public Class<TDocument> getDocumentClass() {
        return documentClass;
    }

    @Override
    public CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    @Override
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    @Override
    public ReadConcern getReadConcern() {
        return readConcern;
    }

    @Override
    @Nullable
    public Long getTimeout(final TimeUnit timeUnit) {
        Long timeoutMS = timeoutSettings.getTimeoutMS();
        return timeoutMS == null ? null : notNull("timeUnit", timeUnit).convert(timeoutMS, MILLISECONDS);
    }

    @Override
    public <NewTDocument> MongoCollection<NewTDocument> withDocumentClass(final Class<NewTDocument> clazz) {
        return new MongoCollectionImpl<>(namespace, clazz, codecRegistry, readPreference, writeConcern, retryWrites,
                retryReads, readConcern, uuidRepresentation, autoEncryptionSettings, timeoutSettings, executor);
    }

    @Override
    public MongoCollection<TDocument> withCodecRegistry(final CodecRegistry codecRegistry) {
        return new MongoCollectionImpl<>(namespace, documentClass, withUuidRepresentation(codecRegistry, uuidRepresentation),
                readPreference, writeConcern, retryWrites, retryReads, readConcern, uuidRepresentation, autoEncryptionSettings, timeoutSettings, executor);
    }

    @Override
    public MongoCollection<TDocument> withReadPreference(final ReadPreference readPreference) {
        return new MongoCollectionImpl<>(namespace, documentClass, codecRegistry, readPreference, writeConcern, retryWrites,
                retryReads, readConcern, uuidRepresentation, autoEncryptionSettings, timeoutSettings, executor);
    }

    @Override
    public MongoCollection<TDocument> withWriteConcern(final WriteConcern writeConcern) {
        return new MongoCollectionImpl<>(namespace, documentClass, codecRegistry, readPreference, writeConcern, retryWrites,
                retryReads, readConcern, uuidRepresentation, autoEncryptionSettings, timeoutSettings, executor);
    }

    @Override
    public MongoCollection<TDocument> withReadConcern(final ReadConcern readConcern) {
        return new MongoCollectionImpl<>(namespace, documentClass, codecRegistry, readPreference, writeConcern, retryWrites,
                retryReads, readConcern, uuidRepresentation, autoEncryptionSettings, timeoutSettings, executor);
    }

    @Override
    public MongoCollection<TDocument> withTimeout(final long timeout, final TimeUnit timeUnit) {
        return new MongoCollectionImpl<>(namespace, documentClass, codecRegistry, readPreference, writeConcern, retryWrites, retryReads,
                readConcern, uuidRepresentation, autoEncryptionSettings, timeoutSettings.withTimeout(timeout, timeUnit), executor);
    }

    @Override
    public long countDocuments() {
        return countDocuments(new BsonDocument());
    }

    @Override
    public long countDocuments(final Bson filter) {
        return countDocuments(filter, new CountOptions());
    }

    @Override
    public long countDocuments(final Bson filter, final CountOptions options) {
        return executeCount(null, filter, options);
    }

    @Override
    public long countDocuments(final ClientSession clientSession) {
        return countDocuments(clientSession, new BsonDocument());
    }

    @Override
    public long countDocuments(final ClientSession clientSession, final Bson filter) {
        return countDocuments(clientSession, filter, new CountOptions());
    }

    @Override
    public long countDocuments(final ClientSession clientSession, final Bson filter, final CountOptions options) {
        notNull("clientSession", clientSession);
        return executeCount(clientSession, filter, options);
    }

    @Override
    public long estimatedDocumentCount() {
        return estimatedDocumentCount(new EstimatedDocumentCountOptions());
    }

    @Override
    public long estimatedDocumentCount(final EstimatedDocumentCountOptions options) {
        return getExecutor(operations.createTimeoutSettings(options))
                .execute(operations.estimatedDocumentCount(options), readPreference, readConcern, null);
    }

    private long executeCount(@Nullable final ClientSession clientSession, final Bson filter, final CountOptions options) {
        return getExecutor(operations.createTimeoutSettings(options))
                .execute(operations.countDocuments(filter, options), readPreference, readConcern, clientSession);
    }

    @Override
    public <TResult> DistinctIterable<TResult> distinct(final String fieldName, final Class<TResult> resultClass) {
        return distinct(fieldName, new BsonDocument(), resultClass);
    }

    @Override
    public <TResult> DistinctIterable<TResult> distinct(final String fieldName, final Bson filter, final Class<TResult> resultClass) {
        return createDistinctIterable(null, fieldName, filter, resultClass);
    }

    @Override
    public <TResult> DistinctIterable<TResult> distinct(final ClientSession clientSession, final String fieldName,
                                                        final Class<TResult> resultClass) {
        return distinct(clientSession, fieldName, new BsonDocument(), resultClass);
    }

    @Override
    public <TResult> DistinctIterable<TResult> distinct(final ClientSession clientSession, final String fieldName, final Bson filter,
                                                        final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return createDistinctIterable(clientSession, fieldName, filter, resultClass);
    }

    private <TResult> DistinctIterable<TResult> createDistinctIterable(@Nullable final ClientSession clientSession, final String fieldName,
                                                                       final Bson filter, final Class<TResult> resultClass) {
        return new DistinctIterableImpl<>(clientSession, namespace, documentClass, resultClass, codecRegistry,
                readPreference, readConcern, executor, fieldName, filter, retryReads, timeoutSettings);
    }

    @Override
    public FindIterable<TDocument> find() {
        return find(new BsonDocument(), documentClass);
    }

    @Override
    public <TResult> FindIterable<TResult> find(final Class<TResult> resultClass) {
        return find(new BsonDocument(), resultClass);
    }

    @Override
    public FindIterable<TDocument> find(final Bson filter) {
        return find(filter, documentClass);
    }

    @Override
    public <TResult> FindIterable<TResult> find(final Bson filter, final Class<TResult> resultClass) {
        return createFindIterable(null, filter, resultClass);
    }

    @Override
    public FindIterable<TDocument> find(final ClientSession clientSession) {
        notNull("clientSession", clientSession);
        return find(clientSession, new BsonDocument(), documentClass);
    }

    @Override
    public <TResult> FindIterable<TResult> find(final ClientSession clientSession, final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return find(clientSession, new BsonDocument(), resultClass);
    }

    @Override
    public FindIterable<TDocument> find(final ClientSession clientSession, final Bson filter) {
        notNull("clientSession", clientSession);
        return find(clientSession, filter, documentClass);
    }

    @Override
    public <TResult> FindIterable<TResult> find(final ClientSession clientSession, final Bson filter,
                                                final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return createFindIterable(clientSession, filter, resultClass);
    }

    private <TResult> FindIterable<TResult> createFindIterable(@Nullable final ClientSession clientSession, final Bson filter,
                                                               final Class<TResult> resultClass) {
        return new FindIterableImpl<>(clientSession, namespace, this.documentClass, resultClass, codecRegistry,
                readPreference, readConcern, executor, filter, retryReads, timeoutSettings);
    }

    @Override
    public AggregateIterable<TDocument> aggregate(final List<? extends Bson> pipeline) {
        return aggregate(pipeline, documentClass);
    }

    @Override
    public <TResult> AggregateIterable<TResult> aggregate(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return createAggregateIterable(null, pipeline, resultClass);
    }

    @Override
    public AggregateIterable<TDocument> aggregate(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return aggregate(clientSession, pipeline, documentClass);
    }

    @Override
    public <TResult> AggregateIterable<TResult> aggregate(final ClientSession clientSession, final List<? extends Bson> pipeline,
                                                          final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return createAggregateIterable(clientSession, pipeline, resultClass);
    }

    private <TResult> AggregateIterable<TResult> createAggregateIterable(@Nullable final ClientSession clientSession,
                                                                         final List<? extends Bson> pipeline,
                                                                         final Class<TResult> resultClass) {
        return new AggregateIterableImpl<>(clientSession, namespace, documentClass, resultClass, codecRegistry,
                readPreference, readConcern, writeConcern, executor, pipeline, AggregationLevel.COLLECTION, retryReads, timeoutSettings);
    }

    @Override
    public ChangeStreamIterable<TDocument> watch() {
        return watch(Collections.emptyList());
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final Class<TResult> resultClass) {
        return watch(Collections.emptyList(), resultClass);
    }

    @Override
    public ChangeStreamIterable<TDocument> watch(final List<? extends Bson> pipeline) {
        return watch(pipeline, documentClass);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return createChangeStreamIterable(null, pipeline, resultClass);
    }

    @Override
    public ChangeStreamIterable<TDocument> watch(final ClientSession clientSession) {
        return watch(clientSession, Collections.emptyList(), documentClass);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final ClientSession clientSession, final Class<TResult> resultClass) {
        return watch(clientSession, Collections.emptyList(), resultClass);
    }

    @Override
    public ChangeStreamIterable<TDocument> watch(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return watch(clientSession, pipeline, documentClass);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final ClientSession clientSession, final List<? extends Bson> pipeline,
                                                         final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return createChangeStreamIterable(clientSession, pipeline, resultClass);
    }

    private <TResult> ChangeStreamIterable<TResult> createChangeStreamIterable(@Nullable final ClientSession clientSession,
                                                                               final List<? extends Bson> pipeline,
                                                                               final Class<TResult> resultClass) {
        return new ChangeStreamIterableImpl<>(clientSession, namespace, codecRegistry, readPreference, readConcern, executor,
                pipeline, resultClass, ChangeStreamLevel.COLLECTION, retryReads, timeoutSettings);
    }

    @SuppressWarnings("deprecation")
    @Override
    public com.mongodb.client.MapReduceIterable<TDocument> mapReduce(final String mapFunction, final String reduceFunction) {
        return mapReduce(mapFunction, reduceFunction, documentClass);
    }

    @SuppressWarnings("deprecation")
    @Override
    public <TResult> com.mongodb.client.MapReduceIterable<TResult> mapReduce(final String mapFunction, final String reduceFunction,
                                                          final Class<TResult> resultClass) {
        return createMapReduceIterable(null, mapFunction, reduceFunction, resultClass);
    }

    @SuppressWarnings("deprecation")
    @Override
    public com.mongodb.client.MapReduceIterable<TDocument> mapReduce(final ClientSession clientSession, final String mapFunction,
                                                  final String reduceFunction) {
        return mapReduce(clientSession, mapFunction, reduceFunction, documentClass);
    }

    @SuppressWarnings("deprecation")
    @Override
    public <TResult> com.mongodb.client.MapReduceIterable<TResult> mapReduce(final ClientSession clientSession, final String mapFunction,
                                                          final String reduceFunction, final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return createMapReduceIterable(clientSession, mapFunction, reduceFunction, resultClass);
    }

    @SuppressWarnings("deprecation")
    private <TResult> com.mongodb.client.MapReduceIterable<TResult> createMapReduceIterable(@Nullable final ClientSession clientSession,
                                                                         final String mapFunction, final String reduceFunction,
                                                                         final Class<TResult> resultClass) {
        return new MapReduceIterableImpl<>(clientSession, namespace, documentClass, resultClass, codecRegistry,
                readPreference, readConcern, writeConcern, executor, mapFunction, reduceFunction, timeoutSettings);
    }

    @Override
    public BulkWriteResult bulkWrite(final List<? extends WriteModel<? extends TDocument>> requests) {
        return bulkWrite(requests, new BulkWriteOptions());
    }

    @Override
    public BulkWriteResult bulkWrite(final List<? extends WriteModel<? extends TDocument>> requests, final BulkWriteOptions options) {
        return executeBulkWrite(null, requests, options);
    }

    @Override
    public BulkWriteResult bulkWrite(final ClientSession clientSession, final List<? extends WriteModel<? extends TDocument>> requests) {
        return bulkWrite(clientSession, requests, new BulkWriteOptions());
    }

    @Override
    public BulkWriteResult bulkWrite(final ClientSession clientSession, final List<? extends WriteModel<? extends TDocument>> requests,
                                     final BulkWriteOptions options) {
        notNull("clientSession", clientSession);
        return executeBulkWrite(clientSession, requests, options);
    }

    private BulkWriteResult executeBulkWrite(@Nullable final ClientSession clientSession,
                                             final List<? extends WriteModel<? extends TDocument>> requests,
                                             final BulkWriteOptions options) {
        notNull("requests", requests);
        return getExecutor(timeoutSettings)
                .execute(operations.bulkWrite(requests, options), readConcern, clientSession);
    }

    @Override
    public InsertOneResult insertOne(final TDocument document) {
        return insertOne(document, new InsertOneOptions());
    }

    @Override
    public InsertOneResult insertOne(final TDocument document, final InsertOneOptions options) {
        notNull("document", document);
        return executeInsertOne(null, document, options);
    }

    @Override
    public InsertOneResult insertOne(final ClientSession clientSession, final TDocument document) {
        return insertOne(clientSession, document, new InsertOneOptions());
    }

    @Override
    public InsertOneResult insertOne(final ClientSession clientSession, final TDocument document, final InsertOneOptions options) {
        notNull("clientSession", clientSession);
        notNull("document", document);
        return executeInsertOne(clientSession, document, options);
    }

    private InsertOneResult executeInsertOne(@Nullable final ClientSession clientSession, final TDocument document,
                                             final InsertOneOptions options) {
        return toInsertOneResult(executeSingleWriteRequest(clientSession, operations.insertOne(document, options), INSERT));
    }

    @Override
    public InsertManyResult insertMany(final List<? extends TDocument> documents) {
        return insertMany(documents, new InsertManyOptions());
    }

    @Override
    public InsertManyResult insertMany(final List<? extends TDocument> documents, final InsertManyOptions options) {
        return executeInsertMany(null, documents, options);
    }

    @Override
    public InsertManyResult insertMany(final ClientSession clientSession, final List<? extends TDocument> documents) {
        return insertMany(clientSession, documents, new InsertManyOptions());
    }


    @Override
    public InsertManyResult insertMany(final ClientSession clientSession, final List<? extends TDocument> documents,
                                       final InsertManyOptions options) {
        notNull("clientSession", clientSession);
        return executeInsertMany(clientSession, documents, options);
    }

    private InsertManyResult executeInsertMany(@Nullable final ClientSession clientSession, final List<? extends TDocument> documents,
                                                final InsertManyOptions options) {
        return toInsertManyResult(
                getExecutor(timeoutSettings).execute(operations.insertMany(documents, options), readConcern, clientSession)
        );
    }

    @Override
    public DeleteResult deleteOne(final Bson filter) {
        return deleteOne(filter, new DeleteOptions());
    }

    @Override
    public DeleteResult deleteOne(final Bson filter, final DeleteOptions options) {
        return executeDelete(null, filter, options, false);
    }

    @Override
    public DeleteResult deleteOne(final ClientSession clientSession, final Bson filter) {
        return deleteOne(clientSession, filter, new DeleteOptions());
    }

    @Override
    public DeleteResult deleteOne(final ClientSession clientSession, final Bson filter, final DeleteOptions options) {
        notNull("clientSession", clientSession);
        return executeDelete(clientSession, filter, options, false);
    }

    @Override
    public DeleteResult deleteMany(final Bson filter) {
        return deleteMany(filter, new DeleteOptions());
    }

    @Override
    public DeleteResult deleteMany(final Bson filter, final DeleteOptions options) {
        return executeDelete(null, filter, options, true);
    }

    @Override
    public DeleteResult deleteMany(final ClientSession clientSession, final Bson filter) {
        return deleteMany(clientSession, filter, new DeleteOptions());
    }

    @Override
    public DeleteResult deleteMany(final ClientSession clientSession, final Bson filter, final DeleteOptions options) {
        notNull("clientSession", clientSession);
        return executeDelete(clientSession, filter, options, true);
    }

    @Override
    public UpdateResult replaceOne(final Bson filter, final TDocument replacement) {
        return replaceOne(filter, replacement, new ReplaceOptions());
    }

    @Override
    public UpdateResult replaceOne(final Bson filter, final TDocument replacement, final ReplaceOptions replaceOptions) {
        return executeReplaceOne(null, filter, replacement, replaceOptions);
    }

    @Override
    public UpdateResult replaceOne(final ClientSession clientSession, final Bson filter, final TDocument replacement) {
        return replaceOne(clientSession, filter, replacement, new ReplaceOptions());
    }

    @Override
    public UpdateResult replaceOne(final ClientSession clientSession, final Bson filter, final TDocument replacement,
                                   final ReplaceOptions replaceOptions) {
        notNull("clientSession", clientSession);
        return executeReplaceOne(clientSession, filter, replacement, replaceOptions);
    }

    private UpdateResult executeReplaceOne(@Nullable final ClientSession clientSession, final Bson filter, final TDocument replacement,
                                           final ReplaceOptions replaceOptions) {
        return toUpdateResult(executeSingleWriteRequest(clientSession, operations.replaceOne(filter, replacement, replaceOptions),
                REPLACE));
    }

    @Override
    public UpdateResult updateOne(final Bson filter, final Bson update) {
        return updateOne(filter, update, new UpdateOptions());
    }

    @Override
    public UpdateResult updateOne(final Bson filter, final Bson update, final UpdateOptions updateOptions) {
        return executeUpdate(null, filter, update, updateOptions, false);
    }

    @Override
    public UpdateResult updateOne(final ClientSession clientSession, final Bson filter, final Bson update) {
        return updateOne(clientSession, filter, update, new UpdateOptions());
    }

    @Override
    public UpdateResult updateOne(final ClientSession clientSession, final Bson filter, final Bson update,
                                  final UpdateOptions updateOptions) {
        notNull("clientSession", clientSession);
        return executeUpdate(clientSession, filter, update, updateOptions, false);

    }

    @Override
    public UpdateResult updateOne(final Bson filter, final List<? extends Bson> update) {
        return updateOne(filter, update, new UpdateOptions());
    }

    @Override
    public UpdateResult updateOne(final Bson filter, final List<? extends Bson> update, final UpdateOptions updateOptions) {
        return executeUpdate(null, filter, update, updateOptions, false);
    }

    @Override
    public UpdateResult updateOne(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update) {
        return updateOne(clientSession, filter, update, new UpdateOptions());
    }

    @Override
    public UpdateResult updateOne(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update,
                                  final UpdateOptions updateOptions) {
        notNull("clientSession", clientSession);
        return executeUpdate(clientSession, filter, update, updateOptions, false);

    }

    @Override
    public UpdateResult updateMany(final Bson filter, final Bson update) {
        return updateMany(filter, update, new UpdateOptions());
    }

    @Override
    public UpdateResult updateMany(final Bson filter, final Bson update, final UpdateOptions updateOptions) {
        return executeUpdate(null, filter, update, updateOptions, true);
    }

    @Override
    public UpdateResult updateMany(final ClientSession clientSession, final Bson filter, final Bson update) {
        return updateMany(clientSession, filter, update, new UpdateOptions());
    }

    @Override
    public UpdateResult updateMany(final ClientSession clientSession, final Bson filter, final Bson update,
                                   final UpdateOptions updateOptions) {
        notNull("clientSession", clientSession);
        return executeUpdate(clientSession, filter, update, updateOptions, true);
    }

    @Override
    public UpdateResult updateMany(final Bson filter, final List<? extends Bson> update) {
        return updateMany(filter, update, new UpdateOptions());
    }

    @Override
    public UpdateResult updateMany(final Bson filter, final List<? extends Bson> update, final UpdateOptions updateOptions) {
        return executeUpdate(null, filter, update, updateOptions, true);
    }

    @Override
    public UpdateResult updateMany(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update) {
        return updateMany(clientSession, filter, update, new UpdateOptions());
    }

    @Override
    public UpdateResult updateMany(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update,
                                   final UpdateOptions updateOptions) {
        notNull("clientSession", clientSession);
        return executeUpdate(clientSession, filter, update, updateOptions, true);
    }

    @Override
    @Nullable
    public TDocument findOneAndDelete(final Bson filter) {
        return findOneAndDelete(filter, new FindOneAndDeleteOptions());
    }

    @Override
    @Nullable
    public TDocument findOneAndDelete(final Bson filter, final FindOneAndDeleteOptions options) {
        return executeFindOneAndDelete(null, filter, options);
    }

    @Override
    @Nullable
    public TDocument findOneAndDelete(final ClientSession clientSession, final Bson filter) {
        return findOneAndDelete(clientSession, filter, new FindOneAndDeleteOptions());
    }

    @Override
    @Nullable
    public TDocument findOneAndDelete(final ClientSession clientSession, final Bson filter, final FindOneAndDeleteOptions options) {
        notNull("clientSession", clientSession);
        return executeFindOneAndDelete(clientSession, filter, options);
    }

    @Nullable
    private TDocument executeFindOneAndDelete(@Nullable final ClientSession clientSession, final Bson filter,
                                              final FindOneAndDeleteOptions options) {
        return getExecutor(operations.createTimeoutSettings(options))
                .execute(operations.findOneAndDelete(filter, options), readConcern, clientSession);
    }

    @Override
    @Nullable
    public TDocument findOneAndReplace(final Bson filter, final TDocument replacement) {
        return findOneAndReplace(filter, replacement, new FindOneAndReplaceOptions());
    }

    @Override
    @Nullable
    public TDocument findOneAndReplace(final Bson filter, final TDocument replacement, final FindOneAndReplaceOptions options) {
        return executeFindOneAndReplace(null, filter, replacement, options);
    }

    @Override
    @Nullable
    public TDocument findOneAndReplace(final ClientSession clientSession, final Bson filter, final TDocument replacement) {
        return findOneAndReplace(clientSession, filter, replacement, new FindOneAndReplaceOptions());
    }

    @Override
    @Nullable
    public TDocument findOneAndReplace(final ClientSession clientSession, final Bson filter, final TDocument replacement,
                                       final FindOneAndReplaceOptions options) {
        notNull("clientSession", clientSession);
        return executeFindOneAndReplace(clientSession, filter, replacement, options);
    }

    @Nullable
    private TDocument executeFindOneAndReplace(@Nullable final ClientSession clientSession, final Bson filter, final TDocument replacement,
                                               final FindOneAndReplaceOptions options) {
        return getExecutor(operations.createTimeoutSettings(options))
                .execute(operations.findOneAndReplace(filter, replacement, options), readConcern, clientSession);
    }

    @Override
    @Nullable
    public TDocument findOneAndUpdate(final Bson filter, final Bson update) {
        return findOneAndUpdate(filter, update, new FindOneAndUpdateOptions());
    }

    @Override
    @Nullable
    public TDocument findOneAndUpdate(final Bson filter, final Bson update, final FindOneAndUpdateOptions options) {
        return executeFindOneAndUpdate(null, filter, update, options);
    }

    @Override
    @Nullable
    public TDocument findOneAndUpdate(final ClientSession clientSession, final Bson filter, final Bson update) {
        return findOneAndUpdate(clientSession, filter, update, new FindOneAndUpdateOptions());
    }

    @Override
    @Nullable
    public TDocument findOneAndUpdate(final ClientSession clientSession, final Bson filter, final Bson update,
                                      final FindOneAndUpdateOptions options) {
        notNull("clientSession", clientSession);
        return executeFindOneAndUpdate(clientSession, filter, update, options);
    }

    @Nullable
    private TDocument executeFindOneAndUpdate(@Nullable final ClientSession clientSession, final Bson filter, final Bson update,
                                              final FindOneAndUpdateOptions options) {
        return getExecutor(operations.createTimeoutSettings(options))
                .execute(operations.findOneAndUpdate(filter, update, options), readConcern, clientSession);
    }

    @Override
    @Nullable
    public TDocument findOneAndUpdate(final Bson filter, final List<? extends Bson> update) {
        return findOneAndUpdate(filter, update, new FindOneAndUpdateOptions());
    }

    @Override
    @Nullable
    public TDocument findOneAndUpdate(final Bson filter, final List<? extends Bson> update, final FindOneAndUpdateOptions options) {
        return executeFindOneAndUpdate(null, filter, update, options);
    }

    @Override
    @Nullable
    public TDocument findOneAndUpdate(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update) {
        return findOneAndUpdate(clientSession, filter, update, new FindOneAndUpdateOptions());
    }

    @Override
    @Nullable
    public TDocument findOneAndUpdate(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update,
                                      final FindOneAndUpdateOptions options) {
        notNull("clientSession", clientSession);
        return executeFindOneAndUpdate(clientSession, filter, update, options);
    }

    @Nullable
    private TDocument executeFindOneAndUpdate(@Nullable final ClientSession clientSession, final Bson filter,
                                              final List<? extends Bson> update, final FindOneAndUpdateOptions options) {
        return getExecutor(operations.createTimeoutSettings(options))
                .execute(operations.findOneAndUpdate(filter, update, options), readConcern, clientSession);
    }

    @Override
    public void drop() {
        executeDrop(null, new DropCollectionOptions());
    }

    @Override
    public void drop(final ClientSession clientSession) {
        notNull("clientSession", clientSession);
        executeDrop(clientSession, new DropCollectionOptions());
    }

    @Override
    public void drop(final DropCollectionOptions dropCollectionOptions) {
        executeDrop(null, dropCollectionOptions);
    }

    @Override
    public void drop(final ClientSession clientSession, final DropCollectionOptions dropCollectionOptions) {
        executeDrop(clientSession, dropCollectionOptions);
    }

    @Override
    public String createSearchIndex(final String indexName, final Bson definition) {
        notNull("indexName", indexName);
        notNull("definition", definition);

        return executeCreateSearchIndexes(singletonList(new SearchIndexModel(indexName, definition))).get(0);
    }

    @Override
    public String createSearchIndex(final Bson definition) {
        notNull("definition", definition);

        return executeCreateSearchIndexes(singletonList(new SearchIndexModel(definition))).get(0);
    }

    @Override
    public List<String> createSearchIndexes(final List<SearchIndexModel> searchIndexModels) {
        notNullElements("searchIndexModels", searchIndexModels);

        return executeCreateSearchIndexes(searchIndexModels);
    }

    @Override
    public void updateSearchIndex(final String indexName, final Bson definition) {
        notNull("indexName", indexName);
        notNull("definition", definition);

        getExecutor(timeoutSettings).execute(operations.updateSearchIndex(indexName, definition), readConcern, null);
    }

    @Override
    public void dropSearchIndex(final String indexName) {
        notNull("indexName", indexName);

        getExecutor(timeoutSettings).execute(operations.dropSearchIndex(indexName), readConcern, null);
    }

    @Override
    public ListSearchIndexesIterable<Document> listSearchIndexes() {
        return createListSearchIndexesIterable(Document.class);
    }

    @Override
    public <TResult> ListSearchIndexesIterable<TResult> listSearchIndexes(final Class<TResult> resultClass) {
        notNull("resultClass", resultClass);
        return createListSearchIndexesIterable(resultClass);
    }

    private void executeDrop(@Nullable final ClientSession clientSession, final DropCollectionOptions dropCollectionOptions) {
        getExecutor(timeoutSettings)
                .execute(operations.dropCollection(dropCollectionOptions, autoEncryptionSettings), readConcern, clientSession);
    }

    @Override
    public String createIndex(final Bson keys) {
        return createIndex(keys, new IndexOptions());
    }

    @Override
    public String createIndex(final Bson keys, final IndexOptions indexOptions) {
        return createIndexes(singletonList(new IndexModel(keys, indexOptions))).get(0);
    }

    @Override
    public String createIndex(final ClientSession clientSession, final Bson keys) {
        return createIndex(clientSession, keys, new IndexOptions());
    }

    @Override
    public String createIndex(final ClientSession clientSession, final Bson keys, final IndexOptions indexOptions) {
        return createIndexes(clientSession, singletonList(new IndexModel(keys, indexOptions))).get(0);
    }

    @Override
    public List<String> createIndexes(final List<IndexModel> indexes) {
        return createIndexes(indexes, new CreateIndexOptions());
    }

    @Override
    public List<String> createIndexes(final List<IndexModel> indexes, final CreateIndexOptions createIndexOptions) {
        return executeCreateIndexes(null, indexes, createIndexOptions);
    }

    @Override
    public List<String> createIndexes(final ClientSession clientSession, final List<IndexModel> indexes) {
        return createIndexes(clientSession, indexes, new CreateIndexOptions());
    }

    @Override
    public List<String> createIndexes(final ClientSession clientSession, final List<IndexModel> indexes,
                                      final CreateIndexOptions createIndexOptions) {
        notNull("clientSession", clientSession);
        return executeCreateIndexes(clientSession, indexes, createIndexOptions);
    }

    private List<String> executeCreateIndexes(@Nullable final ClientSession clientSession, final List<IndexModel> indexes,
                                              final CreateIndexOptions createIndexOptions) {
        getExecutor(operations.createTimeoutSettings(createIndexOptions))
                .execute(operations.createIndexes(indexes, createIndexOptions), readConcern, clientSession);
        return IndexHelper.getIndexNames(indexes, codecRegistry);
    }

    private List<String> executeCreateSearchIndexes(final List<SearchIndexModel> searchIndexModels) {
        getExecutor(timeoutSettings).execute(operations.createSearchIndexes(searchIndexModels), readConcern, null);
        return IndexHelper.getSearchIndexNames(searchIndexModels);
    }

    @Override
    public ListIndexesIterable<Document> listIndexes() {
        return listIndexes(Document.class);
    }

    @Override
    public <TResult> ListIndexesIterable<TResult> listIndexes(final Class<TResult> resultClass) {
        return createListIndexesIterable(null, resultClass);
    }

    @Override
    public ListIndexesIterable<Document> listIndexes(final ClientSession clientSession) {
        return listIndexes(clientSession, Document.class);
    }

    @Override
    public <TResult> ListIndexesIterable<TResult> listIndexes(final ClientSession clientSession, final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return createListIndexesIterable(clientSession, resultClass);
    }

    private <TResult> ListIndexesIterable<TResult> createListIndexesIterable(@Nullable final ClientSession clientSession,
                                                                             final Class<TResult> resultClass) {
        return new ListIndexesIterableImpl<>(clientSession, getNamespace(), resultClass, codecRegistry, ReadPreference.primary(),
                executor, retryReads, timeoutSettings);
    }

    private <TResult> ListSearchIndexesIterable<TResult> createListSearchIndexesIterable(final Class<TResult> resultClass) {
        return new ListSearchIndexesIterableImpl<>(getNamespace(), executor, resultClass, codecRegistry, readPreference,
                retryReads, timeoutSettings);
    }

    @Override
    public void dropIndex(final String indexName) {
        dropIndex(indexName, new DropIndexOptions());
    }

    @Override
    public void dropIndex(final String indexName, final DropIndexOptions dropIndexOptions) {
        executeDropIndex(null, indexName, dropIndexOptions);
    }

    @Override
    public void dropIndex(final Bson keys) {
        dropIndex(keys, new DropIndexOptions());
    }

    @Override
    public void dropIndex(final Bson keys, final DropIndexOptions dropIndexOptions) {
        executeDropIndex(null, keys, dropIndexOptions);
    }

    @Override
    public void dropIndex(final ClientSession clientSession, final String indexName) {
        dropIndex(clientSession, indexName, new DropIndexOptions());
    }

    @Override
    public void dropIndex(final ClientSession clientSession, final Bson keys) {
        dropIndex(clientSession, keys, new DropIndexOptions());
    }

    @Override
    public void dropIndex(final ClientSession clientSession, final String indexName, final DropIndexOptions dropIndexOptions) {
        notNull("clientSession", clientSession);
        executeDropIndex(clientSession, indexName, dropIndexOptions);
    }

    @Override
    public void dropIndex(final ClientSession clientSession, final Bson keys, final DropIndexOptions dropIndexOptions) {
        notNull("clientSession", clientSession);
        executeDropIndex(clientSession, keys, dropIndexOptions);
    }

    @Override
    public void dropIndexes() {
        dropIndex("*");
    }

    @Override
    public void dropIndexes(final ClientSession clientSession) {
        notNull("clientSession", clientSession);
        executeDropIndex(clientSession, "*", new DropIndexOptions());
    }

    @Override
    public void dropIndexes(final DropIndexOptions dropIndexOptions) {
        dropIndex("*", dropIndexOptions);
    }

    @Override
    public void dropIndexes(final ClientSession clientSession, final DropIndexOptions dropIndexOptions) {
        dropIndex(clientSession, "*", dropIndexOptions);
    }

    private void executeDropIndex(@Nullable final ClientSession clientSession, final String indexName,
                                  final DropIndexOptions options) {
        notNull("options", options);
        getExecutor(operations.createTimeoutSettings(options))
                .execute(operations.dropIndex(indexName, options), readConcern, clientSession);
    }

    private void executeDropIndex(@Nullable final ClientSession clientSession, final Bson keys, final DropIndexOptions options) {
        notNull("options", options);
        getExecutor(operations.createTimeoutSettings(options))
                .execute(operations.dropIndex(keys, options), readConcern, clientSession);
    }

    @Override
    public void renameCollection(final MongoNamespace newCollectionNamespace) {
        renameCollection(newCollectionNamespace, new RenameCollectionOptions());
    }

    @Override
    public void renameCollection(final MongoNamespace newCollectionNamespace, final RenameCollectionOptions renameCollectionOptions) {
        executeRenameCollection(null, newCollectionNamespace, renameCollectionOptions);
    }

    @Override
    public void renameCollection(final ClientSession clientSession, final MongoNamespace newCollectionNamespace) {
        renameCollection(clientSession, newCollectionNamespace, new RenameCollectionOptions());
    }

    @Override
    public void renameCollection(final ClientSession clientSession, final MongoNamespace newCollectionNamespace,
                                 final RenameCollectionOptions renameCollectionOptions) {
        notNull("clientSession", clientSession);
        executeRenameCollection(clientSession, newCollectionNamespace, renameCollectionOptions);
    }

    private void executeRenameCollection(@Nullable final ClientSession clientSession, final MongoNamespace newCollectionNamespace,
                                         final RenameCollectionOptions renameCollectionOptions) {
        getExecutor(timeoutSettings)
                .execute(operations.renameCollection(newCollectionNamespace, renameCollectionOptions), readConcern, clientSession);
    }

    private DeleteResult executeDelete(@Nullable final ClientSession clientSession, final Bson filter, final DeleteOptions deleteOptions,
                                       final boolean multi) {
        com.mongodb.bulk.BulkWriteResult result = executeSingleWriteRequest(clientSession,
                multi ? operations.deleteMany(filter, deleteOptions) : operations.deleteOne(filter, deleteOptions), DELETE);
        if (result.wasAcknowledged()) {
            return DeleteResult.acknowledged(result.getDeletedCount());
        } else {
            return DeleteResult.unacknowledged();
        }
    }

    private UpdateResult executeUpdate(@Nullable final ClientSession clientSession, final Bson filter, final Bson update,
                                       final UpdateOptions updateOptions, final boolean multi) {
        return toUpdateResult(executeSingleWriteRequest(clientSession,
                multi ? operations.updateMany(filter, update, updateOptions) : operations.updateOne(filter, update, updateOptions),
                UPDATE));
    }

    private UpdateResult executeUpdate(@Nullable final ClientSession clientSession, final Bson filter,
                                       final List<? extends Bson> update, final UpdateOptions updateOptions, final boolean multi) {
        return toUpdateResult(executeSingleWriteRequest(clientSession,
                multi ? operations.updateMany(filter, update, updateOptions) : operations.updateOne(filter, update, updateOptions),
                UPDATE));
    }

    private BulkWriteResult executeSingleWriteRequest(@Nullable final ClientSession clientSession,
                                                      final WriteOperation<BulkWriteResult> writeOperation,
                                                      final WriteRequest.Type type) {
        try {
            return getExecutor(timeoutSettings)
                    .execute(writeOperation, readConcern, clientSession);
        } catch (MongoBulkWriteException e) {
            if (e.getWriteErrors().isEmpty()) {
                throw new MongoWriteConcernException(e.getWriteConcernError(),
                        translateBulkWriteResult(type, e.getWriteResult()),
                        e.getServerAddress(), e.getErrorLabels());
            } else {
                throw new MongoWriteException(new WriteError(e.getWriteErrors().get(0)), e.getServerAddress(), e.getErrorLabels());
            }

        }
    }

    private WriteConcernResult translateBulkWriteResult(final WriteRequest.Type type, final BulkWriteResult writeResult) {
        switch (type) {
            case INSERT:
                return WriteConcernResult.acknowledged(writeResult.getInsertedCount(), false, null);
            case DELETE:
                return WriteConcernResult.acknowledged(writeResult.getDeletedCount(), false, null);
            case UPDATE:
            case REPLACE:
                return WriteConcernResult.acknowledged(writeResult.getMatchedCount() + writeResult.getUpserts().size(),
                        writeResult.getMatchedCount() > 0,
                        writeResult.getUpserts().isEmpty()
                                ? null : writeResult.getUpserts().get(0).getId());
            default:
                throw new MongoInternalException("Unhandled write request type: " + type);
        }
    }

    private InsertOneResult toInsertOneResult(final com.mongodb.bulk.BulkWriteResult result) {
        if (result.wasAcknowledged()) {
            BsonValue insertedId = result.getInserts().isEmpty() ? null : result.getInserts().get(0).getId();
            return InsertOneResult.acknowledged(insertedId);
        } else {
            return InsertOneResult.unacknowledged();
        }
    }

    private InsertManyResult toInsertManyResult(final com.mongodb.bulk.BulkWriteResult result) {
        if (result.wasAcknowledged()) {
            return InsertManyResult.acknowledged(result.getInserts().stream()
                    .collect(HashMap::new, (m, v) -> m.put(v.getIndex(), v.getId()), HashMap::putAll));
        } else {
            return InsertManyResult.unacknowledged();
        }
    }

    private UpdateResult toUpdateResult(final com.mongodb.bulk.BulkWriteResult result) {
        if (result.wasAcknowledged()) {
            BsonValue upsertedId = result.getUpserts().isEmpty() ? null : result.getUpserts().get(0).getId();
            return UpdateResult.acknowledged(result.getMatchedCount(), (long) result.getModifiedCount(), upsertedId);
        } else {
            return UpdateResult.unacknowledged();
        }
    }

    private OperationExecutor getExecutor(final TimeoutSettings timeoutSettings) {
        return executor.withTimeoutSettings(timeoutSettings);
    }

}
