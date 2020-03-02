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

package com.mongodb.internal.async.client;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.MongoWriteException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernResult;
import com.mongodb.WriteError;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.DeleteOptions;
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
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.internal.bulk.WriteRequest;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.client.model.CountStrategy;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.internal.operation.AsyncOperations;
import com.mongodb.internal.operation.AsyncWriteOperation;
import com.mongodb.internal.operation.IndexHelper;
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

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.bulk.WriteRequest.Type.DELETE;
import static com.mongodb.internal.bulk.WriteRequest.Type.INSERT;
import static com.mongodb.internal.bulk.WriteRequest.Type.REPLACE;
import static com.mongodb.internal.bulk.WriteRequest.Type.UPDATE;
import static com.mongodb.internal.client.model.CountOptionsHelper.fromEstimatedDocumentCountOptions;
import static java.util.Collections.singletonList;
import static org.bson.internal.CodecRegistryHelper.createRegistry;

class AsyncMongoCollectionImpl<TDocument> implements AsyncMongoCollection<TDocument> {
    private final MongoNamespace namespace;
    private final Class<TDocument> documentClass;
    private final ReadPreference readPreference;
    private final CodecRegistry codecRegistry;
    private final WriteConcern writeConcern;
    private final boolean retryWrites;
    private final boolean retryReads;
    private final ReadConcern readConcern;
    private final UuidRepresentation uuidRepresentation;
    private final OperationExecutor executor;
    private final AsyncOperations<TDocument> operations;

    AsyncMongoCollectionImpl(final MongoNamespace namespace, final Class<TDocument> documentClass, final CodecRegistry codecRegistry,
                             final ReadPreference readPreference, final WriteConcern writeConcern, final boolean retryWrites,
                        final boolean retryReads, final ReadConcern readConcern, final UuidRepresentation uuidRepresentation,
                        final OperationExecutor executor) {
        this.namespace = notNull("namespace", namespace);
        this.documentClass = notNull("documentClass", documentClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.retryWrites = retryWrites;
        this.retryReads = retryReads;
        this.readConcern = notNull("readConcern", readConcern);
        this.uuidRepresentation = notNull("uuidRepresentation", uuidRepresentation);
        this.executor = notNull("executor", executor);
        this.operations = new AsyncOperations<>(namespace, documentClass, readPreference, codecRegistry, readConcern, writeConcern,
                retryWrites, retryReads);
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
    public <NewTDocument> AsyncMongoCollection<NewTDocument> withDocumentClass(final Class<NewTDocument> newDocumentClass) {
        return new AsyncMongoCollectionImpl<NewTDocument>(namespace, newDocumentClass, codecRegistry, readPreference, writeConcern,
                retryWrites, retryReads, readConcern, uuidRepresentation, executor);
    }

    @Override
    public AsyncMongoCollection<TDocument> withCodecRegistry(final CodecRegistry codecRegistry) {
        return new AsyncMongoCollectionImpl<TDocument>(namespace, documentClass, createRegistry(codecRegistry, uuidRepresentation),
                readPreference, writeConcern, retryWrites, retryReads, readConcern, uuidRepresentation, executor);
    }

    @Override
    public AsyncMongoCollection<TDocument> withReadPreference(final ReadPreference readPreference) {
        return new AsyncMongoCollectionImpl<TDocument>(namespace, documentClass, codecRegistry, readPreference, writeConcern, retryWrites,
                retryReads, readConcern, uuidRepresentation, executor);
    }

    @Override
    public AsyncMongoCollection<TDocument> withWriteConcern(final WriteConcern writeConcern) {
        return new AsyncMongoCollectionImpl<TDocument>(namespace, documentClass, codecRegistry, readPreference, writeConcern, retryWrites,
                retryReads, readConcern, uuidRepresentation, executor);
    }

    @Override
    public AsyncMongoCollection<TDocument> withReadConcern(final ReadConcern readConcern) {
        return new AsyncMongoCollectionImpl<TDocument>(namespace, documentClass, codecRegistry, readPreference, writeConcern, retryWrites,
                retryReads, readConcern, uuidRepresentation, executor);
    }

    @Override
    public void countDocuments(final SingleResultCallback<Long> callback) {
        countDocuments(new BsonDocument(), callback);
    }

    @Override
    public void countDocuments(final Bson filter, final SingleResultCallback<Long> callback) {
        countDocuments(filter, new CountOptions(), callback);
    }

    @Override
    public void countDocuments(final Bson filter, final CountOptions options, final SingleResultCallback<Long> callback) {
        executeCount(null, filter, options, CountStrategy.AGGREGATE, callback);
    }

    @Override
    public void countDocuments(final AsyncClientSession clientSession, final SingleResultCallback<Long> callback) {
        countDocuments(clientSession, new BsonDocument(), callback);
    }

    @Override
    public void countDocuments(final AsyncClientSession clientSession, final Bson filter, final SingleResultCallback<Long> callback) {
        countDocuments(clientSession, filter, new CountOptions(), callback);
    }

    @Override
    public void countDocuments(final AsyncClientSession clientSession, final Bson filter, final CountOptions options,
                               final SingleResultCallback<Long> callback) {
        notNull("clientSession", clientSession);
        executeCount(clientSession, filter, options, CountStrategy.AGGREGATE, callback);
    }

    @Override
    public void estimatedDocumentCount(final SingleResultCallback<Long> callback) {
        estimatedDocumentCount(new EstimatedDocumentCountOptions(), callback);
    }

    @Override
    public void estimatedDocumentCount(final EstimatedDocumentCountOptions options, final SingleResultCallback<Long> callback) {
        executeCount(null, new BsonDocument(), fromEstimatedDocumentCountOptions(options), CountStrategy.COMMAND, callback);
    }

    private void executeCount(@Nullable final AsyncClientSession clientSession, final Bson filter, final CountOptions options,
                              final CountStrategy countStrategy, final SingleResultCallback<Long> callback) {
        executor.execute(operations.count(filter, options, countStrategy), readPreference, readConcern, clientSession, callback);
    }

    @Override
    public <TResult> AsyncDistinctIterable<TResult> distinct(final String fieldName, final Class<TResult> resultClass) {
        return distinct(fieldName, new BsonDocument(), resultClass);
    }

    @Override
    public <TResult> AsyncDistinctIterable<TResult> distinct(final String fieldName, final Bson filter, final Class<TResult> resultClass) {
        return createDistinctIterable(null, fieldName, filter, resultClass);
    }

    @Override
    public <TResult> AsyncDistinctIterable<TResult> distinct(final AsyncClientSession clientSession, final String fieldName,
                                                        final Class<TResult> resultClass) {
        return distinct(clientSession, fieldName, new BsonDocument(), resultClass);
    }

    @Override
    public <TResult> AsyncDistinctIterable<TResult> distinct(final AsyncClientSession clientSession, final String fieldName,
                                                             final Bson filter, final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return createDistinctIterable(clientSession, fieldName, filter, resultClass);
    }

    private <TResult> AsyncDistinctIterable<TResult> createDistinctIterable(@Nullable final AsyncClientSession clientSession,
                                                                            final String fieldName, final Bson filter,
                                                                            final Class<TResult> resultClass) {
        return new AsyncDistinctIterableImpl<>(clientSession, namespace, documentClass, resultClass, codecRegistry, readPreference,
                readConcern, executor, fieldName, filter, retryReads);
    }

    @Override
    public AsyncFindIterable<TDocument> find() {
        return find(new BsonDocument(), documentClass);
    }

    @Override
    public <TResult> AsyncFindIterable<TResult> find(final Class<TResult> resultClass) {
        return find(new BsonDocument(), resultClass);
    }

    @Override
    public AsyncFindIterable<TDocument> find(final Bson filter) {
        return find(filter, documentClass);
    }

    @Override
    public <TResult> AsyncFindIterable<TResult> find(final Bson filter, final Class<TResult> resultClass) {
        return createFindIterable(null, filter, resultClass);
    }

    @Override
    public AsyncFindIterable<TDocument> find(final AsyncClientSession clientSession) {
        return find(clientSession, new BsonDocument(), documentClass);
    }

    @Override
    public <TResult> AsyncFindIterable<TResult> find(final AsyncClientSession clientSession, final Class<TResult> resultClass) {
        return find(clientSession, new BsonDocument(), resultClass);
    }

    @Override
    public AsyncFindIterable<TDocument> find(final AsyncClientSession clientSession, final Bson filter) {
        return find(clientSession, filter, documentClass);
    }

    @Override
    public <TResult> AsyncFindIterable<TResult> find(final AsyncClientSession clientSession, final Bson filter,
                                                     final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return createFindIterable(clientSession, filter, resultClass);
    }

    private <TResult> AsyncFindIterable<TResult> createFindIterable(@Nullable final AsyncClientSession clientSession, final Bson filter,
                                                               final Class<TResult> resultClass) {
        return new AsyncFindIterableImpl<TDocument, TResult>(clientSession, namespace, documentClass, resultClass, codecRegistry,
                readPreference, readConcern, executor, filter, retryReads);
    }

    @Override
    public AsyncAggregateIterable<TDocument> aggregate(final List<? extends Bson> pipeline) {
        return aggregate(pipeline, documentClass);
    }

    @Override
    public <TResult> AsyncAggregateIterable<TResult> aggregate(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return createAggregateIterable(null, pipeline, resultClass);
    }

    @Override
    public AsyncAggregateIterable<TDocument> aggregate(final AsyncClientSession clientSession, final List<? extends Bson> pipeline) {
        return aggregate(clientSession, pipeline, documentClass);
    }

    @Override
    public <TResult> AsyncAggregateIterable<TResult> aggregate(final AsyncClientSession clientSession, final List<? extends Bson> pipeline,
                                                          final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return createAggregateIterable(clientSession, pipeline, resultClass);
    }

    private <TResult> AsyncAggregateIterable<TResult> createAggregateIterable(@Nullable final AsyncClientSession clientSession,
                                                                              final List<? extends Bson> pipeline,
                                                                              final Class<TResult> resultClass) {
        return new AsyncAggregateIterableImpl<>(clientSession, namespace, documentClass, resultClass, codecRegistry,
                readPreference, readConcern, writeConcern, executor, pipeline, AggregationLevel.COLLECTION, retryReads);
    }

    @Override
    public AsyncChangeStreamIterable<TDocument> watch() {
        return watch(Collections.emptyList());
    }

    @Override
    public <TResult> AsyncChangeStreamIterable<TResult> watch(final Class<TResult> resultClass) {
        return watch(Collections.emptyList(), resultClass);
    }

    @Override
    public AsyncChangeStreamIterable<TDocument> watch(final List<? extends Bson> pipeline) {
        return watch(pipeline, documentClass);
    }

    @Override
    public <TResult> AsyncChangeStreamIterable<TResult> watch(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return createChangeStreamIterable(null, pipeline, resultClass);
    }

    @Override
    public AsyncChangeStreamIterable<TDocument> watch(final AsyncClientSession clientSession) {
        return watch(clientSession, Collections.<Bson>emptyList());
    }

    @Override
    public <TResult> AsyncChangeStreamIterable<TResult> watch(final AsyncClientSession clientSession, final Class<TResult> resultClass) {
        return watch(clientSession, Collections.<Bson>emptyList(), resultClass);
    }

    @Override
    public AsyncChangeStreamIterable<TDocument> watch(final AsyncClientSession clientSession, final List<? extends Bson> pipeline) {
        return watch(clientSession, pipeline, documentClass);
    }

    @Override
    public <TResult> AsyncChangeStreamIterable<TResult> watch(final AsyncClientSession clientSession, final List<? extends Bson> pipeline,
                                                              final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return createChangeStreamIterable(clientSession, pipeline, resultClass);
    }

    private <TResult> AsyncChangeStreamIterable<TResult> createChangeStreamIterable(@Nullable final AsyncClientSession clientSession,
                                                                                    final List<? extends Bson> pipeline,
                                                                                    final Class<TResult> resultClass) {
        return new AsyncChangeStreamIterableImpl<TResult>(clientSession, namespace, codecRegistry, readPreference, readConcern, executor,
                pipeline, resultClass, ChangeStreamLevel.COLLECTION, retryReads);
    }

    @Override
    public AsyncMapReduceIterable<TDocument> mapReduce(final String mapFunction, final String reduceFunction) {
        return mapReduce(mapFunction, reduceFunction, documentClass);
    }

    @Override
    public <TResult> AsyncMapReduceIterable<TResult> mapReduce(final String mapFunction, final String reduceFunction,
                                                               final Class<TResult> resultClass) {
        return createMapReduceIterable(null, mapFunction, reduceFunction, resultClass);
    }

    @Override
    public AsyncMapReduceIterable<TDocument> mapReduce(final AsyncClientSession clientSession, final String mapFunction,
                                                       final String reduceFunction) {
        return mapReduce(clientSession, mapFunction, reduceFunction, documentClass);
    }

    @Override
    public <TResult> AsyncMapReduceIterable<TResult> mapReduce(final AsyncClientSession clientSession, final String mapFunction,
                                                               final String reduceFunction, final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return createMapReduceIterable(clientSession, mapFunction, reduceFunction, resultClass);
    }

    private <TResult> AsyncMapReduceIterable<TResult> createMapReduceIterable(@Nullable final AsyncClientSession clientSession,
                                                                              final String mapFunction, final String reduceFunction,
                                                                              final Class<TResult> resultClass) {
        return new AsyncMapReduceIterableImpl<>(clientSession, namespace, documentClass, resultClass, codecRegistry,
                readPreference, readConcern, writeConcern, executor, mapFunction, reduceFunction);
    }

    @Override
    public void bulkWrite(final List<? extends WriteModel<? extends TDocument>> requests,
                          final SingleResultCallback<BulkWriteResult> callback) {
        bulkWrite(requests, new BulkWriteOptions(), callback);
    }

    @Override
    public void bulkWrite(final List<? extends WriteModel<? extends TDocument>> requests, final BulkWriteOptions options,
                          final SingleResultCallback<BulkWriteResult> callback) {
        executeBulkWrite(null, requests, options, callback);
    }

    @Override
    public void bulkWrite(final AsyncClientSession clientSession, final List<? extends WriteModel<? extends TDocument>> requests,
                          final SingleResultCallback<BulkWriteResult> callback) {
        bulkWrite(clientSession, requests, new BulkWriteOptions(), callback);
    }

    @Override
    public void bulkWrite(final AsyncClientSession clientSession, final List<? extends WriteModel<? extends TDocument>> requests,
                          final BulkWriteOptions options, final SingleResultCallback<BulkWriteResult> callback) {
        notNull("clientSession", clientSession);
        executeBulkWrite(clientSession, requests, options, callback);
    }

    @SuppressWarnings("unchecked")
    private void executeBulkWrite(@Nullable final AsyncClientSession clientSession,
                                  final List<? extends WriteModel<? extends TDocument>> requests,
                                  final BulkWriteOptions options, final SingleResultCallback<BulkWriteResult> callback) {
        notNull("requests", requests);
        executor.execute(operations.bulkWrite(requests, options), readConcern, clientSession, callback);
    }

    @Override
    public void insertOne(final TDocument document, final SingleResultCallback<InsertOneResult> callback) {
        insertOne(document, new InsertOneOptions(), callback);
    }

    @Override
    public void insertOne(final TDocument document, final InsertOneOptions options,
                          final SingleResultCallback<InsertOneResult> callback) {
        executeInsertOne(null, document, options, callback);
    }

    @Override
    public void insertOne(final AsyncClientSession clientSession, final TDocument document,
                          final SingleResultCallback<InsertOneResult> callback) {
        insertOne(clientSession, document, new InsertOneOptions(), callback);
    }

    @Override
    public void insertOne(final AsyncClientSession clientSession, final TDocument document, final InsertOneOptions options,
                          final SingleResultCallback<InsertOneResult> callback) {
        notNull("clientSession", clientSession);
        executeInsertOne(clientSession, document, options, callback);
    }

    private void executeInsertOne(@Nullable final AsyncClientSession clientSession, final TDocument document,
                                  final InsertOneOptions options, final SingleResultCallback<InsertOneResult> callback) {
        executeSingleWriteRequest(clientSession, operations.insertOne(document, options), INSERT,
                (result, t) -> {
                    if (t != null) {
                        callback.onResult(null, t);
                    } else {
                        callback.onResult(toInsertOneResult(result), null);
                    }
                });
    }

    @Override
    public void insertMany(final List<? extends TDocument> documents,
                           final SingleResultCallback<InsertManyResult> callback) {
        insertMany(documents, new InsertManyOptions(), callback);
    }

    @Override
    public void insertMany(final List<? extends TDocument> documents, final InsertManyOptions options,
                           final SingleResultCallback<InsertManyResult> callback) {
        executeInsertMany(null, documents, options, callback);
    }

    @Override
    public void insertMany(final AsyncClientSession clientSession, final List<? extends TDocument> documents,
                           final SingleResultCallback<InsertManyResult> callback) {
        insertMany(clientSession, documents, new InsertManyOptions(), callback);
    }

    @Override
    public void insertMany(final AsyncClientSession clientSession, final List<? extends TDocument> documents,
                           final InsertManyOptions options, final SingleResultCallback<InsertManyResult> callback) {
        notNull("clientSession", clientSession);
        executeInsertMany(clientSession, documents, options, callback);
    }

    private void executeInsertMany(@Nullable final AsyncClientSession clientSession, final List<? extends TDocument> documents,
                                   final InsertManyOptions options, final SingleResultCallback<InsertManyResult> callback) {
        executor.execute(operations.insertMany(documents, options), readConcern, clientSession,
                (result, t) -> {
                    if (t != null) {
                        callback.onResult(null, t);
                    } else {
                        callback.onResult(toInsertManyResult(result), null);
                    }
                });
    }

    @Override
    public void deleteOne(final Bson filter, final SingleResultCallback<DeleteResult> callback) {
        deleteOne(filter, new DeleteOptions(), callback);
    }

    @Override
    public void deleteOne(final Bson filter, final DeleteOptions options, final SingleResultCallback<DeleteResult> callback) {
        executeDelete(null, filter, options, false, callback);
    }

    @Override
    public void deleteOne(final AsyncClientSession clientSession, final Bson filter, final SingleResultCallback<DeleteResult> callback) {
        deleteOne(clientSession, filter, new DeleteOptions(), callback);
    }

    @Override
    public void deleteOne(final AsyncClientSession clientSession, final Bson filter, final DeleteOptions options,
                          final SingleResultCallback<DeleteResult> callback) {
        notNull("clientSession", clientSession);
        executeDelete(clientSession, filter, options, false, callback);
    }

    @Override
    public void deleteMany(final Bson filter, final SingleResultCallback<DeleteResult> callback) {
        deleteMany(filter, new DeleteOptions(), callback);
    }

    @Override
    public void deleteMany(final Bson filter, final DeleteOptions options, final SingleResultCallback<DeleteResult> callback) {
        executeDelete(null, filter, options, true, callback);
    }

    @Override
    public void deleteMany(final AsyncClientSession clientSession, final Bson filter, final SingleResultCallback<DeleteResult> callback) {
        deleteMany(clientSession, filter, new DeleteOptions(), callback);
    }

    @Override
    public void deleteMany(final AsyncClientSession clientSession, final Bson filter, final DeleteOptions options,
                           final SingleResultCallback<DeleteResult> callback) {
        notNull("clientSession", clientSession);
        executeDelete(clientSession, filter, options, true, callback);
    }

    private void executeDelete(@Nullable final AsyncClientSession clientSession, final Bson filter, final DeleteOptions options,
                               final boolean multi, final SingleResultCallback<DeleteResult> callback) {
        executeSingleWriteRequest(clientSession,
                multi ? operations.deleteMany(filter, options) : operations.deleteOne(filter, options), DELETE,
                (result, t) -> {
                    if (t != null) {
                        callback.onResult(null, t);
                    } else {
                        if (result.wasAcknowledged()) {
                            callback.onResult(DeleteResult.acknowledged(result.getDeletedCount()), null);
                        } else {
                            callback.onResult(DeleteResult.unacknowledged(), null);
                        }
                    }
                });
    }

    @Override
    public void replaceOne(final Bson filter, final TDocument replacement, final SingleResultCallback<UpdateResult> callback) {
        replaceOne(filter, replacement, new ReplaceOptions(), callback);
    }

    @Override
    public void replaceOne(final Bson filter, final TDocument replacement, final ReplaceOptions options,
                           final SingleResultCallback<UpdateResult> callback) {
        executeReplaceOne(null, filter, replacement, options, callback);
    }

    @Override
    public void replaceOne(final AsyncClientSession clientSession, final Bson filter, final TDocument replacement,
                           final SingleResultCallback<UpdateResult> callback) {
        replaceOne(clientSession, filter, replacement, new ReplaceOptions(), callback);
    }

    @Override
    public void replaceOne(final AsyncClientSession clientSession, final Bson filter, final TDocument replacement,
                           final ReplaceOptions options, final SingleResultCallback<UpdateResult> callback) {
        notNull("clientSession", clientSession);
        executeReplaceOne(clientSession, filter, replacement, options, callback);
    }

    private void executeReplaceOne(@Nullable final AsyncClientSession clientSession, final Bson filter, final TDocument replacement,
                                   final ReplaceOptions options, final SingleResultCallback<UpdateResult> callback) {
        executeSingleWriteRequest(clientSession, operations.replaceOne(filter, replacement, options), REPLACE,
                (result, t) -> {
                    if (t != null) {
                        callback.onResult(null, t);
                    } else {
                        callback.onResult(toUpdateResult(result), null);
                    }
                });
    }

    @Override
    public void updateOne(final Bson filter, final Bson update, final SingleResultCallback<UpdateResult> callback) {
        updateOne(filter, update, new UpdateOptions(), callback);
    }

    @Override
    public void updateOne(final Bson filter, final Bson update, final UpdateOptions options,
                          final SingleResultCallback<UpdateResult> callback) {
        executeUpdate(null, filter, update, options, false, callback);
    }

    @Override
    public void updateOne(final AsyncClientSession clientSession, final Bson filter, final Bson update,
                          final SingleResultCallback<UpdateResult> callback) {
        updateOne(clientSession, filter, update, new UpdateOptions(), callback);
    }

    @Override
    public void updateOne(final AsyncClientSession clientSession, final Bson filter, final Bson update, final UpdateOptions options,
                          final SingleResultCallback<UpdateResult> callback) {
        notNull("clientSession", clientSession);
        executeUpdate(clientSession, filter, update, options, false, callback);
    }

    @Override
    public void updateOne(final Bson filter, final List<? extends Bson> update, final SingleResultCallback<UpdateResult> callback) {
        updateOne(filter, update, new UpdateOptions(), callback);
    }

    @Override
    public void updateOne(final Bson filter, final List<? extends Bson> update, final UpdateOptions options,
                          final SingleResultCallback<UpdateResult> callback) {
        executeUpdate(null, filter, update, options, false, callback);
    }

    @Override
    public void updateOne(final AsyncClientSession clientSession, final Bson filter, final List<? extends Bson> update,
                          final SingleResultCallback<UpdateResult> callback) {
        updateOne(clientSession, filter, update, new UpdateOptions(), callback);
    }

    @Override
    public void updateOne(final AsyncClientSession clientSession, final Bson filter, final List<? extends Bson> update,
                          final UpdateOptions options, final SingleResultCallback<UpdateResult> callback) {
        notNull("clientSession", clientSession);
        executeUpdate(clientSession, filter, update, options, false, callback);
    }

    @Override
    public void updateMany(final Bson filter, final Bson update, final SingleResultCallback<UpdateResult> callback) {
        updateMany(filter, update, new UpdateOptions(), callback);
    }

    @Override
    public void updateMany(final Bson filter, final Bson update, final UpdateOptions options,
                           final SingleResultCallback<UpdateResult> callback) {
        executeUpdate(null, filter, update, options, true, callback);
    }

    @Override
    public void updateMany(final AsyncClientSession clientSession, final Bson filter, final Bson update,
                           final SingleResultCallback<UpdateResult> callback) {
        updateMany(clientSession, filter, update, new UpdateOptions(), callback);
    }

    @Override
    public void updateMany(final AsyncClientSession clientSession, final Bson filter, final Bson update, final UpdateOptions options,
                           final SingleResultCallback<UpdateResult> callback) {
        notNull("clientSession", clientSession);
        executeUpdate(clientSession, filter, update, options, true, callback);
    }

    @Override
    public void updateMany(final Bson filter, final List<? extends Bson> update, final SingleResultCallback<UpdateResult> callback) {
        updateMany(filter, update, new UpdateOptions(), callback);
    }

    @Override
    public void updateMany(final Bson filter, final List<? extends Bson> update, final UpdateOptions options,
                           final SingleResultCallback<UpdateResult> callback) {
        executeUpdate(null, filter, update, options, true, callback);
    }

    @Override
    public void updateMany(final AsyncClientSession clientSession, final Bson filter, final List<? extends Bson> update,
                           final SingleResultCallback<UpdateResult> callback) {
        updateMany(clientSession, filter, update, new UpdateOptions(), callback);
    }

    @Override
    public void updateMany(final AsyncClientSession clientSession, final Bson filter, final List<? extends Bson> update,
                           final UpdateOptions options, final SingleResultCallback<UpdateResult> callback) {
        notNull("clientSession", clientSession);
        executeUpdate(clientSession, filter, update, options, true, callback);
    }

    private void executeUpdate(@Nullable final AsyncClientSession clientSession, final Bson filter, final Bson update,
                               final UpdateOptions options, final boolean multi, final SingleResultCallback<UpdateResult> callback) {
        executeSingleWriteRequest(clientSession,
                multi ? operations.updateMany(filter, update, options) : operations.updateOne(filter, update, options), UPDATE,
                (result, t) -> {
                    if (t != null) {
                        callback.onResult(null, t);
                    } else {
                        callback.onResult(toUpdateResult(result), null);
                    }
                });
    }

    private void executeUpdate(@Nullable final AsyncClientSession clientSession, final Bson filter, final List<? extends Bson> update,
                               final UpdateOptions options, final boolean multi, final SingleResultCallback<UpdateResult> callback) {
        executeSingleWriteRequest(clientSession,
                multi ? operations.updateMany(filter, update, options) : operations.updateOne(filter, update, options), UPDATE,
                (result, t) -> {
                    if (t != null) {
                        callback.onResult(null, t);
                    } else {
                        callback.onResult(toUpdateResult(result), null);
                    }
                });
    }

    @Override
    public void findOneAndDelete(final Bson filter, final SingleResultCallback<TDocument> callback) {
        findOneAndDelete(filter, new FindOneAndDeleteOptions(), callback);
    }

    @Override
    public void findOneAndDelete(final Bson filter, final FindOneAndDeleteOptions options,
                                 final SingleResultCallback<TDocument> callback) {
        executeFindOneAndDelete(null, filter, options, callback);
    }

    @Override
    public void findOneAndDelete(final AsyncClientSession clientSession, final Bson filter,
                                 final SingleResultCallback<TDocument> callback) {
        findOneAndDelete(clientSession, filter, new FindOneAndDeleteOptions(), callback);
    }

    @Override
    public void findOneAndDelete(final AsyncClientSession clientSession, final Bson filter, final FindOneAndDeleteOptions options,
                                 final SingleResultCallback<TDocument> callback) {
        notNull("clientSession", clientSession);
        executeFindOneAndDelete(clientSession, filter, options, callback);
    }

    private void executeFindOneAndDelete(@Nullable final AsyncClientSession clientSession, final Bson filter,
                                         final FindOneAndDeleteOptions options, final SingleResultCallback<TDocument> callback) {
        executor.execute(operations.findOneAndDelete(filter, options), readConcern, clientSession, callback);
    }

    @Override
    public void findOneAndReplace(final Bson filter, final TDocument replacement, final SingleResultCallback<TDocument> callback) {
        findOneAndReplace(filter, replacement, new FindOneAndReplaceOptions(), callback);
    }

    @Override
    public void findOneAndReplace(final Bson filter, final TDocument replacement, final FindOneAndReplaceOptions options,
                                  final SingleResultCallback<TDocument> callback) {
        executeFindOneAndReplace(null, filter, replacement, options, callback);
    }

    @Override
    public void findOneAndReplace(final AsyncClientSession clientSession, final Bson filter, final TDocument replacement,
                                  final SingleResultCallback<TDocument> callback) {
        findOneAndReplace(clientSession, filter, replacement, new FindOneAndReplaceOptions(), callback);
    }

    @Override
    public void findOneAndReplace(final AsyncClientSession clientSession, final Bson filter, final TDocument replacement,
                                  final FindOneAndReplaceOptions options, final SingleResultCallback<TDocument> callback) {
        notNull("clientSession", clientSession);
        executeFindOneAndReplace(clientSession, filter, replacement, options, callback);
    }

    private void executeFindOneAndReplace(@Nullable final AsyncClientSession clientSession, final Bson filter, final TDocument replacement,
                                          final FindOneAndReplaceOptions options, final SingleResultCallback<TDocument> callback) {
        executor.execute(operations.findOneAndReplace(filter, replacement, options), readConcern, clientSession, callback);
    }

    @Override
    public void findOneAndUpdate(final Bson filter, final Bson update, final SingleResultCallback<TDocument> callback) {
        findOneAndUpdate(filter, update, new FindOneAndUpdateOptions(), callback);
    }

    @Override
    public void findOneAndUpdate(final Bson filter, final Bson update, final FindOneAndUpdateOptions options,
                                 final SingleResultCallback<TDocument> callback) {
        executeFindOneAndUpdate(null, filter, update, options, callback);
    }

    @Override
    public void findOneAndUpdate(final AsyncClientSession clientSession, final Bson filter, final Bson update,
                                 final SingleResultCallback<TDocument> callback) {
        findOneAndUpdate(clientSession, filter, update, new FindOneAndUpdateOptions(), callback);
    }

    @Override
    public void findOneAndUpdate(final AsyncClientSession clientSession, final Bson filter, final Bson update,
                                 final FindOneAndUpdateOptions options, final SingleResultCallback<TDocument> callback) {
        notNull("clientSession", clientSession);
        executeFindOneAndUpdate(clientSession, filter, update, options, callback);
    }

    @Override
    public void findOneAndUpdate(final Bson filter, final List<? extends Bson> update, final SingleResultCallback<TDocument> callback) {
        findOneAndUpdate(filter, update, new FindOneAndUpdateOptions(), callback);
    }

    @Override
    public void findOneAndUpdate(final Bson filter, final List<? extends Bson> update, final FindOneAndUpdateOptions options,
                                 final SingleResultCallback<TDocument> callback) {
        executeFindOneAndUpdate(null, filter, update, options, callback);
    }

    @Override
    public void findOneAndUpdate(final AsyncClientSession clientSession, final Bson filter, final List<? extends Bson> update,
                                 final SingleResultCallback<TDocument> callback) {
        findOneAndUpdate(clientSession, filter, update, new FindOneAndUpdateOptions(), callback);
    }

    @Override
    public void findOneAndUpdate(final AsyncClientSession clientSession, final Bson filter, final List<? extends Bson> update,
                                 final FindOneAndUpdateOptions options, final SingleResultCallback<TDocument> callback) {
        notNull("clientSession", clientSession);
        executeFindOneAndUpdate(clientSession, filter, update, options, callback);
    }

    private void executeFindOneAndUpdate(@Nullable final AsyncClientSession clientSession, final Bson filter, final Bson update,
                                         final FindOneAndUpdateOptions options, final SingleResultCallback<TDocument> callback) {
        executor.execute(operations.findOneAndUpdate(filter, update, options), readConcern, clientSession, callback);
    }

    private void executeFindOneAndUpdate(@Nullable final AsyncClientSession clientSession, final Bson filter,
                                         final List<? extends Bson> update, final FindOneAndUpdateOptions options,
                                         final SingleResultCallback<TDocument> callback) {
        executor.execute(operations.findOneAndUpdate(filter, update, options), readConcern, clientSession, callback);
    }

    @Override
    public void drop(final SingleResultCallback<Void> callback) {
        executeDrop(null, callback);
    }

    @Override
    public void drop(final AsyncClientSession clientSession, final SingleResultCallback<Void> callback) {
        notNull("clientSession", clientSession);
        executeDrop(clientSession, callback);
    }

    private void executeDrop(@Nullable final AsyncClientSession clientSession, final SingleResultCallback<Void> callback) {
        executor.execute(operations.dropCollection(), readConcern, clientSession, callback);
    }

    @Override
    public void createIndex(final Bson key, final SingleResultCallback<String> callback) {
        createIndex(key, new IndexOptions(), callback);
    }

    @Override
    public void createIndex(final Bson key, final IndexOptions indexOptions, final SingleResultCallback<String> callback) {
        createIndexes(singletonList(new IndexModel(key, indexOptions)), (result, t) -> {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                callback.onResult(result.get(0), null);
            }
        });
    }

    @Override
    public void createIndex(final AsyncClientSession clientSession, final Bson key, final SingleResultCallback<String> callback) {
        createIndex(clientSession, key, new IndexOptions(), callback);
    }

    @Override
    public void createIndex(final AsyncClientSession clientSession, final Bson key, final IndexOptions indexOptions,
                            final SingleResultCallback<String> callback) {
        createIndexes(clientSession, singletonList(new IndexModel(key, indexOptions)), (result, t) -> {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                callback.onResult(result.get(0), null);
            }
        });
    }

    @Override
    public void createIndexes(final List<IndexModel> indexes, final SingleResultCallback<List<String>> callback) {
        createIndexes(indexes, new CreateIndexOptions(), callback);
    }

    @Override
    public void createIndexes(final List<IndexModel> indexes, final CreateIndexOptions createIndexOptions,
                              final SingleResultCallback<List<String>> callback) {
        executeCreateIndexes(null, indexes, createIndexOptions, callback);
    }

    @Override
    public void createIndexes(final AsyncClientSession clientSession, final List<IndexModel> indexes,
                              final SingleResultCallback<List<String>> callback) {
        createIndexes(clientSession, indexes, new CreateIndexOptions(), callback);
    }

    @Override
    public void createIndexes(final AsyncClientSession clientSession, final List<IndexModel> indexes,
                              final CreateIndexOptions createIndexOptions, final SingleResultCallback<List<String>> callback) {
        notNull("clientSession", clientSession);
        executeCreateIndexes(clientSession, indexes, createIndexOptions, callback);
    }

    private void executeCreateIndexes(@Nullable final AsyncClientSession clientSession, final List<IndexModel> indexes,
                                      final CreateIndexOptions createIndexOptions, final SingleResultCallback<List<String>> callback) {
        executor.execute(operations.createIndexes(indexes, createIndexOptions), readConcern, clientSession,
                (result, t) -> {
                    if (t != null) {
                        callback.onResult(null, t);
                    } else {
                        callback.onResult(IndexHelper.getIndexNames(indexes, codecRegistry), null);
                    }
                });
    }

    @Override
    public AsyncListIndexesIterable<Document> listIndexes() {
        return listIndexes(Document.class);
    }

    @Override
    public <TResult> AsyncListIndexesIterable<TResult> listIndexes(final Class<TResult> resultClass) {
        return createListIndexesIterable(null, resultClass);
    }

    @Override
    public AsyncListIndexesIterable<Document> listIndexes(final AsyncClientSession clientSession) {
        return listIndexes(clientSession, Document.class);
    }

    @Override
    public <TResult> AsyncListIndexesIterable<TResult> listIndexes(final AsyncClientSession clientSession,
                                                                   final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return createListIndexesIterable(clientSession, resultClass);
    }

    private <TResult> AsyncListIndexesIterable<TResult> createListIndexesIterable(@Nullable final AsyncClientSession clientSession,
                                                                             final Class<TResult> resultClass) {
        return new AsyncListIndexesIterableImpl<TResult>(clientSession, namespace, resultClass, codecRegistry, readPreference, executor,
                retryReads);
    }

    @Override
    public void dropIndex(final String indexName, final SingleResultCallback<Void> callback) {
        dropIndex(indexName, new DropIndexOptions(), callback);
    }

    @Override
    public void dropIndex(final String indexName, final DropIndexOptions dropIndexOptions, final SingleResultCallback<Void> callback) {
        executeDropIndex(null, indexName, dropIndexOptions, callback);
    }

    @Override
    public void dropIndex(final Bson keys, final SingleResultCallback<Void> callback) {
        dropIndex(keys, new DropIndexOptions(), callback);
    }

    @Override
    public void dropIndex(final Bson keys, final DropIndexOptions dropIndexOptions, final SingleResultCallback<Void> callback) {
        executeDropIndex(null, keys, dropIndexOptions, callback);
    }

    @Override
    public void dropIndex(final AsyncClientSession clientSession, final String indexName, final SingleResultCallback<Void> callback) {
        dropIndex(clientSession, indexName, new DropIndexOptions(), callback);
    }

    @Override
    public void dropIndex(final AsyncClientSession clientSession, final String indexName, final DropIndexOptions dropIndexOptions,
                          final SingleResultCallback<Void> callback) {
        notNull("clientSession", clientSession);
        executeDropIndex(clientSession, indexName, dropIndexOptions, callback);
    }

    @Override
    public void dropIndex(final AsyncClientSession clientSession, final Bson keys, final SingleResultCallback<Void> callback) {
        dropIndex(clientSession, keys, new DropIndexOptions(), callback);
    }

    @Override
    public void dropIndex(final AsyncClientSession clientSession, final Bson keys, final DropIndexOptions dropIndexOptions,
                          final SingleResultCallback<Void> callback) {
        notNull("clientSession", clientSession);
        executeDropIndex(clientSession, keys, dropIndexOptions, callback);
    }

    @Override
    public void dropIndexes(final SingleResultCallback<Void> callback) {
        dropIndexes(new DropIndexOptions(), callback);
    }

    @Override
    public void dropIndexes(final DropIndexOptions dropIndexOptions, final SingleResultCallback<Void> callback) {
        dropIndex("*", dropIndexOptions, callback);
    }

    @Override
    public void dropIndexes(final AsyncClientSession clientSession, final SingleResultCallback<Void> callback) {
        dropIndexes(clientSession, new DropIndexOptions(), callback);
    }

    @Override
    public void dropIndexes(final AsyncClientSession clientSession, final DropIndexOptions dropIndexOptions,
                            final SingleResultCallback<Void> callback) {
        dropIndex(clientSession, "*", dropIndexOptions, callback);
    }

    private void executeDropIndex(@Nullable final AsyncClientSession clientSession, final Bson keys,
                                  final DropIndexOptions dropIndexOptions, final SingleResultCallback<Void> callback) {
        executor.execute(operations.dropIndex(keys, dropIndexOptions), readConcern, clientSession, callback);
    }

    private void executeDropIndex(@Nullable final AsyncClientSession clientSession, final String indexName,
                                  final DropIndexOptions dropIndexOptions, final SingleResultCallback<Void> callback) {
        executor.execute(operations.dropIndex(indexName, dropIndexOptions), readConcern, clientSession, callback);
    }

    @Override
    public void renameCollection(final MongoNamespace newCollectionNamespace, final SingleResultCallback<Void> callback) {
        renameCollection(newCollectionNamespace, new RenameCollectionOptions(), callback);
    }

    @Override
    public void renameCollection(final MongoNamespace newCollectionNamespace, final RenameCollectionOptions options,
                                 final SingleResultCallback<Void> callback) {
        executeRenameCollection(null, newCollectionNamespace, options, callback);
    }

    @Override
    public void renameCollection(final AsyncClientSession clientSession, final MongoNamespace newCollectionNamespace,
                                 final SingleResultCallback<Void> callback) {
        renameCollection(clientSession, newCollectionNamespace, new RenameCollectionOptions(), callback);
    }

    @Override
    public void renameCollection(final AsyncClientSession clientSession, final MongoNamespace newCollectionNamespace,
                                 final RenameCollectionOptions options, final SingleResultCallback<Void> callback) {
        notNull("clientSession", clientSession);
        executeRenameCollection(clientSession, newCollectionNamespace, options, callback);
    }

    private void executeRenameCollection(@Nullable final AsyncClientSession clientSession, final MongoNamespace newCollectionNamespace,
                                         final RenameCollectionOptions options, final SingleResultCallback<Void> callback) {
        executor.execute(operations.renameCollection(newCollectionNamespace, options), readConcern, clientSession, callback);
    }

    private void executeSingleWriteRequest(@Nullable final AsyncClientSession clientSession,
                                           final AsyncWriteOperation<BulkWriteResult> writeOperation,
                                           final WriteRequest.Type type, final SingleResultCallback<BulkWriteResult> callback) {
        executor.execute(writeOperation, readConcern, clientSession, (result, t) -> {
            if (t instanceof MongoBulkWriteException) {
                MongoBulkWriteException e = (MongoBulkWriteException) t;
                MongoException exception;
                if (e.getWriteErrors().isEmpty()) {
                    exception = new MongoWriteConcernException(e.getWriteConcernError(),
                            translateBulkWriteResult(type, e.getWriteResult()), e.getServerAddress());
                } else {
                    exception = new MongoWriteException(new WriteError(e.getWriteErrors().get(0)), e.getServerAddress());
                }
                for (final String errorLabel : e.getErrorLabels()) {
                    exception.addLabel(errorLabel);
                }
                callback.onResult(null, exception);
            } else {
                callback.onResult(result, t);
            }
        });
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
}
