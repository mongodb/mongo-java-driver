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
import com.mongodb.bulk.WriteRequest;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.DropIndexOptions;
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
import com.mongodb.client.model.changestream.ChangeStreamLevel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.internal.operation.IndexHelper;
import com.mongodb.internal.operation.SyncOperations;
import com.mongodb.lang.Nullable;
import com.mongodb.operation.RenameCollectionOperation;
import com.mongodb.operation.WriteOperation;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.bulk.WriteRequest.Type.DELETE;
import static com.mongodb.bulk.WriteRequest.Type.INSERT;
import static com.mongodb.bulk.WriteRequest.Type.REPLACE;
import static com.mongodb.bulk.WriteRequest.Type.UPDATE;
import static com.mongodb.client.model.ReplaceOptions.createReplaceOptions;
import static java.util.Collections.singletonList;

class MongoCollectionImpl<TDocument> implements MongoCollection<TDocument> {
    private final MongoNamespace namespace;
    private final Class<TDocument> documentClass;
    private final ReadPreference readPreference;
    private final CodecRegistry codecRegistry;
    private final WriteConcern writeConcern;
    private final boolean retryWrites;
    private final ReadConcern readConcern;
    private final SyncOperations<TDocument> operations;
    private final OperationExecutor executor;

    MongoCollectionImpl(final MongoNamespace namespace, final Class<TDocument> documentClass, final CodecRegistry codecRegistry,
                        final ReadPreference readPreference, final WriteConcern writeConcern, final boolean retryWrites,
                        final ReadConcern readConcern, final OperationExecutor executor) {
        this.namespace = notNull("namespace", namespace);
        this.documentClass = notNull("documentClass", documentClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.retryWrites = retryWrites;
        this.readConcern = notNull("readConcern", readConcern);
        this.executor = notNull("executor", executor);
        this.operations = new SyncOperations<TDocument>(namespace, documentClass, readPreference, codecRegistry, writeConcern, retryWrites);
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
    public <NewTDocument> MongoCollection<NewTDocument> withDocumentClass(final Class<NewTDocument> clazz) {
        return new MongoCollectionImpl<NewTDocument>(namespace, clazz, codecRegistry, readPreference, writeConcern, retryWrites,
                readConcern, executor);
    }

    @Override
    public MongoCollection<TDocument> withCodecRegistry(final CodecRegistry codecRegistry) {
        return new MongoCollectionImpl<TDocument>(namespace, documentClass, codecRegistry, readPreference, writeConcern, retryWrites,
                readConcern, executor);
    }

    @Override
    public MongoCollection<TDocument> withReadPreference(final ReadPreference readPreference) {
        return new MongoCollectionImpl<TDocument>(namespace, documentClass, codecRegistry, readPreference, writeConcern, retryWrites,
                readConcern, executor);
    }

    @Override
    public MongoCollection<TDocument> withWriteConcern(final WriteConcern writeConcern) {
        return new MongoCollectionImpl<TDocument>(namespace, documentClass, codecRegistry, readPreference, writeConcern, retryWrites,
                readConcern, executor);
    }

    @Override
    public MongoCollection<TDocument> withReadConcern(final ReadConcern readConcern) {
        return new MongoCollectionImpl<TDocument>(namespace, documentClass, codecRegistry, readPreference, writeConcern, retryWrites,
                readConcern, executor);
    }

    @Override
    public long count() {
        return count(new BsonDocument(), new CountOptions());
    }

    @Override
    public long count(final Bson filter) {
        return count(filter, new CountOptions());
    }

    @Override
    public long count(final Bson filter, final CountOptions options) {
        return executeCount(null, filter, options);
    }

    @Override
    public long count(final ClientSession clientSession) {
        return count(clientSession, new BsonDocument());
    }

    @Override
    public long count(final ClientSession clientSession, final Bson filter) {
        return count(clientSession, filter, new CountOptions());
    }

    @Override
    public long count(final ClientSession clientSession, final Bson filter, final CountOptions options) {
        notNull("clientSession", clientSession);
        return executeCount(clientSession, filter, options);
    }

    private long executeCount(@Nullable final ClientSession clientSession, final Bson filter, final CountOptions options) {
        return executor.execute(operations.count(filter, options), readPreference, readConcern, clientSession);
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
        return new DistinctIterableImpl<TDocument, TResult>(clientSession, namespace, documentClass, resultClass, codecRegistry,
                readPreference, readConcern, executor, fieldName, filter);
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
        return new FindIterableImpl<TDocument, TResult>(clientSession, namespace, this.documentClass, resultClass, codecRegistry,
                readPreference, readConcern, executor, filter);
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
        return new AggregateIterableImpl<TDocument, TResult>(clientSession, namespace, documentClass, resultClass, codecRegistry,
                readPreference, readConcern, writeConcern, executor, pipeline);
    }

    @Override
    public ChangeStreamIterable<TDocument> watch() {
        return watch(Collections.<Bson>emptyList());
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final Class<TResult> resultClass) {
        return watch(Collections.<Bson>emptyList(), resultClass);
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
        return watch(clientSession, Collections.<Bson>emptyList(), documentClass);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final ClientSession clientSession, final Class<TResult> resultClass) {
        return watch(clientSession, Collections.<Bson>emptyList(), resultClass);
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
        return new ChangeStreamIterableImpl<TResult>(clientSession, namespace, codecRegistry, readPreference, readConcern, executor,
                pipeline, resultClass, ChangeStreamLevel.COLLECTION);
    }

    @Override
    public MapReduceIterable<TDocument> mapReduce(final String mapFunction, final String reduceFunction) {
        return mapReduce(mapFunction, reduceFunction, documentClass);
    }

    @Override
    public <TResult> MapReduceIterable<TResult> mapReduce(final String mapFunction, final String reduceFunction,
                                                          final Class<TResult> resultClass) {
        return createMapReduceIterable(null, mapFunction, reduceFunction, resultClass);
    }

    @Override
    public MapReduceIterable<TDocument> mapReduce(final ClientSession clientSession, final String mapFunction,
                                                  final String reduceFunction) {
        return mapReduce(clientSession, mapFunction, reduceFunction, documentClass);
    }

    @Override
    public <TResult> MapReduceIterable<TResult> mapReduce(final ClientSession clientSession, final String mapFunction,
                                                          final String reduceFunction, final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return createMapReduceIterable(clientSession, mapFunction, reduceFunction, resultClass);
    }

    private <TResult> MapReduceIterable<TResult> createMapReduceIterable(@Nullable final ClientSession clientSession,
                                                                         final String mapFunction, final String reduceFunction,
                                                                         final Class<TResult> resultClass) {
        return new MapReduceIterableImpl<TDocument, TResult>(clientSession, namespace, documentClass, resultClass, codecRegistry,
                readPreference, readConcern, writeConcern, executor, mapFunction, reduceFunction);
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

    @SuppressWarnings("unchecked")
    private BulkWriteResult executeBulkWrite(@Nullable final ClientSession clientSession,
                                             final List<? extends WriteModel<? extends TDocument>> requests,
                                             final BulkWriteOptions options) {
        notNull("requests", requests);
        return executor.execute(operations.bulkWrite(requests, options), readConcern, clientSession);
    }

    @Override
    public void insertOne(final TDocument document) {
        insertOne(document, new InsertOneOptions());
    }

    @Override
    public void insertOne(final TDocument document, final InsertOneOptions options) {
        notNull("document", document);
        executeInsertOne(null, document, options);
    }

    @Override
    public void insertOne(final ClientSession clientSession, final TDocument document) {
        insertOne(clientSession, document, new InsertOneOptions());
    }

    @Override
    public void insertOne(final ClientSession clientSession, final TDocument document, final InsertOneOptions options) {
        notNull("clientSession", clientSession);
        notNull("document", document);
        executeInsertOne(clientSession, document, options);
    }

    private void executeInsertOne(@Nullable final ClientSession clientSession, final TDocument document, final InsertOneOptions options) {
        executeSingleWriteRequest(clientSession, operations.insertOne(document, options), INSERT);
    }

    @Override
    public void insertMany(final List<? extends TDocument> documents) {
        insertMany(documents, new InsertManyOptions());
    }

    @Override
    public void insertMany(final List<? extends TDocument> documents, final InsertManyOptions options) {
        executeInsertMany(null, documents, options);
    }

    @Override
    public void insertMany(final ClientSession clientSession, final List<? extends TDocument> documents) {
        insertMany(clientSession, documents, new InsertManyOptions());
    }

    @Override
    public void insertMany(final ClientSession clientSession, final List<? extends TDocument> documents, final InsertManyOptions options) {
        notNull("clientSession", clientSession);
        executeInsertMany(clientSession, documents, options);
    }

    private void executeInsertMany(@Nullable final ClientSession clientSession, final List<? extends TDocument> documents,
                                   final InsertManyOptions options) {
        executor.execute(operations.insertMany(documents, options), readConcern, clientSession);
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
    @Deprecated
    public UpdateResult replaceOne(final Bson filter, final TDocument replacement, final UpdateOptions updateOptions) {
        return replaceOne(filter, replacement, createReplaceOptions(updateOptions));
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
    @Deprecated
    public UpdateResult replaceOne(final ClientSession clientSession, final Bson filter, final TDocument replacement,
                                   final UpdateOptions updateOptions) {
        return replaceOne(clientSession, filter, replacement, createReplaceOptions(updateOptions));
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
    public TDocument findOneAndDelete(final Bson filter) {
        return findOneAndDelete(filter, new FindOneAndDeleteOptions());
    }

    @Override
    public TDocument findOneAndDelete(final Bson filter, final FindOneAndDeleteOptions options) {
        return executeFindOneAndDelete(null, filter, options);
    }

    @Override
    public TDocument findOneAndDelete(final ClientSession clientSession, final Bson filter) {
        return findOneAndDelete(clientSession, filter, new FindOneAndDeleteOptions());
    }

    @Override
    public TDocument findOneAndDelete(final ClientSession clientSession, final Bson filter, final FindOneAndDeleteOptions options) {
        notNull("clientSession", clientSession);
        return executeFindOneAndDelete(clientSession, filter, options);
    }

    private TDocument executeFindOneAndDelete(@Nullable final ClientSession clientSession, final Bson filter,
                                              final FindOneAndDeleteOptions options) {
        return executor.execute(operations.findOneAndDelete(filter, options), readConcern, clientSession);
    }

    @Override
    public TDocument findOneAndReplace(final Bson filter, final TDocument replacement) {
        return findOneAndReplace(filter, replacement, new FindOneAndReplaceOptions());
    }

    @Override
    public TDocument findOneAndReplace(final Bson filter, final TDocument replacement, final FindOneAndReplaceOptions options) {
        return executeFindOneAndReplace(null, filter, replacement, options);
    }

    @Override
    public TDocument findOneAndReplace(final ClientSession clientSession, final Bson filter, final TDocument replacement) {
        return findOneAndReplace(clientSession, filter, replacement, new FindOneAndReplaceOptions());
    }

    @Override
    public TDocument findOneAndReplace(final ClientSession clientSession, final Bson filter, final TDocument replacement,
                                       final FindOneAndReplaceOptions options) {
        notNull("clientSession", clientSession);
        return executeFindOneAndReplace(clientSession, filter, replacement, options);
    }

    private TDocument executeFindOneAndReplace(@Nullable final ClientSession clientSession, final Bson filter, final TDocument replacement,
                                               final FindOneAndReplaceOptions options) {
        return executor.execute(operations.findOneAndReplace(filter, replacement, options), readConcern, clientSession);
    }

    @Override
    public TDocument findOneAndUpdate(final Bson filter, final Bson update) {
        return findOneAndUpdate(filter, update, new FindOneAndUpdateOptions());
    }

    @Override
    public TDocument findOneAndUpdate(final Bson filter, final Bson update, final FindOneAndUpdateOptions options) {
        return executeFindOneAndUpdate(null, filter, update, options);
    }

    @Override
    public TDocument findOneAndUpdate(final ClientSession clientSession, final Bson filter, final Bson update) {
        return findOneAndUpdate(clientSession, filter, update, new FindOneAndUpdateOptions());
    }

    @Override
    public TDocument findOneAndUpdate(final ClientSession clientSession, final Bson filter, final Bson update,
                                      final FindOneAndUpdateOptions options) {
        notNull("clientSession", clientSession);
        return executeFindOneAndUpdate(clientSession, filter, update, options);
    }

    private TDocument executeFindOneAndUpdate(@Nullable final ClientSession clientSession, final Bson filter, final Bson update,
                                              final FindOneAndUpdateOptions options) {
        return executor.execute(operations.findOneAndUpdate(filter, update, options), readConcern, clientSession);
    }

    @Override
    public void drop() {
        executeDrop(null);
    }

    @Override
    public void drop(final ClientSession clientSession) {
        notNull("clientSession", clientSession);
        executeDrop(clientSession);
    }

    private void executeDrop(@Nullable final ClientSession clientSession) {
        executor.execute(operations.dropCollection(), readConcern, clientSession);
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
        executor.execute(operations.createIndexes(indexes, createIndexOptions), readConcern, clientSession);
        return IndexHelper.getIndexNames(indexes, codecRegistry);
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
        return new ListIndexesIterableImpl<TResult>(clientSession, getNamespace(), resultClass, codecRegistry, ReadPreference.primary(),
                executor);
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
                                  final DropIndexOptions dropIndexOptions) {
        notNull("dropIndexOptions", dropIndexOptions);
        executor.execute(operations.dropIndex(indexName, dropIndexOptions), readConcern, clientSession);
    }

    private void executeDropIndex(@Nullable final ClientSession clientSession, final Bson keys, final DropIndexOptions dropIndexOptions) {
        executor.execute(operations.dropIndex(keys, dropIndexOptions), readConcern, clientSession);
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
        executor.execute(new RenameCollectionOperation(getNamespace(), newCollectionNamespace, writeConcern)
                        .dropTarget(renameCollectionOptions.isDropTarget()),
                readConcern, clientSession);
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

    private BulkWriteResult executeSingleWriteRequest(@Nullable final ClientSession clientSession,
                                                      final WriteOperation<BulkWriteResult> writeOperation,
                                                      final WriteRequest.Type type) {
        try {
            return executor.execute(writeOperation, readConcern, clientSession);
        } catch (MongoBulkWriteException e) {
            if (e.getWriteErrors().isEmpty()) {
                throw new MongoWriteConcernException(e.getWriteConcernError(),
                        translateBulkWriteResult(type, e.getWriteResult()),
                        e.getServerAddress());
            } else {
                throw new MongoWriteException(new WriteError(e.getWriteErrors().get(0)), e.getServerAddress());
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

    private UpdateResult toUpdateResult(final com.mongodb.bulk.BulkWriteResult result) {
        if (result.wasAcknowledged()) {
            Long modifiedCount = result.isModifiedCountAvailable() ? (long) result.getModifiedCount() : null;
            BsonValue upsertedId = result.getUpserts().isEmpty() ? null : result.getUpserts().get(0).getId();
            return UpdateResult.acknowledged(result.getMatchedCount(), modifiedCount, upsertedId);
        } else {
            return UpdateResult.unacknowledged();
        }
    }

}
