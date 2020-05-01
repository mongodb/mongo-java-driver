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

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.DropIndexOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.MapReduceAction;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.client.model.CountStrategy;
import com.mongodb.internal.client.model.FindOptions;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.List;

/**
 * This class is NOT part of the public API. It may change at any time without notification.
 */
public final class AsyncOperations<TDocument> {
    private Operations<TDocument> operations;

    public AsyncOperations(final Class<TDocument> documentClass, final ReadPreference readPreference,
                           final CodecRegistry codecRegistry) {
        this(null, documentClass, readPreference, codecRegistry, ReadConcern.DEFAULT, WriteConcern.ACKNOWLEDGED, false, true);
    }

    public AsyncOperations(final Class<TDocument> documentClass, final ReadPreference readPreference,
                           final CodecRegistry codecRegistry, final boolean retryReads) {
        this(null, documentClass, readPreference, codecRegistry, ReadConcern.DEFAULT, WriteConcern.ACKNOWLEDGED, false, retryReads);
    }

    public AsyncOperations(final MongoNamespace namespace, final Class<TDocument> documentClass, final ReadPreference readPreference,
                           final CodecRegistry codecRegistry) {
        this(namespace, documentClass, readPreference, codecRegistry, ReadConcern.DEFAULT, WriteConcern.ACKNOWLEDGED, false, true);
    }

    public AsyncOperations(final MongoNamespace namespace, final Class<TDocument> documentClass, final ReadPreference readPreference,
                           final CodecRegistry codecRegistry, final boolean retryReads) {
        this(namespace, documentClass, readPreference, codecRegistry, ReadConcern.DEFAULT, WriteConcern.ACKNOWLEDGED, false, retryReads);
    }

    public AsyncOperations(final MongoNamespace namespace, final Class<TDocument> documentClass, final ReadPreference readPreference,
                           final CodecRegistry codecRegistry, final WriteConcern writeConcern) {
        this(namespace, documentClass, readPreference, codecRegistry, ReadConcern.DEFAULT, writeConcern, false, true);
    }

    public AsyncOperations(final MongoNamespace namespace, final Class<TDocument> documentClass, final ReadPreference readPreference,
                           final CodecRegistry codecRegistry, final ReadConcern readConcern, final WriteConcern writeConcern) {
        this(namespace, documentClass, readPreference, codecRegistry, readConcern, writeConcern, false, true);
    }

    public AsyncOperations(final MongoNamespace namespace, final Class<TDocument> documentClass, final ReadPreference readPreference,
                           final CodecRegistry codecRegistry, final ReadConcern readConcern, final WriteConcern writeConcern,
                           final boolean retryWrites, final boolean retryReads) {
        this.operations = new Operations<TDocument>(namespace, documentClass, readPreference, codecRegistry, readConcern, writeConcern,
                retryWrites, retryReads);
    }

    public AsyncReadOperation<Long> count(final Bson filter, final CountOptions options, final CountStrategy countStrategy) {
        return operations.count(filter, options, countStrategy);
    }

    public <TResult> AsyncReadOperation<AsyncBatchCursor<TResult>> findFirst(final Bson filter, final Class<TResult> resultClass,
                                                                             final FindOptions options) {
        return operations.findFirst(filter, resultClass, options);
    }

    public <TResult> AsyncReadOperation<AsyncBatchCursor<TResult>> find(final Bson filter, final Class<TResult> resultClass,
                                                                        final FindOptions options) {
        return operations.find(filter, resultClass, options);
    }

    public <TResult> AsyncReadOperation<AsyncBatchCursor<TResult>> find(final MongoNamespace findNamespace, final Bson filter,
                                                                        final Class<TResult> resultClass, final FindOptions options) {
        return operations.find(findNamespace, filter, resultClass, options);
    }

    public <TResult> AsyncReadOperation<AsyncBatchCursor<TResult>> distinct(final String fieldName, final Bson filter,
                                                                            final Class<TResult> resultClass, final long maxTimeMS,
                                                                            final Collation collation) {
        return operations.distinct(fieldName, filter, resultClass, maxTimeMS, collation);
    }

    public <TResult> AsyncReadOperation<AsyncBatchCursor<TResult>> aggregate(final List<? extends Bson> pipeline,
                                                                             final Class<TResult> resultClass,
                                                                             final long maxTimeMS, final long maxAwaitTimeMS,
                                                                             final Integer batchSize, final Collation collation,
                                                                             final Bson hint, final String comment,
                                                                             final Boolean allowDiskUse,
                                                                             final AggregationLevel aggregationLevel) {
        return operations.aggregate(pipeline, resultClass, maxTimeMS, maxAwaitTimeMS, batchSize, collation, hint, comment, allowDiskUse,
                aggregationLevel);
    }

    public AsyncWriteOperation<Void> aggregateToCollection(final List<? extends Bson> pipeline, final long maxTimeMS,
                                                           final Boolean allowDiskUse, final Boolean bypassDocumentValidation,
                                                           final Collation collation, final Bson hint, final String comment,
                                                           final AggregationLevel aggregationLevel) {
        return operations.aggregateToCollection(pipeline, maxTimeMS, allowDiskUse, bypassDocumentValidation, collation, hint, comment,
                aggregationLevel);
    }

    public AsyncWriteOperation<MapReduceStatistics> mapReduceToCollection(final String databaseName, final String collectionName,
                                                                          final String mapFunction, final String reduceFunction,
                                                                          final String finalizeFunction, final Bson filter, final int limit,
                                                                          final long maxTimeMS, final boolean jsMode, final Bson scope,
                                                                          final Bson sort, final boolean verbose,
                                                                          final MapReduceAction action, final boolean nonAtomic,
                                                                          final boolean sharded, final Boolean bypassDocumentValidation,
                                                                          final Collation collation) {
        return operations.mapReduceToCollection(databaseName, collectionName, mapFunction, reduceFunction, finalizeFunction, filter, limit,
                maxTimeMS, jsMode, scope, sort, verbose, action, nonAtomic, sharded, bypassDocumentValidation, collation);
    }

    public <TResult> AsyncReadOperation<MapReduceAsyncBatchCursor<TResult>> mapReduce(final String mapFunction, final String reduceFunction,
                                                                                      final String finalizeFunction,
                                                                                      final Class<TResult> resultClass,
                                                                                      final Bson filter, final int limit,
                                                                                      final long maxTimeMS, final boolean jsMode,
                                                                                      final Bson scope, final Bson sort,
                                                                                      final boolean verbose, final Collation collation) {
        return operations.mapReduce(mapFunction, reduceFunction, finalizeFunction, resultClass, filter, limit, maxTimeMS, jsMode, scope,
                sort, verbose, collation);
    }

    public AsyncWriteOperation<TDocument> findOneAndDelete(final Bson filter, final FindOneAndDeleteOptions options) {
        return operations.findOneAndDelete(filter, options);
    }

    public AsyncWriteOperation<TDocument> findOneAndReplace(final Bson filter, final TDocument replacement,
                                                            final FindOneAndReplaceOptions options) {
        return operations.findOneAndReplace(filter, replacement, options);
    }

    public AsyncWriteOperation<TDocument> findOneAndUpdate(final Bson filter, final Bson update, final FindOneAndUpdateOptions options) {
        return operations.findOneAndUpdate(filter, update, options);
    }

    public AsyncWriteOperation<TDocument> findOneAndUpdate(final Bson filter, final List<? extends Bson> update,
                                                           final FindOneAndUpdateOptions options) {
        return operations.findOneAndUpdate(filter, update, options);
    }

    public AsyncWriteOperation<BulkWriteResult> insertOne(final TDocument document, final InsertOneOptions options) {
        return operations.insertOne(document, options);
    }


    public AsyncWriteOperation<BulkWriteResult> replaceOne(final Bson filter, final TDocument replacement, final ReplaceOptions options) {
        return operations.replaceOne(filter, replacement, options);
    }

    public AsyncWriteOperation<BulkWriteResult> deleteOne(final Bson filter, final DeleteOptions options) {
        return operations.deleteOne(filter, options);
    }

    public AsyncWriteOperation<BulkWriteResult> deleteMany(final Bson filter, final DeleteOptions options) {
        return operations.deleteMany(filter, options);
    }

    public AsyncWriteOperation<BulkWriteResult> updateOne(final Bson filter, final Bson update, final UpdateOptions updateOptions) {
        return operations.updateOne(filter, update, updateOptions);
    }

    public AsyncWriteOperation<BulkWriteResult> updateOne(final Bson filter, final List<? extends Bson> update,
                                                          final UpdateOptions updateOptions) {
        return operations.updateOne(filter, update, updateOptions);
    }

    public AsyncWriteOperation<BulkWriteResult> updateMany(final Bson filter, final Bson update, final UpdateOptions updateOptions) {
        return operations.updateMany(filter, update, updateOptions);
    }

    public AsyncWriteOperation<BulkWriteResult> updateMany(final Bson filter, final List<? extends Bson> update,
                                                           final UpdateOptions updateOptions) {
        return operations.updateMany(filter, update, updateOptions);
    }

    public AsyncWriteOperation<BulkWriteResult> insertMany(final List<? extends TDocument> documents,
                                                           final InsertManyOptions options) {
        return operations.insertMany(documents, options);
    }

    @SuppressWarnings("unchecked")
    public AsyncWriteOperation<BulkWriteResult> bulkWrite(final List<? extends WriteModel<? extends TDocument>> requests,
                                                          final BulkWriteOptions options) {
        return operations.bulkWrite(requests, options);
    }


    public AsyncWriteOperation<Void> dropCollection() {
        return operations.dropCollection();
    }

    public AsyncWriteOperation<Void> renameCollection(final MongoNamespace newCollectionNamespace,
                                                      final RenameCollectionOptions options) {
        return operations.renameCollection(newCollectionNamespace, options);
    }

    public AsyncWriteOperation<Void> createIndexes(final List<IndexModel> indexes, final CreateIndexOptions options) {
        return operations.createIndexes(indexes, options);
    }

    public AsyncWriteOperation<Void> dropIndex(final String indexName, final DropIndexOptions options) {
        return operations.dropIndex(indexName, options);
    }

    public AsyncWriteOperation<Void> dropIndex(final Bson keys, final DropIndexOptions options) {
        return operations.dropIndex(keys, options);
    }

    public <TResult> AsyncReadOperation<AsyncBatchCursor<TResult>> listCollections(final String databaseName,
                                                                                   final Class<TResult> resultClass, final Bson filter,
                                                                                   final boolean collectionNamesOnly,
                                                                                   final Integer batchSize, final long maxTimeMS) {
        return operations.listCollections(databaseName, resultClass, filter, collectionNamesOnly, batchSize, maxTimeMS);
    }

    public <TResult> AsyncReadOperation<AsyncBatchCursor<TResult>> listDatabases(final Class<TResult> resultClass, final Bson filter,
                                                                                 final Boolean nameOnly, final long maxTimeMS,
                                                                                 final Boolean authorizedDatabasesOnly) {
        return operations.listDatabases(resultClass, filter, nameOnly, maxTimeMS, authorizedDatabasesOnly);
    }

    public <TResult> AsyncReadOperation<AsyncBatchCursor<TResult>> listIndexes(final Class<TResult> resultClass, final Integer batchSize,
                                                                               final long maxTimeMS) {
        return operations.listIndexes(resultClass, batchSize, maxTimeMS);
    }
}
