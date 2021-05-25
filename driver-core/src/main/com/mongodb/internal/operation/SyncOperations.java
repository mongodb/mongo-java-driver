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
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.DeleteOptions;
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
import com.mongodb.internal.ClientSideOperationTimeout;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.client.model.FindOptions;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.List;

/**
 * This class is NOT part of the public API. It may change at any time without notification.
 */
public final class SyncOperations<TDocument> {
    private final Operations<TDocument> operations;

    public SyncOperations(final Class<TDocument> documentClass, final ReadPreference readPreference,
                          final CodecRegistry codecRegistry) {
        this(null, documentClass, readPreference, codecRegistry, ReadConcern.DEFAULT, WriteConcern.ACKNOWLEDGED, true, true);
    }

    public SyncOperations(final Class<TDocument> documentClass, final ReadPreference readPreference,
                          final CodecRegistry codecRegistry, final boolean retryReads) {
        this(null, documentClass, readPreference, codecRegistry, ReadConcern.DEFAULT, WriteConcern.ACKNOWLEDGED, true, retryReads);
    }

    public SyncOperations(final MongoNamespace namespace, final Class<TDocument> documentClass, final ReadPreference readPreference,
                          final CodecRegistry codecRegistry) {
        this(namespace, documentClass, readPreference, codecRegistry, ReadConcern.DEFAULT, WriteConcern.ACKNOWLEDGED, true, true);
    }

    public SyncOperations(final MongoNamespace namespace, final Class<TDocument> documentClass, final ReadPreference readPreference,
                          final CodecRegistry codecRegistry, final boolean retryReads) {
        this(namespace, documentClass, readPreference, codecRegistry, ReadConcern.DEFAULT, WriteConcern.ACKNOWLEDGED, true, retryReads);
    }

    public SyncOperations(final MongoNamespace namespace, final Class<TDocument> documentClass, final ReadPreference readPreference,
                          final CodecRegistry codecRegistry, final ReadConcern readConcern, final WriteConcern writeConcern,
                          final boolean retryWrites, final boolean retryReads) {
        this.operations = new Operations<TDocument>(namespace, documentClass, readPreference, codecRegistry, readConcern, writeConcern,
                retryWrites, retryReads);
    }

    public ReadOperation<Long> countDocuments(final ClientSideOperationTimeout clientSideOperationTimeout,
                                              final Bson filter, final CountOptions options) {
        return operations.countDocuments(clientSideOperationTimeout, filter, options);
    }

    public ReadOperation<Long> estimatedDocumentCount(final ClientSideOperationTimeout clientSideOperationTimeout) {
        return operations.estimatedDocumentCount(clientSideOperationTimeout);
    }

    public <TResult> ReadOperation<BatchCursor<TResult>> findFirst(
            final ClientSideOperationTimeout clientSideOperationTimeout, final Bson filter, final Class<TResult> resultClass,
            final FindOptions options) {
        return operations.findFirst(clientSideOperationTimeout, filter, resultClass, options);
    }

    public <TResult> ExplainableReadOperation<BatchCursor<TResult>> find(
            final ClientSideOperationTimeout clientSideOperationTimeout, final Bson filter, final Class<TResult> resultClass,
            final FindOptions options) {
        return operations.find(clientSideOperationTimeout, filter, resultClass, options);
    }

    public <TResult> ReadOperation<BatchCursor<TResult>> find(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                              final MongoNamespace findNamespace, final Bson filter,
                                                              final Class<TResult> resultClass, final FindOptions options) {
        return operations.find(clientSideOperationTimeout, findNamespace, filter, resultClass, options);
    }

    public <TResult> ReadOperation<BatchCursor<TResult>> distinct(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                                  final String fieldName, final Bson filter,
                                                                  final Class<TResult> resultClass, final Collation collation) {
        return operations.distinct(clientSideOperationTimeout, fieldName, filter, resultClass, collation);
    }

    public <TResult> ExplainableReadOperation<BatchCursor<TResult>> aggregate(
            final ClientSideOperationTimeout clientSideOperationTimeout, final List<? extends Bson> pipeline,
            final Class<TResult> resultClass, final Integer batchSize, final Collation collation, final Bson hint,
            final String comment, final Boolean allowDiskUse, final AggregationLevel aggregationLevel) {
        return operations.aggregate(clientSideOperationTimeout, pipeline, resultClass, batchSize, collation, hint, comment,
                allowDiskUse, aggregationLevel);
    }

    public WriteOperation<Void> aggregateToCollection(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                      final List<? extends Bson> pipeline, final Boolean allowDiskUse,
                                                      final Boolean bypassDocumentValidation, final Collation collation, final Bson hint,
                                                      final String comment, final AggregationLevel aggregationLevel) {
        return operations.aggregateToCollection(clientSideOperationTimeout, pipeline, allowDiskUse, bypassDocumentValidation,
                collation, hint, comment, aggregationLevel);
    }

    public WriteOperation<MapReduceStatistics> mapReduceToCollection(
            final ClientSideOperationTimeout clientSideOperationTimeout, final String databaseName,
            final String collectionName, final String mapFunction, final String reduceFunction, final String finalizeFunction,
            final Bson filter, final int limit, final boolean jsMode, final Bson scope, final Bson sort, final boolean verbose,
            final MapReduceAction action, final boolean nonAtomic, final boolean sharded, final Boolean bypassDocumentValidation,
            final Collation collation) {
        return operations.mapReduceToCollection(clientSideOperationTimeout, databaseName, collectionName, mapFunction,
                reduceFunction, finalizeFunction, filter, limit, jsMode, scope, sort, verbose, action, nonAtomic, sharded,
                bypassDocumentValidation, collation);
    }

    public <TResult> ReadOperation<MapReduceBatchCursor<TResult>> mapReduce(
            final ClientSideOperationTimeout clientSideOperationTimeout, final String mapFunction,
            final String reduceFunction, final String finalizeFunction, final Class<TResult> resultClass, final Bson filter,
            final int limit, final boolean jsMode, final Bson scope, final Bson sort, final boolean verbose, final Collation collation) {
        return operations.mapReduce(clientSideOperationTimeout, mapFunction, reduceFunction, finalizeFunction, resultClass,
                filter, limit, jsMode, scope, sort, verbose, collation);
    }

    public WriteOperation<TDocument> findOneAndDelete(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                      final Bson filter, final FindOneAndDeleteOptions options) {
        return operations.findOneAndDelete(clientSideOperationTimeout, filter, options);
    }

    public WriteOperation<TDocument> findOneAndReplace(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                       final Bson filter, final TDocument replacement,
                                                       final FindOneAndReplaceOptions options) {
        return operations.findOneAndReplace(clientSideOperationTimeout, filter, replacement, options);
    }

    public WriteOperation<TDocument> findOneAndUpdate(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                      final Bson filter, final Bson update, final FindOneAndUpdateOptions options) {
        return operations.findOneAndUpdate(clientSideOperationTimeout, filter, update, options);
    }

    public WriteOperation<TDocument> findOneAndUpdate(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                      final Bson filter, final List<? extends Bson> update,
                                                      final FindOneAndUpdateOptions options) {
        return operations.findOneAndUpdate(clientSideOperationTimeout, filter, update, options);
    }

    public WriteOperation<BulkWriteResult> insertOne(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                     final TDocument document, final InsertOneOptions options) {
        return operations.insertOne(clientSideOperationTimeout, document, options);
    }


    public WriteOperation<BulkWriteResult> replaceOne(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                      final Bson filter, final TDocument replacement, final ReplaceOptions options) {
        return operations.replaceOne(clientSideOperationTimeout, filter, replacement, options);
    }

    public WriteOperation<BulkWriteResult> deleteOne(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                     final Bson filter, final DeleteOptions options) {
        return operations.deleteOne(clientSideOperationTimeout, filter, options);
    }

    public WriteOperation<BulkWriteResult> deleteMany(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                      final Bson filter, final DeleteOptions options) {
        return operations.deleteMany(clientSideOperationTimeout, filter, options);
    }

    public WriteOperation<BulkWriteResult> updateOne(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                     final Bson filter, final Bson update, final UpdateOptions updateOptions) {
        return operations.updateOne(clientSideOperationTimeout, filter, update, updateOptions);
    }

    public WriteOperation<BulkWriteResult> updateOne(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                     final Bson filter, final List<? extends Bson> update,
                                                     final UpdateOptions updateOptions) {
        return operations.updateOne(clientSideOperationTimeout, filter, update, updateOptions);
    }

    public WriteOperation<BulkWriteResult> updateMany(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                      final Bson filter, final Bson update, final UpdateOptions updateOptions) {
        return operations.updateMany(clientSideOperationTimeout, filter, update, updateOptions);
    }

    public WriteOperation<BulkWriteResult> updateMany(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                      final Bson filter, final List<? extends Bson> update,
                                                      final UpdateOptions updateOptions) {
        return operations.updateMany(clientSideOperationTimeout, filter, update, updateOptions);
    }

    public WriteOperation<BulkWriteResult> insertMany(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                      final List<? extends TDocument> documents, final InsertManyOptions options) {
        return operations.insertMany(clientSideOperationTimeout, documents, options);
    }

    @SuppressWarnings("unchecked")
    public WriteOperation<BulkWriteResult> bulkWrite(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                     final List<? extends WriteModel<? extends TDocument>> requests,
                                                     final BulkWriteOptions options) {
        return operations.bulkWrite(clientSideOperationTimeout, requests, options);
    }


    public WriteOperation<Void> dropCollection(final ClientSideOperationTimeout clientSideOperationTimeout) {
        return operations.dropCollection(clientSideOperationTimeout);
    }

    public WriteOperation<Void> renameCollection(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                 final MongoNamespace newCollectionNamespace,
                                                 final RenameCollectionOptions options) {
        return operations.renameCollection(clientSideOperationTimeout, newCollectionNamespace, options);
    }

    public WriteOperation<Void> createIndexes(final ClientSideOperationTimeout clientSideOperationTimeout,
                                              final List<IndexModel> indexes, final CreateIndexOptions options) {
        return operations.createIndexes(clientSideOperationTimeout, indexes, options);
    }

    public WriteOperation<Void> dropIndex(final ClientSideOperationTimeout clientSideOperationTimeout,
                                          final String indexName) {
        return operations.dropIndex(clientSideOperationTimeout, indexName);
    }

    public WriteOperation<Void> dropIndex(final ClientSideOperationTimeout clientSideOperationTimeout, final Bson keys) {
        return operations.dropIndex(clientSideOperationTimeout, keys);
    }

    public <TResult> ReadOperation<BatchCursor<TResult>> listCollections(
            final ClientSideOperationTimeout clientSideOperationTimeout, final String databaseName,
            final Class<TResult> resultClass, final Bson filter, final boolean collectionNamesOnly, final Integer batchSize) {
        return operations.listCollections(clientSideOperationTimeout, databaseName, resultClass, filter, collectionNamesOnly,
                batchSize);
    }

    public <TResult> ReadOperation<BatchCursor<TResult>> listDatabases(
            final ClientSideOperationTimeout clientSideOperationTimeout, final Class<TResult> resultClass, final Bson filter,
            final Boolean nameOnly, final Boolean authorizedDatabases) {
        return operations.listDatabases(clientSideOperationTimeout, resultClass, filter, nameOnly, authorizedDatabases);
    }

    public <TResult> ReadOperation<BatchCursor<TResult>> listIndexes(
            final ClientSideOperationTimeout clientSideOperationTimeout, final Class<TResult> resultClass,
            final Integer batchSize) {
        return operations.listIndexes(clientSideOperationTimeout, resultClass, batchSize);
    }
}
