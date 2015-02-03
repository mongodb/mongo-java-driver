/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.DeleteRequest;
import com.mongodb.bulk.InsertRequest;
import com.mongodb.bulk.UpdateRequest;
import com.mongodb.bulk.WriteRequest;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.FindOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.operation.CountOperation;
import com.mongodb.operation.CreateIndexOperation;
import com.mongodb.operation.DropCollectionOperation;
import com.mongodb.operation.DropIndexOperation;
import com.mongodb.operation.FindAndDeleteOperation;
import com.mongodb.operation.FindAndReplaceOperation;
import com.mongodb.operation.FindAndUpdateOperation;
import com.mongodb.operation.MixedBulkWriteOperation;
import com.mongodb.operation.OperationExecutor;
import com.mongodb.operation.RenameCollectionOperation;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.CollectibleCodec;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class MongoCollectionImpl<T> implements MongoCollection<T> {
    private final MongoNamespace namespace;
    private final Class<T> clazz;
    private final ReadPreference readPreference;
    private final CodecRegistry codecRegistry;
    private final WriteConcern writeConcern;
    private final OperationExecutor executor;

    MongoCollectionImpl(final MongoNamespace namespace, final Class<T> clazz, final CodecRegistry codecRegistry,
                        final ReadPreference readPreference, final WriteConcern writeConcern, final OperationExecutor executor) {
        this.namespace = notNull("namespace", namespace);
        this.clazz = notNull("clazz", clazz);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.executor = notNull("executor", executor);
    }

    @Override
    public MongoNamespace getNamespace() {
        return namespace;
    }

    @Override
    public Class<T> getDefaultClass() {
        return clazz;
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
    public <C> MongoCollection<C> withDefaultClass(final Class<C> clazz) {
        return new MongoCollectionImpl<C>(namespace, clazz, codecRegistry, readPreference, writeConcern, executor);
    }

    @Override
    public MongoCollection<T> withCodecRegistry(final CodecRegistry codecRegistry) {
        return new MongoCollectionImpl<T>(namespace, clazz, codecRegistry, readPreference, writeConcern, executor);
    }

    @Override
    public MongoCollection<T> withReadPreference(final ReadPreference readPreference) {
        return new MongoCollectionImpl<T>(namespace, clazz, codecRegistry, readPreference, writeConcern, executor);
    }

    @Override
    public MongoCollection<T> withWriteConcern(final WriteConcern writeConcern) {
        return new MongoCollectionImpl<T>(namespace, clazz, codecRegistry, readPreference, writeConcern, executor);
    }

    @Override
    public long count() {
        return count(new Document(), new CountOptions());
    }

    @Override
    public long count(final Object filter) {
        return count(filter, new CountOptions());
    }

    @Override
    public long count(final Object filter, final CountOptions options) {
        CountOperation operation = new CountOperation(namespace)
                                       .filter(asBson(filter))
                                       .skip(options.getSkip())
                                       .limit(options.getLimit())
                                       .maxTime(options.getMaxTime(MILLISECONDS), MILLISECONDS);
        if (options.getHint() != null) {
            operation.hint(asBson(options.getHint()));
        } else if (options.getHintString() != null) {
            operation.hint(new BsonString(options.getHintString()));
        }
        return executor.execute(operation, readPreference);
    }

    @Override
    public <C> DistinctIterable<C> distinct(final String fieldName, final Class<C> clazz) {
        return new DistinctIterableImpl<C>(namespace, clazz, codecRegistry, readPreference, executor, fieldName);
    }

    @Override
    public FindIterable<T> find() {
        return find(new BsonDocument(), clazz);
    }

    @Override
    public <C> FindIterable<C> find(final Class<C> clazz) {
        return find(new BsonDocument(), clazz);
    }

    @Override
    public FindIterable<T> find(final Object filter) {
        return find(filter, clazz);
    }

    @Override
    public <C> FindIterable<C> find(final Object filter, final Class<C> clazz) {
        return new FindIterableImpl<C>(namespace, clazz, codecRegistry, readPreference, executor, filter, new FindOptions());
    }

    @Override
    public AggregateIterable<Document> aggregate(final List<?> pipeline) {
        return aggregate(pipeline, Document.class);
    }

    @Override
    public <C> AggregateIterable<C> aggregate(final List<?> pipeline, final Class<C> clazz) {
        return new AggregateIterableImpl<C>(namespace, clazz, codecRegistry, readPreference, executor, pipeline);
    }

    @Override
    public MapReduceIterable<Document> mapReduce(final String mapFunction, final String reduceFunction) {
        return mapReduce(mapFunction, reduceFunction, Document.class);
    }

    @Override
    public <C> MapReduceIterable<C> mapReduce(final String mapFunction, final String reduceFunction, final Class<C> clazz) {
        return new MapReduceIterableImpl<C>(namespace, clazz, codecRegistry, readPreference, executor, mapFunction, reduceFunction);
    }

    @Override
    public BulkWriteResult bulkWrite(final List<? extends WriteModel<? extends T>> requests) {
        return bulkWrite(requests, new BulkWriteOptions());
    }

    @Override
    @SuppressWarnings("unchecked")
    public BulkWriteResult bulkWrite(final List<? extends WriteModel<? extends T>> requests, final BulkWriteOptions options) {
        List<WriteRequest> writeRequests = new ArrayList<WriteRequest>(requests.size());
        for (WriteModel<? extends T> writeModel : requests) {
            WriteRequest writeRequest;
            if (writeModel instanceof InsertOneModel) {
                InsertOneModel<T> insertOneModel = (InsertOneModel<T>) writeModel;
                if (getCodec() instanceof CollectibleCodec) {
                    ((CollectibleCodec<T>) getCodec()).generateIdIfAbsentFromDocument(insertOneModel.getDocument());
                }
                writeRequest = new InsertRequest(asBson(insertOneModel.getDocument()));
            } else if (writeModel instanceof ReplaceOneModel) {
                ReplaceOneModel<T> replaceOneModel = (ReplaceOneModel<T>) writeModel;
                writeRequest = new UpdateRequest(asBson(replaceOneModel.getFilter()), asBson(replaceOneModel.getReplacement()),
                                                 WriteRequest.Type.REPLACE)
                                   .upsert(replaceOneModel.getOptions().isUpsert());
            } else if (writeModel instanceof UpdateOneModel) {
                UpdateOneModel<T> updateOneModel = (UpdateOneModel<T>) writeModel;
                writeRequest = new UpdateRequest(asBson(updateOneModel.getFilter()), asBson(updateOneModel.getUpdate()),
                                                 WriteRequest.Type.UPDATE)
                                   .multi(false)
                                   .upsert(updateOneModel.getOptions().isUpsert());
            } else if (writeModel instanceof UpdateManyModel) {
                UpdateManyModel<T> updateManyModel = (UpdateManyModel<T>) writeModel;
                writeRequest = new UpdateRequest(asBson(updateManyModel.getFilter()), asBson(updateManyModel.getUpdate()),
                                                 WriteRequest.Type.UPDATE)
                                   .multi(true)
                                   .upsert(updateManyModel.getOptions().isUpsert());
            } else if (writeModel instanceof DeleteOneModel) {
                DeleteOneModel<T> deleteOneModel = (DeleteOneModel<T>) writeModel;
                writeRequest = new DeleteRequest(asBson(deleteOneModel.getFilter())).multi(false);
            } else if (writeModel instanceof DeleteManyModel) {
                DeleteManyModel<T> deleteManyModel = (DeleteManyModel<T>) writeModel;
                writeRequest = new DeleteRequest(asBson(deleteManyModel.getFilter())).multi(true);
            } else {
                throw new UnsupportedOperationException(format("WriteModel of type %s is not supported", writeModel.getClass()));
            }

            writeRequests.add(writeRequest);
        }

        return executor.execute(new MixedBulkWriteOperation(namespace, writeRequests, options.isOrdered(),
                this.writeConcern));
    }

    @Override
    public void insertOne(final T document) {
        if (getCodec() instanceof CollectibleCodec) {
            ((CollectibleCodec<T>) getCodec()).generateIdIfAbsentFromDocument(document);
        }

        executeSingleWriteRequest(new InsertRequest(asBson(document)));
    }

    @Override
    public void insertMany(final List<? extends T> documents) {
        insertMany(documents, new InsertManyOptions());
    }

    @Override
    public void insertMany(final List<? extends T> documents, final InsertManyOptions options) {
        List<InsertRequest> requests = new ArrayList<InsertRequest>(documents.size());
        for (T document : documents) {
            if (getCodec() instanceof CollectibleCodec) {
                ((CollectibleCodec<T>) getCodec()).generateIdIfAbsentFromDocument(document);
            }
            requests.add(new InsertRequest(asBson(document)));
        }
        executor.execute(new MixedBulkWriteOperation(namespace, requests, options.isOrdered(), this.writeConcern));
    }

    @Override
    public DeleteResult deleteOne(final Object filter) {
        return delete(filter, false);
    }

    @Override
    public DeleteResult deleteMany(final Object filter) {
        return delete(filter, true);
    }

    @Override
    public UpdateResult replaceOne(final Object filter, final T replacement) {
        return replaceOne(filter, replacement, new UpdateOptions());
    }

    @Override
    public UpdateResult replaceOne(final Object filter, final T replacement, final UpdateOptions updateOptions) {
        return toUpdateResult(executeSingleWriteRequest(new UpdateRequest(asBson(filter), asBson(replacement), WriteRequest.Type.REPLACE)
                                                        .upsert(updateOptions.isUpsert())));
    }

    @Override
    public UpdateResult updateOne(final Object filter, final Object update) {
        return updateOne(filter, update, new UpdateOptions());
    }

    @Override
    public UpdateResult updateOne(final Object filter, final Object update, final UpdateOptions updateOptions) {
        return update(filter, update, updateOptions, false);
    }

    @Override
    public UpdateResult updateMany(final Object filter, final Object update) {
        return updateMany(filter, update, new UpdateOptions());
    }

    @Override
    public UpdateResult updateMany(final Object filter, final Object update, final UpdateOptions updateOptions) {
        return update(filter, update, updateOptions, true);
    }

    @Override
    public T findOneAndDelete(final Object filter) {
        return findOneAndDelete(filter, new FindOneAndDeleteOptions());
    }

    @Override
    public T findOneAndDelete(final Object filter, final FindOneAndDeleteOptions options) {
        return executor.execute(new FindAndDeleteOperation<T>(namespace, getCodec())
                .filter(asBson(filter))
                .projection(asBson(options.getProjection()))
                .sort(asBson(options.getSort())));
    }

    @Override
    public T findOneAndReplace(final Object filter, final T replacement) {
        return findOneAndReplace(filter, replacement, new FindOneAndReplaceOptions());
    }

    @Override
    public T findOneAndReplace(final Object filter, final T replacement, final FindOneAndReplaceOptions options) {
        return executor.execute(new FindAndReplaceOperation<T>(namespace, getCodec(), asBson(replacement))
                .filter(asBson(filter))
                .projection(asBson(options.getProjection()))
                .sort(asBson(options.getSort()))
                .returnOriginal(options.getReturnOriginal())
                .upsert(options.isUpsert()));
    }

    @Override
    public T findOneAndUpdate(final Object filter, final Object update) {
        return findOneAndUpdate(filter, update, new FindOneAndUpdateOptions());
    }

    @Override
    public T findOneAndUpdate(final Object filter, final Object update, final FindOneAndUpdateOptions options) {
        return executor.execute(new FindAndUpdateOperation<T>(namespace, getCodec(), asBson(update))
                .filter(asBson(filter))
                .projection(asBson(options.getProjection()))
                .sort(asBson(options.getSort()))
                .returnOriginal(options.getReturnOriginal())
                .upsert(options.isUpsert()));
    }

    @Override
    public void dropCollection() {
        executor.execute(new DropCollectionOperation(namespace));
    }

    @Override
    public void createIndex(final Object key) {
        createIndex(key, new CreateIndexOptions());
    }

    @Override
    public void createIndex(final Object key, final CreateIndexOptions createIndexOptions) {
        executor.execute(new CreateIndexOperation(getNamespace(), asBson(key))
                         .name(createIndexOptions.getName())
                         .background(createIndexOptions.isBackground())
                         .unique(createIndexOptions.isUnique())
                         .sparse(createIndexOptions.isSparse())
                         .expireAfterSeconds(createIndexOptions.getExpireAfterSeconds())
                         .version(createIndexOptions.getVersion())
                         .weights(asBson(createIndexOptions.getWeights()))
                         .defaultLanguage(createIndexOptions.getDefaultLanguage())
                         .languageOverride(createIndexOptions.getLanguageOverride())
                         .textIndexVersion(createIndexOptions.getTextIndexVersion())
                         .twoDSphereIndexVersion(createIndexOptions.getTwoDSphereIndexVersion())
                         .bits(createIndexOptions.getBits())
                         .min(createIndexOptions.getMin())
                         .max(createIndexOptions.getMax())
                         .bucketSize(createIndexOptions.getBucketSize()));
    }

    @Override
    public ListIndexesIterable<Document> listIndexes() {
        return listIndexes(Document.class);
    }

    @Override
    public <C> ListIndexesIterable<C> listIndexes(final Class<C> clazz) {
        return new ListIndexesIterableImpl<C>(getNamespace(), clazz, codecRegistry, ReadPreference.primary(), executor);
    }

    @Override
    public void dropIndex(final String indexName) {
        executor.execute(new DropIndexOperation(namespace, indexName));
    }

    @Override
    public void dropIndexes() {
        dropIndex("*");
    }

    @Override
    public void renameCollection(final MongoNamespace newCollectionNamespace) {
        renameCollection(newCollectionNamespace, new RenameCollectionOptions());
    }

    @Override
    public void renameCollection(final MongoNamespace newCollectionNamespace, final RenameCollectionOptions renameCollectionOptions) {
        executor.execute(new RenameCollectionOperation(getNamespace(), newCollectionNamespace)
                             .dropTarget(renameCollectionOptions.isDropTarget()));
    }

    private DeleteResult delete(final Object filter, final boolean multi) {
        com.mongodb.bulk.BulkWriteResult result = executeSingleWriteRequest(new DeleteRequest(asBson(filter)).multi(multi));
        if (result.wasAcknowledged()) {
            return DeleteResult.acknowledged(result.getDeletedCount());
        } else {
            return DeleteResult.unacknowledged();
        }
    }

    private UpdateResult update(final Object filter, final Object update, final UpdateOptions updateOptions, final boolean multi) {
        return toUpdateResult(executeSingleWriteRequest(new UpdateRequest(asBson(filter), asBson(update), WriteRequest.Type.UPDATE)
                                                        .upsert(updateOptions.isUpsert()).multi(multi)));
    }


    private BulkWriteResult executeSingleWriteRequest(final WriteRequest request) {
        try {
            return executor.execute(new MixedBulkWriteOperation(namespace, asList(request), true, writeConcern));
        } catch (MongoBulkWriteException e) {
            if (e.getWriteErrors().isEmpty()) {
                throw new MongoWriteConcernException(e.getWriteConcernError(), e.getServerAddress());
            } else {
                throw new MongoWriteException(new WriteError(e.getWriteErrors().get(0)), e.getServerAddress());
            }
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

    private Codec<T> getCodec() {
        return getCodec(clazz);
    }

    private <C> Codec<C> getCodec(final Class<C> clazz) {
        return codecRegistry.get(clazz);
    }

    private BsonDocument asBson(final Object document) {
        return BsonDocumentWrapper.asBsonDocument(document, codecRegistry);
    }

}
