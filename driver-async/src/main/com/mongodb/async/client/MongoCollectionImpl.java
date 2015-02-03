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

package com.mongodb.async.client;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.MongoWriteException;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.WriteError;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.DeleteRequest;
import com.mongodb.bulk.InsertRequest;
import com.mongodb.bulk.UpdateRequest;
import com.mongodb.bulk.WriteRequest;
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
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.CountOperation;
import com.mongodb.operation.CreateIndexOperation;
import com.mongodb.operation.DropCollectionOperation;
import com.mongodb.operation.DropIndexOperation;
import com.mongodb.operation.FindAndDeleteOperation;
import com.mongodb.operation.FindAndReplaceOperation;
import com.mongodb.operation.FindAndUpdateOperation;
import com.mongodb.operation.MixedBulkWriteOperation;
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
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class MongoCollectionImpl<T> implements MongoCollection<T> {
    private final MongoNamespace namespace;
    private final Class<T> clazz;
    private final ReadPreference readPreference;
    private final CodecRegistry codecRegistry;
    private final WriteConcern writeConcern;
    private final AsyncOperationExecutor executor;

    MongoCollectionImpl(final MongoNamespace namespace, final Class<T> clazz, final CodecRegistry codecRegistry,
                        final ReadPreference readPreference, final WriteConcern writeConcern, final AsyncOperationExecutor executor) {
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
    public void count(final SingleResultCallback<Long> callback) {
        count(new BsonDocument(), new CountOptions(), callback);
    }

    @Override
    public void count(final Object filter, final SingleResultCallback<Long> callback) {
        count(filter, new CountOptions(), callback);
    }

    @Override
    public void count(final Object filter, final CountOptions options, final SingleResultCallback<Long> callback) {
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
        executor.execute(operation, readPreference, callback);
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
    public void bulkWrite(final List<? extends WriteModel<? extends T>> requests, final SingleResultCallback<BulkWriteResult> callback) {
        bulkWrite(requests, new BulkWriteOptions(), callback);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void bulkWrite(final List<? extends WriteModel<? extends T>> requests, final BulkWriteOptions options,
                          final SingleResultCallback<BulkWriteResult> callback) {
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

        executor.execute(new MixedBulkWriteOperation(namespace, writeRequests, options.isOrdered(), writeConcern), callback);
    }

    @Override
    public void insertOne(final T document, final SingleResultCallback<Void> callback) {
        if (getCodec() instanceof CollectibleCodec) {
            ((CollectibleCodec<T>) getCodec()).generateIdIfAbsentFromDocument(document);
        }
        executeSingleWriteRequest(new InsertRequest(asBson(document)), new SingleResultCallback<BulkWriteResult>() {
            @Override
            public void onResult(final BulkWriteResult result, final Throwable t) {
                callback.onResult(null, t);
            }
        });
    }

    @Override
    public void insertMany(final List<? extends T> documents, final SingleResultCallback<Void> callback) {
        insertMany(documents, new InsertManyOptions(), callback);
    }

    @Override
    public void insertMany(final List<? extends T> documents, final InsertManyOptions options,
                           final SingleResultCallback<Void> callback) {
        List<InsertRequest> requests = new ArrayList<InsertRequest>(documents.size());
        for (T document : documents) {
            if (getCodec() instanceof CollectibleCodec) {
                ((CollectibleCodec<T>) getCodec()).generateIdIfAbsentFromDocument(document);
            }
            requests.add(new InsertRequest(asBson(document)));
        }
        executor.execute(new MixedBulkWriteOperation(namespace, requests, options.isOrdered(), writeConcern),
                         errorHandlingCallback(new SingleResultCallback<BulkWriteResult>() {
                             @Override
                             public void onResult(final BulkWriteResult result, final Throwable t) {
                                 callback.onResult(null, t);
                             }
                         }));
    }

    @Override
    public void deleteOne(final Object filter, final SingleResultCallback<DeleteResult> callback) {
        delete(filter, false, callback);
    }

    @Override
    public void deleteMany(final Object filter, final SingleResultCallback<DeleteResult> callback) {
        delete(filter, true, callback);
    }

    @Override
    public void replaceOne(final Object filter, final T replacement, final SingleResultCallback<UpdateResult> callback) {
        replaceOne(filter, replacement, new UpdateOptions(), callback);
    }

    @Override
    public void replaceOne(final Object filter, final T replacement, final UpdateOptions options,
                           final SingleResultCallback<UpdateResult> callback) {
        executeSingleWriteRequest(new UpdateRequest(asBson(filter), asBson(replacement), WriteRequest.Type.REPLACE)
                                  .upsert(options.isUpsert()),
                                  new SingleResultCallback<BulkWriteResult>() {
                                      @Override
                                      public void onResult(final BulkWriteResult result, final Throwable t) {
                                          if (t != null) {
                                              callback.onResult(null, t);
                                          } else {
                                              callback.onResult(toUpdateResult(result), null);
                                          }
                                      }
                                  });
    }

    @Override
    public void updateOne(final Object filter, final Object update, final SingleResultCallback<UpdateResult> callback) {
        updateOne(filter, update, new UpdateOptions(), callback);
    }

    @Override
    public void updateOne(final Object filter, final Object update, final UpdateOptions options,
                          final SingleResultCallback<UpdateResult> callback) {
        update(filter, update, options, false, callback);
    }

    @Override
    public void updateMany(final Object filter, final Object update, final SingleResultCallback<UpdateResult> callback) {
        updateMany(filter, update, new UpdateOptions(), callback);
    }

    @Override
    public void updateMany(final Object filter, final Object update, final UpdateOptions options,
                           final SingleResultCallback<UpdateResult> callback) {
        update(filter, update, options, true, callback);
    }

    @Override
    public void findOneAndDelete(final Object filter, final SingleResultCallback<T> callback) {
        findOneAndDelete(filter, new FindOneAndDeleteOptions(), callback);
    }

    @Override
    public void findOneAndDelete(final Object filter, final FindOneAndDeleteOptions options, final SingleResultCallback<T> callback) {
        executor.execute(new FindAndDeleteOperation<T>(namespace, getCodec())
                         .filter(asBson(filter))
                         .projection(asBson(options.getProjection()))
                         .sort(asBson(options.getSort())), callback);
    }

    @Override
    public void findOneAndReplace(final Object filter, final T replacement, final SingleResultCallback<T> callback) {
        findOneAndReplace(filter, replacement, new FindOneAndReplaceOptions(), callback);
    }

    @Override
    public void findOneAndReplace(final Object filter, final T replacement, final FindOneAndReplaceOptions options,
                                  final SingleResultCallback<T> callback) {
        executor.execute(new FindAndReplaceOperation<T>(namespace, getCodec(), asBson(replacement))
                         .filter(asBson(filter))
                         .projection(asBson(options.getProjection()))
                         .sort(asBson(options.getSort()))
                         .returnOriginal(options.getReturnOriginal())
                         .upsert(options.isUpsert()), callback);
    }

    @Override
    public void findOneAndUpdate(final Object filter, final Object update, final SingleResultCallback<T> callback) {
        findOneAndUpdate(filter, update, new FindOneAndUpdateOptions(), callback);
    }

    @Override
    public void findOneAndUpdate(final Object filter, final Object update, final FindOneAndUpdateOptions options,
                                 final SingleResultCallback<T> callback) {
        executor.execute(new FindAndUpdateOperation<T>(namespace, getCodec(), asBson(update))
                         .filter(asBson(filter))
                         .projection(asBson(options.getProjection()))
                         .sort(asBson(options.getSort()))
                         .returnOriginal(options.getReturnOriginal())
                         .upsert(options.isUpsert()), callback);
    }

    @Override
    public void dropCollection(final SingleResultCallback<Void> callback) {
        executor.execute(new DropCollectionOperation(namespace), callback);
    }

    @Override
    public void createIndex(final Object key, final SingleResultCallback<Void> callback) {
        createIndex(key, new CreateIndexOptions(), callback);
    }

    @Override
    public void createIndex(final Object key, final CreateIndexOptions options, final SingleResultCallback<Void> callback) {
        executor.execute(new CreateIndexOperation(getNamespace(), asBson(key))
                         .name(options.getName())
                         .background(options.isBackground())
                         .unique(options.isUnique())
                         .sparse(options.isSparse())
                         .expireAfterSeconds(options.getExpireAfterSeconds())
                         .version(options.getVersion())
                         .weights(asBson(options.getWeights()))
                         .defaultLanguage(options.getDefaultLanguage())
                         .languageOverride(options.getLanguageOverride())
                         .textIndexVersion(options.getTextIndexVersion())
                         .twoDSphereIndexVersion(options.getTwoDSphereIndexVersion())
                         .bits(options.getBits())
                         .min(options.getMin())
                         .max(options.getMax())
                         .bucketSize(options.getBucketSize()), callback);
    }

    @Override
    public ListIndexesIterable<Document> listIndexes() {
        return listIndexes(Document.class);
    }

    @Override
    public <C> ListIndexesIterable<C> listIndexes(final Class<C> clazz) {
        return new ListIndexesIterableImpl<C>(namespace, clazz, codecRegistry, readPreference, executor);
    }

    @Override
    public void dropIndex(final String indexName, final SingleResultCallback<Void> callback) {
        executor.execute(new DropIndexOperation(namespace, indexName), callback);
    }

    @Override
    public void dropIndexes(final SingleResultCallback<Void> callback) {
        dropIndex("*", callback);
    }

    @Override
    public void renameCollection(final MongoNamespace newCollectionNamespace, final SingleResultCallback<Void> callback) {
        renameCollection(newCollectionNamespace, new RenameCollectionOptions(), callback);
    }

    @Override
    public void renameCollection(final MongoNamespace newCollectionNamespace, final RenameCollectionOptions options,
                                 final SingleResultCallback<Void> callback) {
        executor.execute(new RenameCollectionOperation(getNamespace(), newCollectionNamespace)
                         .dropTarget(options.isDropTarget()), callback);
    }

    private void delete(final Object filter, final boolean multi, final SingleResultCallback<DeleteResult> callback) {
        executeSingleWriteRequest(new DeleteRequest(asBson(filter)).multi(multi), new SingleResultCallback<BulkWriteResult>() {
            @Override
            public void onResult(final BulkWriteResult result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    if (result.wasAcknowledged()) {
                        callback.onResult(DeleteResult.acknowledged(result.getDeletedCount()), null);
                    } else {
                        callback.onResult(DeleteResult.unacknowledged(), null);
                    }

                }
            }
        });
    }

    private void update(final Object filter, final Object update, final UpdateOptions updateOptions, final boolean multi,
                        final SingleResultCallback<UpdateResult> callback) {
        executeSingleWriteRequest(new UpdateRequest(asBson(filter), asBson(update), WriteRequest.Type.UPDATE)
                                  .upsert(updateOptions.isUpsert()).multi(multi), new SingleResultCallback<BulkWriteResult>() {
            @Override
            public void onResult(final BulkWriteResult result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    callback.onResult(toUpdateResult(result), null);
                }
            }
        });
    }

    private void executeSingleWriteRequest(final WriteRequest request, final SingleResultCallback<BulkWriteResult> callback) {
        executor.execute(new MixedBulkWriteOperation(namespace, asList(request), true, writeConcern),
                         new SingleResultCallback<BulkWriteResult>() {
                             @Override
                             public void onResult(final BulkWriteResult result, final Throwable t) {
                                 if (t instanceof MongoBulkWriteException) {
                                     MongoBulkWriteException e = (MongoBulkWriteException) t;
                                     if (e.getWriteErrors().isEmpty()) {
                                         callback.onResult(null, new MongoWriteConcernException(e.getWriteConcernError(),
                                                                                                e.getServerAddress()));
                                     } else {
                                         callback.onResult(null, new MongoWriteException(new WriteError(e.getWriteErrors().get(0)),
                                                                                         e.getServerAddress()));
                                     }
                                 } else {
                                     callback.onResult(result, t);
                                 }
                             }
                         });
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
