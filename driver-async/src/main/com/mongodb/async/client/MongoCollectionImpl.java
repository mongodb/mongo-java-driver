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
import com.mongodb.MongoException;
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
import com.mongodb.client.model.AggregateOptions;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.DistinctOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.FindOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.MapReduceOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.options.OperationOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.operation.AggregateOperation;
import com.mongodb.operation.AggregateToCollectionOperation;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.CountOperation;
import com.mongodb.operation.CreateIndexOperation;
import com.mongodb.operation.DistinctOperation;
import com.mongodb.operation.DropCollectionOperation;
import com.mongodb.operation.DropIndexOperation;
import com.mongodb.operation.FindAndDeleteOperation;
import com.mongodb.operation.FindAndReplaceOperation;
import com.mongodb.operation.FindAndUpdateOperation;
import com.mongodb.operation.ListIndexesOperation;
import com.mongodb.operation.MapReduceStatistics;
import com.mongodb.operation.MapReduceToCollectionOperation;
import com.mongodb.operation.MapReduceWithInlineResultsOperation;
import com.mongodb.operation.MixedBulkWriteOperation;
import com.mongodb.operation.RenameCollectionOperation;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonJavaScript;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.CollectibleCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class MongoCollectionImpl<T> implements MongoCollection<T> {
    private final MongoNamespace namespace;
    private final OperationOptions options;
    private final Class<T> clazz;
    private final AsyncOperationExecutor executor;

    MongoCollectionImpl(final MongoNamespace namespace, final Class<T> clazz,
                        final OperationOptions options, final AsyncOperationExecutor executor) {
        this.namespace = notNull("namespace", namespace);
        this.clazz = notNull("clazz", clazz);
        this.options = notNull("options", options);
        this.executor = notNull("executor", executor);
    }

    @Override
    public MongoNamespace getNamespace() {
        return namespace;
    }

    @Override
    public OperationOptions getOptions() {
        return options;
    }

    @Override
    public MongoCollection<T> withReadPreference(final ReadPreference readPreference) {
        return new MongoCollectionImpl<T>(namespace, clazz, options.withReadPreference(readPreference), executor);
    }

    @Override
    public MongoCollection<T> withWriteConcern(final WriteConcern writeConcern) {
        return new MongoCollectionImpl<T>(namespace, clazz, options.withWriteConcern(writeConcern), executor);
    }

    @Override
    public MongoCollection<T> withCodecRegistry(final CodecRegistry codecRegistry) {
        return new MongoCollectionImpl<T>(namespace, clazz, options.withCodecRegistry(codecRegistry), executor);
    }

    @Override
    public <C> MongoCollection<C> withClazz(final Class<C> clazz) {
        return new MongoCollectionImpl<C>(namespace, clazz, options, executor);
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
        executor.execute(operation, this.options.getReadPreference(), callback);
    }

    @Override
    public void distinct(final String fieldName, final Object filter, final SingleResultCallback<List<Object>> callback) {
        distinct(fieldName, filter, new DistinctOptions(), callback);
    }

    @Override
    public void distinct(final String fieldName, final Object filter, final DistinctOptions options,
                         final SingleResultCallback<List<Object>> callback) {
        DistinctOperation operation = new DistinctOperation(namespace, fieldName)
                                      .filter(asBson(filter))
                                      .maxTime(options.getMaxTime(MILLISECONDS), MILLISECONDS);
        executor.execute(operation, this.options.getReadPreference(), errorHandlingCallback(new SingleResultCallback<BsonArray>() {
            @Override
            public void onResult(final BsonArray result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, MongoException.fromThrowable(t));
                } else {
                    try {
                        List<Object> distinctList = new ArrayList<Object>();
                        for (BsonValue value : result) {
                            BsonDocument bsonDocument = new BsonDocument("value", value);
                            Document document = getOptions().getCodecRegistry().get(Document.class)
                                                            .decode(new BsonDocumentReader(bsonDocument),
                                                                    DecoderContext.builder().build());
                            distinctList.add(document.get("value"));
                        }
                        callback.onResult(distinctList, null);
                    } catch (Throwable tr) {
                        callback.onResult(null, new MongoException("Error when decoding distinct results", tr));
                    }
                }
            }
        }));
    }

    @Override
    public FindFluent<T> find() {
        return find(new BsonDocument(), clazz);
    }

    @Override
    public <C> FindFluent<C> find(final Class<C> clazz) {
        return find(new BsonDocument(), clazz);
    }

    @Override
    public FindFluent<T> find(final Object filter) {
        return find(filter, clazz);
    }

    @Override
    public <C> FindFluent<C> find(final Object filter, final Class<C> clazz) {
        return new FindFluentImpl<C>(namespace, options, executor, filter, new FindOptions(), clazz);
    }

    @Override
    public MongoIterable<Document> aggregate(final List<?> pipeline) {
        return aggregate(pipeline, new AggregateOptions(), Document.class);
    }

    @Override
    public <C> MongoIterable<C> aggregate(final List<?> pipeline, final Class<C> clazz) {
        return aggregate(pipeline, new AggregateOptions(), clazz);
    }

    @Override
    public MongoIterable<Document> aggregate(final List<?> pipeline, final AggregateOptions options) {
        return aggregate(pipeline, options, Document.class);
    }

    @Override
    public <C> MongoIterable<C> aggregate(final List<?> pipeline, final AggregateOptions options, final Class<C> clazz) {
        List<BsonDocument> aggregateList = createBsonDocumentList(pipeline);
        BsonValue outCollection = getAggregateOutCollection(aggregateList);

        if (outCollection != null) {
            AggregateToCollectionOperation operation = new AggregateToCollectionOperation(namespace, aggregateList)
                                                       .maxTime(options.getMaxTime(MILLISECONDS), MILLISECONDS)
                                                       .allowDiskUse(options.getAllowDiskUse());
            MongoIterable<C> delegated = new FindFluentImpl<C>(new MongoNamespace(namespace.getDatabaseName(),
                                                                                  outCollection.asString().getValue()),
                                                               getOptionsWithPrimaryReadPreference(), executor, new BsonDocument(),
                                                               new FindOptions(), clazz);
            return new AwaitingWriteOperationIterable<C, Void>(operation, executor, delegated);
        } else {
            return new OperationIterable<C>(new AggregateOperation<C>(namespace, aggregateList, getCodec(clazz))
                                            .maxTime(options.getMaxTime(MILLISECONDS), MILLISECONDS)
                                            .allowDiskUse(options.getAllowDiskUse())
                                            .batchSize(options.getBatchSize())
                                            .useCursor(options.getUseCursor()),
                                            this.options.getReadPreference(),
                                            executor);
        }
    }

    @Override
    public void aggregateToCollection(final List<?> pipeline, final SingleResultCallback<Void> callback) {
        aggregateToCollection(pipeline, new AggregateOptions(), callback);
    }

    @Override
    public void aggregateToCollection(final List<?> pipeline, final AggregateOptions options, final SingleResultCallback<Void> callback) {
        List<BsonDocument> aggregateList = createBsonDocumentList(pipeline);
        BsonValue outCollection = getAggregateOutCollection(aggregateList);

        if (outCollection == null) {
            throw new IllegalArgumentException("The last stage of the aggregation pipeline must be $out");
        }

        executor.execute(new AggregateToCollectionOperation(namespace, aggregateList)
                         .maxTime(options.getMaxTime(MILLISECONDS), MILLISECONDS)
                         .allowDiskUse(options.getAllowDiskUse()), callback);
    }

    private BsonValue getAggregateOutCollection(final List<BsonDocument> aggregateList) {
        return aggregateList.size() == 0 ? null : aggregateList.get(aggregateList.size() - 1).get("$out");
    }

    @Override
    public MongoIterable<Document> mapReduce(final String mapFunction, final String reduceFunction) {
        return mapReduce(mapFunction, reduceFunction, new MapReduceOptions());
    }

    @Override
    public MongoIterable<Document> mapReduce(final String mapFunction, final String reduceFunction, final MapReduceOptions options) {
        return mapReduce(mapFunction, reduceFunction, options, Document.class);
    }

    @Override
    public <C> MongoIterable<C> mapReduce(final String mapFunction, final String reduceFunction, final Class<C> clazz) {
        return mapReduce(mapFunction, reduceFunction, new MapReduceOptions(), clazz);
    }

    @Override
    public <C> MongoIterable<C> mapReduce(final String mapFunction, final String reduceFunction, final MapReduceOptions options,
                                          final Class<C> clazz) {
        if (options.isInline()) {
            MapReduceWithInlineResultsOperation<C> operation =
            new MapReduceWithInlineResultsOperation<C>(getNamespace(),
                                                       new BsonJavaScript(mapFunction),
                                                       new BsonJavaScript(reduceFunction),
                                                       getCodec(clazz))
            .filter(asBson(options.getFilter()))
            .limit(options.getLimit())
            .maxTime(options.getMaxTime(MILLISECONDS), MILLISECONDS)
            .jsMode(options.isJsMode())
            .scope(asBson(options.getScope()))
            .sort(asBson(options.getSort()))
            .verbose(options.isVerbose());
            if (options.getFinalizeFunction() != null) {
                operation.finalizeFunction(new BsonJavaScript(options.getFinalizeFunction()));
            }
            return new OperationIterable<C>(operation, this.options.getReadPreference(), executor);
        } else {
            MapReduceToCollectionOperation operation = createMapReduceToCollectionOperation(mapFunction, reduceFunction, options);

            String databaseName = options.getDatabaseName() != null ? options.getDatabaseName() : namespace.getDatabaseName();
            MongoIterable<C> delegated = new FindFluentImpl<C>(new MongoNamespace(databaseName, options.getCollectionName()),
                                                               getOptionsWithPrimaryReadPreference(), executor,
                                                               new BsonDocument(), new FindOptions(), clazz);
            return new AwaitingWriteOperationIterable<C, MapReduceStatistics>(operation, executor, delegated);
        }
    }

    @Override
    public void mapReduceToCollection(final String mapFunction, final String reduceFunction, final MapReduceOptions options,
                                      final SingleResultCallback<Void> callback) {

        if (options.isInline()) {
            throw new IllegalArgumentException("The options must specify a non-inline result");
        }

        executor.execute(createMapReduceToCollectionOperation(mapFunction, reduceFunction, options),
                         new SingleResultCallback<MapReduceStatistics>() {
                             @Override
                             public void onResult(final MapReduceStatistics result, final Throwable t) {
                                 callback.onResult(null, t);
                             }
                         });
    }

    private MapReduceToCollectionOperation createMapReduceToCollectionOperation(final String mapFunction, final String reduceFunction,
                                                                                final MapReduceOptions options) {
        MapReduceToCollectionOperation operation =
        new MapReduceToCollectionOperation(getNamespace(),
                                           new BsonJavaScript(mapFunction),
                                           new BsonJavaScript(reduceFunction),
                                           options.getCollectionName())
        .filter(asBson(options.getFilter()))
        .limit(options.getLimit())
        .maxTime(options.getMaxTime(MILLISECONDS), MILLISECONDS)
        .jsMode(options.isJsMode())
        .scope(asBson(options.getScope()))
        .sort(asBson(options.getSort()))
        .verbose(options.isVerbose())
        .action(options.getAction().getValue())
        .nonAtomic(options.isNonAtomic())
        .sharded(options.isSharded())
        .databaseName(options.getDatabaseName());

        if (options.getFinalizeFunction() != null) {
            operation.finalizeFunction(new BsonJavaScript(options.getFinalizeFunction()));
        }
        return operation;
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

        executor.execute(new MixedBulkWriteOperation(namespace, writeRequests, options.isOrdered(), this.options.getWriteConcern()),
                         callback);
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
        executor.execute(new MixedBulkWriteOperation(namespace, requests, options.isOrdered(), this.options.getWriteConcern()),
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
    public void getIndexes(final SingleResultCallback<List<Document>> callback) {
        getIndexes(Document.class, callback);
    }

    @Override
    public <C> void getIndexes(final Class<C> clazz, final SingleResultCallback<List<C>> callback) {
        new OperationIterable<C>(new ListIndexesOperation<C>(namespace, getCodec(clazz)), this.options.getReadPreference(), executor)
        .into(new ArrayList<C>(), callback);
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
        executor.execute(new MixedBulkWriteOperation(namespace, asList(request), true, options.getWriteConcern()),
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
        return options.getCodecRegistry().get(clazz);
    }

    private BsonDocument asBson(final Object document) {
        return BsonDocumentWrapper.asBsonDocument(document, options.getCodecRegistry());
    }

    private <D> List<BsonDocument> createBsonDocumentList(final List<D> pipeline) {
        List<BsonDocument> aggregateList = new ArrayList<BsonDocument>(pipeline.size());
        for (D obj : pipeline) {
            aggregateList.add(asBson(obj));
        }
        return aggregateList;
    }

    private OperationOptions getOptionsWithPrimaryReadPreference() {
        return OperationOptions.builder().readPreference(primary()).build().withDefaults(this.options);
    }
}
