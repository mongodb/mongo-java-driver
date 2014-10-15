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

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCollectionOptions;
import com.mongodb.client.MongoIterable;
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
import com.mongodb.client.model.ParallelCollectionScanOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.codecs.CollectibleCodec;
import com.mongodb.operation.AggregateOperation;
import com.mongodb.operation.AggregateToCollectionOperation;
import com.mongodb.operation.CountOperation;
import com.mongodb.operation.CreateIndexOperation;
import com.mongodb.operation.DeleteOperation;
import com.mongodb.operation.DeleteRequest;
import com.mongodb.operation.DistinctOperation;
import com.mongodb.operation.DropCollectionOperation;
import com.mongodb.operation.DropIndexOperation;
import com.mongodb.operation.FindAndDeleteOperation;
import com.mongodb.operation.FindAndReplaceOperation;
import com.mongodb.operation.FindAndUpdateOperation;
import com.mongodb.operation.FindOperation;
import com.mongodb.operation.InsertOperation;
import com.mongodb.operation.InsertRequest;
import com.mongodb.operation.ListIndexesOperation;
import com.mongodb.operation.MapReduceToCollectionOperation;
import com.mongodb.operation.MapReduceWithInlineResultsOperation;
import com.mongodb.operation.MixedBulkWriteOperation;
import com.mongodb.operation.OperationExecutor;
import com.mongodb.operation.ParallelCollectionScanOperation;
import com.mongodb.operation.ReadOperation;
import com.mongodb.operation.RenameCollectionOperation;
import com.mongodb.operation.UpdateOperation;
import com.mongodb.operation.UpdateRequest;
import com.mongodb.operation.WriteRequest;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonJavaScript;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;
import org.mongodb.BulkWriteResult;
import org.mongodb.Document;
import org.mongodb.WriteResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class MongoCollectionImpl<T> implements MongoCollection<T> {
    private final MongoNamespace namespace;
    private final MongoCollectionOptions options;
    private final Class<T> clazz;
    private final OperationExecutor executor;

    MongoCollectionImpl(final MongoNamespace namespace, final Class<T> clazz,
                        final MongoCollectionOptions options, final OperationExecutor executor) {
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
    public MongoCollectionOptions getOptions() {
        return options;
    }

    @Override
    public long count() {
        return count(new Document(), new CountOptions());
    }

    @Override
    public long count(final Object criteria) {
        return count(criteria, new CountOptions());
    }

    @Override
    public long count(final Object criteria, final CountOptions options) {
        CountOperation operation = new CountOperation(namespace)
                                       .criteria(asBson(criteria))
                                       .skip(options.getSkip())
                                       .limit(options.getLimit())
                                       .maxTime(options.getMaxTime(MILLISECONDS), MILLISECONDS);
        if (options.getHint() != null) {
            operation.hint(asBson(options.getHint()));
        } else if (options.getHintString() != null) {
            operation.hint(new BsonString(options.getHintString()));
        }
        return executor.execute(operation, this.options.getReadPreference());
    }

    @Override
    public List<Object> distinct(final String fieldName) {
        return distinct(fieldName, new DistinctOptions());
    }

    @Override
    public List<Object> distinct(final String fieldName, final DistinctOptions distinctOptions) {
        DistinctOperation operation = new DistinctOperation(namespace, fieldName)
                                          .criteria(asBson(distinctOptions.getCriteria()))
                                          .maxTime(distinctOptions.getMaxTime(MILLISECONDS), MILLISECONDS);
        BsonArray distinctArray = executor.execute(operation, options.getReadPreference());
        List<Object> distinctList = new ArrayList<Object>();
        for (BsonValue value : distinctArray) {
            BsonDocument bsonDocument = new BsonDocument("value", value);
            Document document = options.getCodecRegistry().get(Document.class).decode(new BsonDocumentReader(bsonDocument),
                                                                                      DecoderContext.builder().build());
            distinctList.add(document.get("value"));
        }

        return distinctList;
    }

    @Override
    public MongoIterable<T> find() {
        return find(new Document(), new FindOptions());
    }

    @Override
    public <C> MongoIterable<C> find(final Class<C> clazz) {
        return find(new Document(), new FindOptions(), clazz);
    }

    @Override
    public MongoIterable<T> find(final Object criteria) {
        return find(criteria, new FindOptions());
    }

    @Override
    public <C> MongoIterable<C> find(final Object criteria, final Class<C> clazz) {
        return find(criteria, new FindOptions(), clazz);
    }

    @Override
    public MongoIterable<T> find(final Object criteria, final FindOptions findOptions) {
        return find(criteria, findOptions, clazz);
    }

    @Override
    public <C> MongoIterable<C> find(final Object criteria, final FindOptions findOptions, final Class<C> clazz) {
        return new OperationIterable<C>(createQueryOperation(namespace, criteria, findOptions, getCodec(clazz)),
                                        options.getReadPreference());
    }

    @Override
    public MongoIterable<Document> aggregate(final List<?> pipeline) {
        return aggregate(pipeline, Document.class);
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

        BsonValue outCollection = aggregateList.size() == 0 ? null : aggregateList.get(aggregateList.size() - 1).get("$out");

        if (outCollection != null) {
            AggregateToCollectionOperation operation = new AggregateToCollectionOperation(namespace, aggregateList)
                                                           .maxTime(options.getMaxTime(MILLISECONDS), MILLISECONDS)
                                                           .allowDiskUse(options.getAllowDiskUse());
            executor.execute(operation);
            return new OperationIterable<C>(new FindOperation<C>(new MongoNamespace(namespace.getDatabaseName(),
                                                                                    outCollection.asString().getValue()),
                                                                 getCodec(clazz)),
                                            this.options.getReadPreference());
        } else {
            return new OperationIterable<C>(new AggregateOperation<C>(namespace, aggregateList, getCodec(clazz))
                                            .maxTime(options.getMaxTime(MILLISECONDS), MILLISECONDS)
                                            .allowDiskUse(options.getAllowDiskUse())
                                            .batchSize(options.getBatchSize())
                                            .useCursor(options.getUseCursor()),
                                            this.options.getReadPreference());
        }
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
                    .criteria(asBson(options.getCriteria()))
                    .limit(options.getLimit())
                    .maxTime(options.getMaxTime(MILLISECONDS), MILLISECONDS)
                    .jsMode(options.isJsMode())
                    .scope(asBson(options.getScope()))
                    .sort(asBson(options.getSort()))
                    .verbose(options.isVerbose());
            if (options.getFinalizeFunction() != null) {
                operation.finalizeFunction(new BsonJavaScript(options.getFinalizeFunction()));
            }
            return new MapReduceResultsIterable<C>(operation, this.options.getReadPreference(), executor);
        } else {
            MapReduceToCollectionOperation operation =
                new MapReduceToCollectionOperation(getNamespace(),
                                                   new BsonJavaScript(mapFunction),
                                                   new BsonJavaScript(reduceFunction),
                                                   options.getCollectionName())
                    .criteria(asBson(options.getCriteria()))
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
            executor.execute(operation);

            String databaseName = options.getDatabaseName() != null ? options.getDatabaseName() : namespace.getDatabaseName();
            FindOperation<C> findOperation = createQueryOperation(new MongoNamespace(databaseName, options.getCollectionName()),
                                                                  new Document(), new FindOptions(), getCodec(clazz));
            return new OperationIterable<C>(findOperation, this.options.getReadPreference());
        }
    }

    @Override
    public BulkWriteResult bulkWrite(final List<? extends WriteModel<? extends T>> requests) {
        return bulkWrite(requests, new BulkWriteOptions());
    }

    @Override
    @SuppressWarnings("unchecked")
    public BulkWriteResult bulkWrite(final List<? extends WriteModel<? extends T>> requests, final BulkWriteOptions options) {
        List<WriteRequest> writeRequests = new ArrayList<WriteRequest>();
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
                writeRequest = new UpdateRequest(asBson(replaceOneModel.getCriteria()), asBson(replaceOneModel.getReplacement()),
                                                 WriteRequest.Type.REPLACE)
                                   .upsert(replaceOneModel.getOptions().isUpsert());
            } else if (writeModel instanceof UpdateOneModel) {
                UpdateOneModel<T> updateOneModel = (UpdateOneModel<T>) writeModel;
                writeRequest = new UpdateRequest(asBson(updateOneModel.getCriteria()), asBson(updateOneModel.getUpdate()),
                                                 WriteRequest.Type.UPDATE)
                                   .multi(false)
                                   .upsert(updateOneModel.getOptions().isUpsert());
            } else if (writeModel instanceof UpdateManyModel) {
                UpdateManyModel<T> updateManyModel = (UpdateManyModel<T>) writeModel;
                writeRequest = new UpdateRequest(asBson(updateManyModel.getCriteria()), asBson(updateManyModel.getUpdate()),
                                                 WriteRequest.Type.UPDATE)
                                   .multi(true)
                                   .upsert(updateManyModel.getOptions().isUpsert());
            } else if (writeModel instanceof DeleteOneModel) {
                DeleteOneModel<T> deleteOneModel = (DeleteOneModel<T>) writeModel;
                writeRequest = new DeleteRequest(asBson(deleteOneModel.getCriteria())).multi(false);
            } else if (writeModel instanceof DeleteManyModel) {
                DeleteManyModel<T> deleteManyModel = (DeleteManyModel<T>) writeModel;
                writeRequest = new DeleteRequest(asBson(deleteManyModel.getCriteria())).multi(true);
            } else {
                throw new UnsupportedOperationException(format("WriteModel of type %s is not supported", writeModel.getClass()));
            }

            writeRequests.add(writeRequest);
        }

        return executor.execute(new MixedBulkWriteOperation(namespace, writeRequests, options.isOrdered(),
                                                            this.options.getWriteConcern()));
    }

    @Override
    public void insertOne(final T document) {
        if (getCodec() instanceof CollectibleCodec) {
            ((CollectibleCodec<T>) getCodec()).generateIdIfAbsentFromDocument(document);
        }
        List<InsertRequest> requests = new ArrayList<InsertRequest>();
        requests.add(new InsertRequest(asBson(document)));
        executor.execute(new InsertOperation(namespace, true, options.getWriteConcern(), requests));
    }

    @Override
    public void insertMany(final List<? extends T> documents) {
        insertMany(documents, new InsertManyOptions());
    }

    @Override
    public void insertMany(final List<? extends T> documents, final InsertManyOptions options) {
        List<InsertRequest> requests = new ArrayList<InsertRequest>();
        for (T document : documents) {
            if (getCodec() instanceof CollectibleCodec) {
                ((CollectibleCodec<T>) getCodec()).generateIdIfAbsentFromDocument(document);
            }
            requests.add(new InsertRequest(asBson(document)));
        }
        executor.execute(new InsertOperation(namespace, options.isOrdered(), this.options.getWriteConcern(), requests));
    }

    @Override
    public DeleteResult deleteOne(final Object criteria) {
        WriteResult writeResult = executor.execute(new DeleteOperation(namespace, true, options.getWriteConcern(),
                                                                       asList(new DeleteRequest(asBson(criteria))
                                                                                  .multi(false))));
        return new DeleteResult(writeResult.getCount());
    }

    @Override
    public DeleteResult deleteMany(final Object criteria) {
        WriteResult writeResult = executor.execute(new DeleteOperation(namespace, true, options.getWriteConcern(),
                                                                       asList(new DeleteRequest(asBson(criteria))
                                                                                  .multi(true))));
        return new DeleteResult(writeResult.getCount());
    }

    @Override
    public UpdateResult replaceOne(final Object criteria, final T replacement) {
        return replaceOne(criteria, replacement, new UpdateOptions());
    }

    @Override
    public UpdateResult replaceOne(final Object criteria, final T replacement, final UpdateOptions options) {
        return replaceOne(new ReplaceOneModel<T>(criteria, replacement, options));
    }

    private UpdateResult replaceOne(final ReplaceOneModel<T> model) {
        List<UpdateRequest> requests = new ArrayList<UpdateRequest>();
        requests.add(new UpdateRequest(asBson(model.getCriteria()), asBson(model.getReplacement()), WriteRequest.Type.REPLACE)
                         .upsert(model.getOptions().isUpsert()));
        WriteResult writeResult = executor.execute(new UpdateOperation(namespace, true, options.getWriteConcern(), requests));
        return createUpdateResult(writeResult);
    }

    @Override
    public UpdateResult updateOne(final Object criteria, final Object update) {
        return updateOne(criteria, update, new UpdateOptions());
    }

    @Override
    public UpdateResult updateOne(final Object criteria, final Object update, final UpdateOptions options) {
        return updateOne(new UpdateOneModel<T>(criteria, update, options));
    }

    @Override
    public UpdateResult updateOne(final UpdateOneModel<T> model) {
        WriteResult writeResult = executor
                                      .execute(new UpdateOperation(namespace, true, options.getWriteConcern(),
                                                                   asList(new UpdateRequest(asBson(model.getCriteria()),
                                                                                            asBson(model.getUpdate()),
                                                                                            WriteRequest.Type.UPDATE)
                                                                              .multi(false)
                                                                              .upsert(model.getOptions().isUpsert()))));
        return createUpdateResult(writeResult);
    }

    @Override
    public UpdateResult updateMany(final Object criteria, final Object update) {
        return updateMany(criteria, update, new UpdateOptions());
    }

    @Override
    public UpdateResult updateMany(final Object criteria, final Object update, final UpdateOptions options) {
        return updateMany(new UpdateManyModel<T>(criteria, update, options));
    }

    private UpdateResult updateMany(final UpdateManyModel<T> model) {
        WriteResult writeResult = executor
                                      .execute(new UpdateOperation(namespace, true, options.getWriteConcern(),
                                                                   asList(new UpdateRequest(asBson(model.getCriteria()),
                                                                                            asBson(model.getUpdate()),
                                                                                            WriteRequest.Type.UPDATE)
                                                                              .multi(true)
                                                                              .upsert(model.getOptions().isUpsert()))));
        return createUpdateResult(writeResult);
    }

    @Override
    public T findOneAndDelete(final Object criteria) {
        return findOneAndDelete(criteria, new FindOneAndDeleteOptions());
    }

    @Override
    public T findOneAndDelete(final Object criteria, final FindOneAndDeleteOptions options) {
        return executor.execute(new FindAndDeleteOperation<T>(namespace, getCodec())
                                                  .criteria(asBson(criteria))
                                                  .projection(asBson(options.getProjection()))
                                                  .sort(asBson(options.getSort())));
    }

    // TODO modifiedCount
    private UpdateResult createUpdateResult(final WriteResult writeResult) {
        return new UpdateResult(writeResult.getCount(), 0, writeResult.getUpsertedId());
    }

    @Override
    public T findOneAndReplace(final Object criteria, final T replacement) {
        return findOneAndReplace(criteria, replacement, new FindOneAndReplaceOptions());
    }

    @Override
    public T findOneAndReplace(final Object criteria, final T replacement, final FindOneAndReplaceOptions options) {
        return executor.execute(new FindAndReplaceOperation<T>(namespace, getCodec(), asBson(replacement))
                                                   .criteria(asBson(criteria))
                                                   .projection(asBson(options.getProjection()))
                                                   .sort(asBson(options.getSort()))
                                                   .returnReplaced(options.getReturnReplaced())
                                                   .upsert(options.isUpsert()));
    }

    @Override
    public T findOneAndUpdate(final Object criteria, final Object update) {
        return findOneAndUpdate(criteria, update, new FindOneAndUpdateOptions());
    }

    @Override
    public T findOneAndUpdate(final Object criteria, final Object update, final FindOneAndUpdateOptions options) {
        return executor.execute(new FindAndUpdateOperation<T>(namespace, getCodec(), asBson(update))
                                                  .criteria(asBson(criteria))
                                                  .projection(asBson(options.getProjection()))
                                                  .sort(asBson(options.getSort()))
                                                  .returnUpdated(options.getReturnUpdated())
                                                  .upsert(options.isUpsert()));
    }

    @Override
    public List<MongoCursor<T>> parallelCollectionScan(final int numCursors) {
        return parallelCollectionScan(numCursors, new ParallelCollectionScanOptions());
    }

    @Override
    public List<MongoCursor<T>> parallelCollectionScan(final int numCursors,
                                                       final ParallelCollectionScanOptions parallelCollectionScanOptions) {
        return parallelCollectionScan(numCursors, parallelCollectionScanOptions, clazz);
    }

    @Override
    public <C> List<MongoCursor<C>> parallelCollectionScan(final int numCursors, final Class<C> clazz) {
       return parallelCollectionScan(numCursors, new ParallelCollectionScanOptions(), clazz);
    }

    @Override
    public <C> List<MongoCursor<C>> parallelCollectionScan(final int numCursors,
                                                           final ParallelCollectionScanOptions parallelCollectionScanOptions,
                                                           final Class<C> clazz) {
        return executor.execute(new ParallelCollectionScanOperation<C>(namespace, numCursors, getCodec(clazz))
                                .batchSize(parallelCollectionScanOptions.getBatchSize()), options.getReadPreference());
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
    public List<Document> getIndexes() {
        return executor.execute(new ListIndexesOperation<Document>(namespace, getCodec(Document.class)), options.getReadPreference());
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

    private Codec<T> getCodec() {
        return getCodec(clazz);
    }

    private <C> Codec<C> getCodec(final Class<C> clazz) {
        return options.getCodecRegistry().get(clazz);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private BsonDocument asBson(final Object document) {
        if (document == null) {
            return null;
        }
        if (document instanceof BsonDocument) {
            return (BsonDocument) document;
        } else {
            return new BsonDocumentWrapper(document, options.getCodecRegistry().get(document.getClass()));
        }
    }

    private <C> FindOperation<C> createQueryOperation(final MongoNamespace namespace, final Object criteria, final FindOptions options,
                                                      final Decoder<C> decoder) {
        return new FindOperation<C>(namespace, decoder)
                   .criteria(asBson(criteria))
                   .batchSize(options.getBatchSize())
                   .skip(options.getSkip())
                   .limit(options.getLimit())
                   .maxTime(options.getMaxTime(MILLISECONDS), MILLISECONDS)
                   .modifiers(asBson(options.getModifiers()))
                   .projection(asBson(options.getProjection()))
                   .sort(asBson(options.getSort()))
                   .awaitData(options.isAwaitData())
                   .exhaust(options.isExhaust())
                   .noCursorTimeout(options.isNoCursorTimeout())
                   .oplogReplay(options.isOplogReplay())
                   .partial(options.isPartial())
                   .tailableCursor(options.isTailable());
    }

    private <D> List<BsonDocument> createBsonDocumentList(final List<D> pipeline) {
        List<BsonDocument> aggregateList = new ArrayList<BsonDocument>(pipeline.size());
        for (D obj : pipeline) {
            aggregateList.add(asBson(obj));
        }
        return aggregateList;
    }

    private final class OperationIterable<D> implements MongoIterable<D> {
        private final ReadOperation<? extends MongoCursor<D>> operation;
        private final ReadPreference readPreference;

        private OperationIterable(final ReadOperation<? extends MongoCursor<D>> operation, final ReadPreference readPreference) {
            this.operation = operation;
            this.readPreference = readPreference;
        }

        @Override
        public <U> MongoIterable<U> map(final Function<D, U> mapper) {
            return new MappingIterable<D, U>(this, mapper);
        }

        @Override
        public MongoCursor<D> iterator() {
            return executor.execute(operation, readPreference);
        }

        @Override
        public D first() {
            MongoCursor<D> iterator = iterator();
            if (!iterator.hasNext()) {
                return null;
            }
            return iterator.next();
        }

        @Override
        public void forEach(final Block<? super D> block) {
            MongoCursor<D> cursor = iterator();
            try {
                while (cursor.hasNext()) {
                    block.apply(cursor.next());
                }
            } finally {
                cursor.close();
            }
        }

        @Override
        public <A extends Collection<? super D>> A into(final A target) {
            forEach(new Block<D>() {
                @Override
                public void apply(final D document) {
                    target.add(document);
                }
            });
            return target;
        }
    }

}
