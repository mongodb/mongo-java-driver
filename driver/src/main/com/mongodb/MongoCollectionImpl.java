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

import com.mongodb.client.CollectionAdministration;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCollectionOptions;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.AggregateModel;
import com.mongodb.client.model.AggregateOptions;
import com.mongodb.client.model.BulkWriteModel;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountModel;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.DistinctModel;
import com.mongodb.client.model.DistinctOptions;
import com.mongodb.client.model.ExplainableModel;
import com.mongodb.client.model.FindModel;
import com.mongodb.client.model.FindOneAndDeleteModel;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceModel;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateModel;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.FindOptions;
import com.mongodb.client.model.InsertManyModel;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOneOptions;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateManyOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOneOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.codecs.CollectibleCodec;
import com.mongodb.codecs.DocumentCodec;
import com.mongodb.operation.AggregateOperation;
import com.mongodb.operation.AggregateToCollectionOperation;
import com.mongodb.operation.CountOperation;
import com.mongodb.operation.DeleteOperation;
import com.mongodb.operation.DeleteRequest;
import com.mongodb.operation.DistinctOperation;
import com.mongodb.operation.FindAndDeleteOperation;
import com.mongodb.operation.FindAndReplaceOperation;
import com.mongodb.operation.FindAndUpdateOperation;
import com.mongodb.operation.FindOperation;
import com.mongodb.operation.InsertOperation;
import com.mongodb.operation.InsertRequest;
import com.mongodb.operation.MixedBulkWriteOperation;
import com.mongodb.operation.OperationExecutor;
import com.mongodb.operation.ReadOperation;
import com.mongodb.operation.UpdateOperation;
import com.mongodb.operation.UpdateRequest;
import com.mongodb.operation.WriteRequest;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;
import org.mongodb.BulkWriteResult;
import org.mongodb.Document;
import org.mongodb.WriteResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class MongoCollectionImpl<T> implements MongoCollection<T> {
    private final MongoNamespace namespace;
    private final MongoCollectionOptions options;
    private final Class<T> clazz;
    private final OperationExecutor operationExecutor;

    MongoCollectionImpl(final MongoNamespace namespace, final Class<T> clazz,
                        final MongoCollectionOptions options, final OperationExecutor operationExecutor) {

        this.namespace = namespace;
        this.clazz = clazz;
        this.options = options;
        this.operationExecutor = operationExecutor;
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
        return count(new CountModel());
    }

    @Override
    public long count(final CountOptions options) {
        return operationExecutor.execute(createCountOperation(new CountModel(options)), this.options.getReadPreference());
    }

    private long count(final CountModel model) {
        return operationExecutor.execute(createCountOperation(model), options.getReadPreference());
    }

    @Override
    public List<Object> distinct(final String fieldName) {
        return distinct(fieldName, new DistinctOptions());
    }

    @Override
    public List<Object> distinct(final String fieldName, final DistinctOptions options) {
        return distinct(new DistinctModel(fieldName, options));
    }

    private List<Object> distinct(final DistinctModel model) {
        DistinctOperation operation = new DistinctOperation(namespace, model.getFieldName())
                                          .criteria(asBson(model.getOptions().getCriteria()))
                                          .maxTime(model.getOptions().getMaxTime(MILLISECONDS), MILLISECONDS);
        BsonArray distinctArray = operationExecutor.execute(operation, options.getReadPreference());
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
        return find(new FindOptions());
    }

    @Override
    public <C> MongoIterable<C> find(final Class<C> clazz) {
        return find(new FindOptions(), clazz);
    }

    @Override
    public MongoIterable<T> find(final FindOptions findOptions) {
        return find(findOptions, clazz);
    }

    @Override
    public <C> MongoIterable<C> find(final FindOptions findOptions, final Class<C> clazz) {
        return find(new FindModel(findOptions), clazz);
    }

    private <C> MongoIterable<C> find(final FindModel findModel, final Class<C> clazz) {
        return new OperationIterable<C>(createQueryOperation(findModel, options.getCodecRegistry().get(clazz)),
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
        return aggregate(new AggregateModel(pipeline, options), clazz);
    }

    private MongoIterable<Document> aggregate(final AggregateModel model) {
        return aggregate(model, Document.class);
    }

    private <C> MongoIterable<C> aggregate(final AggregateModel model, final Class<C> clazz) {
        List<BsonDocument> aggregateList = createBsonDocumentList(model.getPipeline());

        BsonValue outCollection = aggregateList.size() == 0 ? null : aggregateList.get(aggregateList.size() - 1).get("$out");

        if (outCollection != null) {
            AggregateToCollectionOperation operation = new AggregateToCollectionOperation(namespace, aggregateList)
                                                           .maxTime(model.getOptions().getMaxTime(MILLISECONDS), MILLISECONDS)
                                                           .allowDiskUse(model.getOptions().getAllowDiskUse());
            operationExecutor.execute(operation);
            return new OperationIterable<C>(new FindOperation<C>(new MongoNamespace(namespace.getDatabaseName(),
                                                                                    outCollection.asString().getValue()),
                                                                 options.getCodecRegistry().get(clazz)),
                                            options.getReadPreference());
        } else {
            return new OperationIterable<C>(createAggregateOperation(model, options.getCodecRegistry().get(clazz), aggregateList),
                                            options.getReadPreference());
        }
    }

    @Override
    public BulkWriteResult bulkWrite(final List<? extends WriteModel<? extends T>> requests) {
        return bulkWrite(requests, new BulkWriteOptions());
    }

    @Override
    public BulkWriteResult bulkWrite(final List<? extends WriteModel<? extends T>> requests, final BulkWriteOptions options) {
        return bulkWrite(new BulkWriteModel<T>(requests, options));
    }

    @SuppressWarnings("unchecked")
    private BulkWriteResult bulkWrite(final BulkWriteModel<? extends T> model) {
        List<WriteRequest> requests = new ArrayList<WriteRequest>();
        for (WriteModel<? extends T> writeModel : model.getRequests()) {
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

            requests.add(writeRequest);
        }

        return operationExecutor.execute(new MixedBulkWriteOperation(namespace, requests, model.getOptions().isOrdered(),
                                                                     options.getWriteConcern()));
    }

    @Override
    public void insertOne(final T document) {
        if (getCodec() instanceof CollectibleCodec) {
            ((CollectibleCodec<T>) getCodec()).generateIdIfAbsentFromDocument(document);
        }
        List<InsertRequest> requests = new ArrayList<InsertRequest>();
        requests.add(new InsertRequest(asBson(document)));
        operationExecutor.execute(new InsertOperation(namespace, true, options.getWriteConcern(), requests));
    }

    @Override
    public void insertMany(final List<? extends T> documents) {
        insertMany(documents, new InsertManyOptions());
    }

    @Override
    public void insertMany(final List<? extends T> documents, final InsertManyOptions options) {
        insertMany(new InsertManyModel<T>(documents, options));
    }

    private void insertMany(final InsertManyModel<T> model) {
        List<InsertRequest> requests = new ArrayList<InsertRequest>();
        for (T document : model.getDocuments()) {
            if (getCodec() instanceof CollectibleCodec) {
                ((CollectibleCodec<T>) getCodec()).generateIdIfAbsentFromDocument(document);
            }
            requests.add(new InsertRequest(asBson(document)));
        }
        operationExecutor.execute(new InsertOperation(namespace, model.getOptions().isOrdered(), options.getWriteConcern(), requests));
    }

    @Override
    public DeleteResult deleteOne(final Object criteria) {
        WriteResult writeResult = operationExecutor.execute(new DeleteOperation(namespace, true, options.getWriteConcern(),
                                                                                asList(new DeleteRequest(asBson(criteria))
                                                                                           .multi(false))));
        return new DeleteResult(writeResult.getCount());
    }

    @Override
    public DeleteResult deleteMany(final Object criteria) {
        WriteResult writeResult = operationExecutor.execute(new DeleteOperation(namespace, true, options.getWriteConcern(),
                                                                                asList(new DeleteRequest(asBson(criteria))
                                                                                           .multi(true))));
        return new DeleteResult(writeResult.getCount());
    }

    @Override
    public UpdateResult replaceOne(final Object criteria, final T replacement) {
        return replaceOne(criteria, replacement, new ReplaceOneOptions());
    }

    @Override
    public UpdateResult replaceOne(final Object criteria, final T replacement, final ReplaceOneOptions options) {
        return replaceOne(new ReplaceOneModel<T>(criteria, replacement, options));
    }

    private UpdateResult replaceOne(final ReplaceOneModel<T> model) {
        List<UpdateRequest> requests = new ArrayList<UpdateRequest>();
        requests.add(new UpdateRequest(asBson(model.getCriteria()), asBson(model.getReplacement()), WriteRequest.Type.REPLACE)
                         .upsert(model.getOptions().isUpsert()));
        WriteResult writeResult = operationExecutor.execute(new UpdateOperation(namespace, true, options.getWriteConcern(), requests));
        return createUpdateResult(writeResult);
    }

    @Override
    public UpdateResult updateOne(final Object criteria, final Object update) {
        return updateOne(criteria, update, new UpdateOneOptions());
    }

    @Override
    public UpdateResult updateOne(final Object criteria, final Object update, final UpdateOneOptions options) {
        return updateOne(new UpdateOneModel<T>(criteria, update, options));
    }

    @Override
    public UpdateResult updateOne(final UpdateOneModel<T> model) {
        WriteResult writeResult = operationExecutor
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
        return updateMany(criteria, update, new UpdateManyOptions());
    }

    @Override
    public UpdateResult updateMany(final Object criteria, final Object update, final UpdateManyOptions options) {
        return updateMany(new UpdateManyModel<T>(criteria, update, options));
    }

    private UpdateResult updateMany(final UpdateManyModel<T> model) {
        WriteResult writeResult = operationExecutor
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
        return findOneAndDelete(new FindOneAndDeleteModel(criteria, options));
    }

    // TODO modifiedCount
    private UpdateResult createUpdateResult(final WriteResult writeResult) {
        return new UpdateResult(writeResult.getCount(), 0, writeResult.getUpsertedId());
    }

    private T findOneAndDelete(final FindOneAndDeleteModel model) {
        FindAndDeleteOperation<T> operation = new FindAndDeleteOperation<T>(namespace, getCodec())
                                                  .criteria(asBson(model.getCriteria()))
                                                  .projection(asBson(model.getOptions().getProjection()))
                                                  .sort(asBson(model.getOptions().getSort()));
        return operationExecutor.execute(operation);
    }

    @Override
    public T findOneAndReplace(final Object criteria, final T replacement) {
        return findOneAndReplace(criteria, replacement, new FindOneAndReplaceOptions());
    }

    @Override
    public T findOneAndReplace(final Object criteria, final T replacement, final FindOneAndReplaceOptions options) {
        return findOneAndReplace(new FindOneAndReplaceModel<T>(criteria, replacement, options));
    }

    T findOneAndUpdate(final FindOneAndUpdateModel model) {
        FindAndUpdateOperation<T> operation = new FindAndUpdateOperation<T>(namespace, getCodec(), asBson(model.getUpdate()))
                                                  .criteria(asBson(model.getCriteria()))
                                                  .projection(asBson(model.getOptions().getProjection()))
                                                  .sort(asBson(model.getOptions().getSort()))
                                                  .returnUpdated(model.getOptions().getReturnUpdated())
                                                  .upsert(model.getOptions().isUpsert());
        return operationExecutor.execute(operation);
    }

    private T findOneAndReplace(final FindOneAndReplaceModel<T> model) {
        FindAndReplaceOperation<T> operation = new FindAndReplaceOperation<T>(namespace, getCodec(), asBson(model.getReplacement()))
                                                   .criteria(asBson(model.getCriteria()))
                                                   .projection(asBson(model.getOptions().getProjection()))
                                                   .sort(asBson(model.getOptions().getSort()))
                                                   .returnReplaced(model.getOptions().getReturnReplaced())
                                                   .upsert(model.getOptions().isUpsert());
        return operationExecutor.execute(operation);
    }

    @Override
    public T findOneAndUpdate(final Object criteria, final Object update) {
        return findOneAndUpdate(criteria, update, new FindOneAndUpdateOptions());
    }

    @Override
    public T findOneAndUpdate(final Object criteria, final Object update, final FindOneAndUpdateOptions options) {
        return findOneAndUpdate(new FindOneAndUpdateModel(criteria, update, options));
    }

    @Override
    public Document explain(final ExplainableModel explainableModel, final ExplainVerbosity verbosity) {
        if (explainableModel instanceof AggregateModel) {
            return explainAggregate((AggregateModel) explainableModel, verbosity);
        } else if (explainableModel instanceof FindModel) {
            return explainFind((FindModel) explainableModel, verbosity);
        } else if (explainableModel instanceof CountModel) {
            return explainCount((CountModel) explainableModel, verbosity);
        } else {
            throw new UnsupportedOperationException(format("Unsupported explainable model type %s", explainableModel.getClass()));
        }
    }

    @Override
    public CollectionAdministration tools() {
        return new CollectionAdministrationImpl(getNamespace(), operationExecutor);
    }

    private Document explainCount(final CountModel countModel, final ExplainVerbosity verbosity) {
        CountOperation countOperation = createCountOperation(countModel);
        BsonDocument bsonDocument = operationExecutor.execute(countOperation.asExplainableOperation(verbosity),
                                                              options.getReadPreference());
        return new DocumentCodec().decode(new BsonDocumentReader(bsonDocument), DecoderContext.builder().build());
    }

    private Document explainFind(final FindModel findModel, final ExplainVerbosity verbosity) {
        FindOperation<BsonDocument> findOperation = createQueryOperation(findModel, new BsonDocumentCodec());
        BsonDocument bsonDocument = operationExecutor.execute(findOperation.asExplainableOperation(verbosity),
                                                              options.getReadPreference());
        return new DocumentCodec().decode(new BsonDocumentReader(bsonDocument), DecoderContext.builder().build());
    }

    private Document explainAggregate(final AggregateModel aggregateModel, final ExplainVerbosity verbosity) {
        AggregateOperation<BsonDocument> operation = createAggregateOperation(aggregateModel, new BsonDocumentCodec(),
                                                                              createBsonDocumentList(aggregateModel.getPipeline()));
        BsonDocument bsonDocument = operationExecutor.execute(operation.asExplainableOperation(verbosity), options.getReadPreference());
        return new DocumentCodec().decode(new BsonDocumentReader(bsonDocument), DecoderContext.builder().build());
    }

    private Codec<T> getCodec() {
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

    private <C> FindOperation<C> createQueryOperation(final FindModel model, final Decoder<C> decoder) {
        return new FindOperation<C>(namespace, decoder)
                   .criteria(asBson(model.getOptions().getCriteria()))
                   .batchSize(model.getOptions().getBatchSize())
                   .cursorFlags(getCursorFlags(model))
                   .skip(model.getOptions().getSkip())
                   .limit(model.getOptions().getLimit())
                   .maxTime(model.getOptions().getMaxTime(MILLISECONDS), MILLISECONDS)
                   .modifiers(asBson(model.getOptions().getModifiers()))
                   .projection(asBson(model.getOptions().getProjection()))
                   .sort(asBson(model.getOptions().getSort()));
    }

    private EnumSet<CursorFlag> getCursorFlags(final FindModel model) {
        EnumSet<CursorFlag> cursorFlags = EnumSet.noneOf(CursorFlag.class);
        if (model.getOptions().isAwaitData()) {
            cursorFlags.add(CursorFlag.AWAIT_DATA);
        }
        if (model.getOptions().isExhaust()) {
            cursorFlags.add(CursorFlag.EXHAUST);
        }
        if (model.getOptions().isNoCursorTimeout()) {
            cursorFlags.add(CursorFlag.NO_CURSOR_TIMEOUT);
        }
        if (model.getOptions().isOplogReplay()) {
            cursorFlags.add(CursorFlag.OPLOG_REPLAY);
        }
        if (model.getOptions().isPartial()) {
            cursorFlags.add(CursorFlag.PARTIAL);
        }
        if (model.getOptions().isTailable()) {
            cursorFlags.add(CursorFlag.TAILABLE);
        }
        return cursorFlags;
    }

    private CountOperation createCountOperation(final CountModel model) {
        CountOperation operation = new CountOperation(namespace)
                                       .criteria(asBson(model.getOptions().getCriteria()))
                                       .skip(model.getOptions().getSkip())
                                       .limit(model.getOptions().getLimit())
                                       .maxTime(model.getOptions().getMaxTime(MILLISECONDS), MILLISECONDS);
        if (model.getOptions().getHint() != null) {
            operation.hint(asBson(model.getOptions().getHint()));
        } else if (model.getOptions().getHintString() != null) {
            operation.hint(new BsonString(model.getOptions().getHintString()));
        }
        return operation;
    }

    private <C> AggregateOperation<C> createAggregateOperation(final AggregateModel model, final Decoder<C> decoder,
                                                               final List<BsonDocument> aggregateList) {
        return new AggregateOperation<C>(namespace, aggregateList, decoder)
                   .maxTime(model.getOptions().getMaxTime(MILLISECONDS), MILLISECONDS)
                   .allowDiskUse(model.getOptions().getAllowDiskUse())
                   .batchSize(model.getOptions().getBatchSize())
                   .useCursor(model.getOptions().getUseCursor());
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
            return operationExecutor.execute(operation, readPreference);
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
