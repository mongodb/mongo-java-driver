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

import com.mongodb.client.MongoCollectionOptions;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.NewMongoCollection;
import com.mongodb.client.model.AggregateModel;
import com.mongodb.client.model.BulkWriteModel;
import com.mongodb.client.model.CountModel;
import com.mongodb.client.model.DistinctModel;
import com.mongodb.client.model.ExplainableModel;
import com.mongodb.client.model.FindModel;
import com.mongodb.client.model.FindOneAndRemoveModel;
import com.mongodb.client.model.FindOneAndReplaceModel;
import com.mongodb.client.model.FindOneAndUpdateModel;
import com.mongodb.client.model.InsertManyModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.RemoveManyModel;
import com.mongodb.client.model.RemoveOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.RemoveResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.codecs.CollectibleCodec;
import com.mongodb.codecs.DocumentCodec;
import com.mongodb.operation.AggregateOperation;
import com.mongodb.operation.AggregateToCollectionOperation;
import com.mongodb.operation.CountOperation;
import com.mongodb.operation.DistinctOperation;
import com.mongodb.operation.FindAndRemoveOperation;
import com.mongodb.operation.FindAndReplaceOperation;
import com.mongodb.operation.FindAndUpdateOperation;
import com.mongodb.operation.InsertOperation;
import com.mongodb.operation.InsertRequest;
import com.mongodb.operation.MixedBulkWriteOperation;
import com.mongodb.operation.OperationExecutor;
import com.mongodb.operation.QueryOperation;
import com.mongodb.operation.ReadOperation;
import com.mongodb.operation.RemoveOperation;
import com.mongodb.operation.RemoveRequest;
import com.mongodb.operation.ReplaceOperation;
import com.mongodb.operation.ReplaceRequest;
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
import java.util.List;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class NewMongoCollectionImpl<T> implements NewMongoCollection<T> {
    private final MongoNamespace namespace;
    private final MongoCollectionOptions options;
    private final Class<T> clazz;
    private final OperationExecutor operationExecutor;

    NewMongoCollectionImpl(final MongoNamespace namespace, final Class<T> clazz,
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
    public <D> MongoIterable<Document> aggregate(final AggregateModel<D> model) {
        return aggregate(model, Document.class);
    }

    @Override
    public <D, C> MongoIterable<C> aggregate(final AggregateModel<D> model, final Class<C> clazz) {
        List<BsonDocument> aggregateList = createBsonDocumentList(model.getPipeline());

        BsonValue outCollection = aggregateList.size() == 0 ? null : aggregateList.get(aggregateList.size() - 1).get("$out");

        if (outCollection != null) {
            AggregateToCollectionOperation operation = new AggregateToCollectionOperation(namespace, aggregateList)
                                                       .maxTime(model.getMaxTime(MILLISECONDS), MILLISECONDS)
                                                       .allowDiskUse(model.getAllowDiskUse());
            operationExecutor.execute(operation);
            return new OperationIterable<C>(new QueryOperation<C>(new MongoNamespace(namespace.getDatabaseName(),
                                                                                     outCollection.asString().getValue()),
                                                                  options.getCodecRegistry().get(clazz)),
                                            options.getReadPreference());
        } else {
            return new OperationIterable<C>(createAggregateOperation(model, options.getCodecRegistry().get(clazz), aggregateList),
                                            options.getReadPreference());
        }
    }

    @Override
    public <D> long count(final CountModel<D> model) {
        return operationExecutor.execute(createCountOperation(model), options.getReadPreference());
    }

    @Override
    public <D> List<Object> distinct(final DistinctModel<D> model) {
        DistinctOperation operation = new DistinctOperation(namespace, model.getFieldName())
                                      .criteria(asBson(model.getCriteria()))
                                      .maxTime(model.getMaxTime(MILLISECONDS), MILLISECONDS);
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
    public <F> MongoIterable<T> find(final FindModel<F> model) {
        return find(model, clazz);
    }

    @Override
    public <F, D> MongoIterable<D> find(final FindModel<F> model, final Class<D> clazz) {
        return new OperationIterable<D>(createQueryOperation(model, options.getCodecRegistry().get(clazz)), options.getReadPreference());
    }

    @Override
    public <D> BulkWriteResult bulkWrite(final BulkWriteModel<? extends T, D> model) {
        List<WriteRequest> requests = new ArrayList<WriteRequest>();
        for (WriteModel<? extends T, D> writeModel : model.getOperations()) {
            WriteRequest writeRequest;
            if (writeModel instanceof InsertOneModel) {
                InsertOneModel<T, D> insertOneModel = (InsertOneModel<T, D>) writeModel;
                if (getCodec() instanceof CollectibleCodec) {
                    ((CollectibleCodec<T>) getCodec()).generateIdIfAbsentFromDocument(insertOneModel.getDocument());
                }
                writeRequest = new InsertRequest(asBson(insertOneModel.getDocument()));
            } else if (writeModel instanceof ReplaceOneModel) {
                ReplaceOneModel<T, D> replaceOneModel = (ReplaceOneModel<T, D>) writeModel;
                writeRequest = new ReplaceRequest(asBson(replaceOneModel.getFilter()), asBson(replaceOneModel.getReplacement()))
                               .upsert(replaceOneModel.isUpsert());
            } else if (writeModel instanceof UpdateOneModel) {
                UpdateOneModel updateOneModel = (UpdateOneModel) writeModel;
                writeRequest = new UpdateRequest(asBson(updateOneModel.getFilter()), asBson(updateOneModel.getUpdate()))
                               .upsert(updateOneModel.isUpsert());
            } else if (writeModel instanceof UpdateManyModel) {
                UpdateManyModel updateManyModel = (UpdateManyModel) writeModel;
                writeRequest = new UpdateRequest(asBson(updateManyModel.getFilter()), asBson(updateManyModel.getUpdate()))
                               .multi(true)
                               .upsert(updateManyModel.isUpsert());
            } else if (writeModel instanceof RemoveOneModel) {
                RemoveOneModel removeOneModel = (RemoveOneModel) writeModel;
                writeRequest = new RemoveRequest(asBson(removeOneModel.getFilter()));
            } else if (writeModel instanceof RemoveManyModel) {
                RemoveManyModel removeManyModel = (RemoveManyModel) writeModel;
                writeRequest = new RemoveRequest(asBson(removeManyModel.getFilter()))
                               .multi(true);
            } else {
                throw new UnsupportedOperationException(format("WriteModel of type %s is not supported", writeModel.getClass()));
            }

            requests.add(writeRequest);
        }

        return operationExecutor.execute(new MixedBulkWriteOperation(namespace, requests, model.isOrdered(), options.getWriteConcern()));
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
    public void insertMany(final InsertManyModel<T> model) {
        List<InsertRequest> requests = new ArrayList<InsertRequest>();
        for (T document : model.getDocuments()) {
            if (getCodec() instanceof CollectibleCodec) {
                ((CollectibleCodec<T>) getCodec()).generateIdIfAbsentFromDocument(document);
            }
            requests.add(new InsertRequest(asBson(document)));
        }
        operationExecutor.execute(new InsertOperation(namespace, model.isOrdered(), options.getWriteConcern(), requests));
    }

    @Override
    public <D> RemoveResult removeOne(final RemoveOneModel<T, D> model) {
        WriteResult writeResult = operationExecutor.execute(new RemoveOperation(namespace, true, options.getWriteConcern(),
                                                                                asList(new RemoveRequest(asBson(model.getFilter())))));
        return new RemoveResult(writeResult.getCount());
    }

    @Override
    public <D> RemoveResult removeMany(final RemoveManyModel<T, D> model) {
        WriteResult writeResult = operationExecutor.execute(new RemoveOperation(namespace, true, options.getWriteConcern(),
                                                                                asList(new RemoveRequest(asBson(model.getFilter()))
                                                                                       .multi(true))));
        return new RemoveResult(writeResult.getCount());
    }

    @Override
    public <D> UpdateResult replaceOne(final ReplaceOneModel<T, D> model) {
        List<ReplaceRequest> requests = new ArrayList<ReplaceRequest>();
        requests.add(new ReplaceRequest(asBson(model.getFilter()), asBson(model.getReplacement())));
        WriteResult writeResult = operationExecutor.execute(new ReplaceOperation(namespace, true, options.getWriteConcern(), requests));
        return new UpdateResult(writeResult.getCount(), 0, writeResult.getUpsertedId());  // TODO matchedCount
    }

    @Override
    public <D> UpdateResult updateOne(final UpdateOneModel<T, D> model) {
        WriteResult writeResult = operationExecutor
                                  .execute(new UpdateOperation(namespace, true, options.getWriteConcern(),
                                                               asList(new UpdateRequest(asBson(model.getFilter()),
                                                                                        asBson(model.getUpdate())))));
        return new UpdateResult(writeResult.getCount(), 0, writeResult.getUpsertedId());
    }

    @Override
    public <D> UpdateResult updateMany(final UpdateManyModel<T, D> model) {
        WriteResult writeResult = operationExecutor
                                  .execute(new UpdateOperation(namespace, true, options.getWriteConcern(),
                                                               asList(new UpdateRequest(asBson(model.getFilter()),
                                                                                        asBson(model.getUpdate())).multi(true))));
        return new UpdateResult(writeResult.getCount(), 0, writeResult.getUpsertedId());
    }

    @Override
    public <D> T findOneAndRemove(final FindOneAndRemoveModel<D> model) {
        FindAndRemoveOperation<T> operation = new FindAndRemoveOperation<T>(namespace, getCodec())
                                              .criteria(asBson(model.getCriteria()))
                                              .projection(asBson(model.getProjection()))
                                              .sort(asBson(model.getSort()));
        return operationExecutor.execute(operation);
    }

    @Override
    public <D> T findOneAndUpdate(final FindOneAndUpdateModel<D> model) {
        FindAndUpdateOperation<T> operation = new FindAndUpdateOperation<T>(namespace, getCodec(), asBson(model.getUpdate()))
                                              .criteria(asBson(model.getCriteria()))
                                              .projection(asBson(model.getProjection()))
                                              .sort(asBson(model.getSort()))
                                              .returnUpdated(model.getReturnUpdated())
                                              .upsert(model.isUpsert());
        return operationExecutor.execute(operation);
    }

    @Override
    public <D> T findOneAndReplace(final FindOneAndReplaceModel<T, D> model) {
        FindAndReplaceOperation<T> operation = new FindAndReplaceOperation<T>(namespace, getCodec(), asBson(model.getReplacement()))
                                               .criteria(asBson(model.getCriteria()))
                                               .projection(asBson(model.getProjection()))
                                               .sort(asBson(model.getSort()))
                                               .returnReplaced(model.getReturnReplaced())
                                               .upsert(model.isUpsert());
        return operationExecutor.execute(operation);
    }

    @Override
    public <D> Document explain(final ExplainableModel<D> explainableModel, final ExplainVerbosity verbosity) {
        if (explainableModel instanceof AggregateModel) {
            return explainAggregate((AggregateModel<D>) explainableModel, verbosity);
        } else if (explainableModel instanceof FindModel) {
            return explainFind((FindModel<D>) explainableModel, verbosity);
        } else if (explainableModel instanceof CountModel) {
            return explainCount((CountModel<D>) explainableModel, verbosity);
        } else {
            throw new UnsupportedOperationException(format("Unsupported explainable model type %s", explainableModel.getClass()));
        }
    }

    private <D> Document explainCount(final CountModel<D> countModel, final ExplainVerbosity verbosity) {
        CountOperation countOperation = createCountOperation(countModel);
        BsonDocument bsonDocument = operationExecutor.execute(countOperation.asExplainableOperation(verbosity),
                                                              options.getReadPreference());
        return new DocumentCodec().decode(new BsonDocumentReader(bsonDocument), DecoderContext.builder().build());
    }

    private <D> Document explainFind(final FindModel<D> findModel, final ExplainVerbosity verbosity) {
        QueryOperation<BsonDocument> queryOperation = createQueryOperation(findModel, new BsonDocumentCodec());
        BsonDocument bsonDocument = operationExecutor.execute(queryOperation.asExplainableOperation(verbosity),
                                                              options.getReadPreference());
        return new DocumentCodec().decode(new BsonDocumentReader(bsonDocument), DecoderContext.builder().build());
    }

    private <D> Document explainAggregate(final AggregateModel<D> aggregateModel, final ExplainVerbosity verbosity) {
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

    private <F, D> QueryOperation<D> createQueryOperation(final FindModel<F> model, final Decoder<D> decoder) {
        return new QueryOperation<D>(namespace, decoder)
               .criteria(asBson(model.getCriteria()))
               .batchSize(model.getBatchSize())
               .cursorFlags(model.getCursorFlags())
               .skip(model.getSkip())
               .limit(model.getLimit())
               .maxTime(model.getMaxTime(MILLISECONDS), MILLISECONDS)
               .modifiers(asBson(model.getModifiers()))
               .projection(asBson(model.getProjection()))
               .sort(asBson(model.getSort()));
    }

    private <D> CountOperation createCountOperation(final CountModel<D> model) {
        CountOperation operation = new CountOperation(namespace)
                                   .criteria(asBson(model.getCriteria()))
                                   .skip(model.getSkip())
                                   .limit(model.getLimit())
                                   .maxTime(model.getMaxTime(MILLISECONDS), MILLISECONDS);
        if (model.getHint() != null) {
            operation.hint(asBson(model.getHint()));
        } else if (model.getHintString() != null) {
            operation.hint(new BsonString(model.getHintString()));
        }
        return operation;
    }

    private <D, C> AggregateOperation<C> createAggregateOperation(final AggregateModel<D> model, final Decoder<C> decoder,
                                                                  final List<BsonDocument> aggregateList) {
        return new AggregateOperation<C>(namespace, aggregateList, decoder)
               .maxTime(model.getMaxTime(MILLISECONDS), MILLISECONDS)
               .allowDiskUse(model.getAllowDiskUse())
               .batchSize(model.getBatchSize())
               .useCursor(model.getUseCursor());
    }

    private <D> List<BsonDocument> createBsonDocumentList(final List<D> pipeline) {
        List<BsonDocument> aggregateList = new ArrayList<BsonDocument>(pipeline.size());
        for (D obj : pipeline) {
            aggregateList.add(asBson(obj));
        }
        return aggregateList;
    }

    private final class OperationIterable<D> implements MongoIterable<D> {
        private final ReadOperation<MongoCursor<D>> operation;
        private final ReadPreference readPreference;

        private OperationIterable(final ReadOperation<MongoCursor<D>> operation, final ReadPreference readPreference) {
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
