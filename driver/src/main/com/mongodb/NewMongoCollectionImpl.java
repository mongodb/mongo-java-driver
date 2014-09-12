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
import com.mongodb.client.model.BulkWriteModel;
import com.mongodb.client.model.CountModel;
import com.mongodb.client.model.DistinctModel;
import com.mongodb.client.model.ExplainableModel;
import com.mongodb.client.model.FindModel;
import com.mongodb.client.model.InsertManyModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.RemoveManyModel;
import com.mongodb.client.model.RemoveOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.RemoveResult;
import com.mongodb.client.result.ReplaceOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.operation.AggregateOperation;
import com.mongodb.operation.CountOperation;
import com.mongodb.operation.DistinctOperation;
import com.mongodb.operation.Find;
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
import org.bson.BsonValue;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.mongodb.BulkWriteResult;
import org.mongodb.Document;
import org.mongodb.WriteResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;

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
    public MongoIterable<Document> aggregate(final List<?> pipeline) {
        return aggregate(pipeline, Document.class);
    }

    @Override
    public <D> MongoIterable<D> aggregate(final List<?> pipeline, final Class<D> clazz) {
        List<BsonDocument> aggregateList = new ArrayList<BsonDocument>(pipeline.size());
        for (Object obj : pipeline) {
            aggregateList.add(asBson(obj));
        }
        return new OperationIterable<D>(new AggregateOperation<D>(namespace, aggregateList, options.getCodecRegistry().get(clazz),
                                                                  com.mongodb.operation.AggregationOptions.builder().build()),
                                        options.getReadPreference());
    }

    @Override
    public long count() {
        return count(new CountModel());
    }

    @Override
    public long count(final CountModel model) {
        return operationExecutor.execute(new CountOperation(namespace, new Find()), options.getReadPreference());
    }

    @Override
    public List<Object> distinct(final String fieldName) {
        return distinct(new DistinctModel(fieldName));
    }

    @Override
    public List<Object> distinct(final DistinctModel model) {
        BsonArray distinctArray = operationExecutor.execute(new DistinctOperation(namespace, model.getFieldName(), new Find()),
                                                            options.getReadPreference());
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
    public MongoIterable<T> find(final FindModel model) {
        QueryOperation<T> operation = new QueryOperation<T>(namespace, getCodec());
        operation.setCriteria(asBson(model.getCriteria()));
        operation.setProjection(asBson(model.getProjection()));
        operation.setSort(asBson(model.getSort()));
        operation.setSkip(model.getSkip());
        operation.setLimit(model.getLimit());
        operation.setCursorFlags(model.getCursorFlags());
        operation.setModifiers(asBson(model.getModifiers()));
        operation.setBatchSize(model.getBatchSize());
        operation.setMaxTime(model.getMaxTime(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);

        return new OperationIterable<T>(operation, options.getReadPreference());
    }

    @Override
    public <D> MongoIterable<D> find(final FindModel model, Class<D> clazz) {
        return new OperationIterable<D>(new QueryOperation<D>(namespace, new Find(), options.getCodecRegistry().get(clazz)),
                                        options.getReadPreference());
    }

    @Override
    public BulkWriteResult bulkWrite(final List<? extends WriteModel<? extends T>> operations) {
        return bulkWrite(new BulkWriteModel<T>(operations));
    }

    @Override
    public BulkWriteResult bulkWrite(final BulkWriteModel<? extends T> model) {
        List<WriteRequest> requests = new ArrayList<WriteRequest>();
        for (WriteModel<? extends T> writeModel : model.getOperations()) {
            WriteRequest writeRequest;
            if (writeModel instanceof InsertOneModel) {
                InsertOneModel<T> insertOneModel = (InsertOneModel<T>) writeModel;
                writeRequest = new InsertRequest<T>(insertOneModel.getDocument());
            } else if (writeModel instanceof ReplaceOneModel) {
                ReplaceOneModel<T> replaceOneModel = (ReplaceOneModel<T>) writeModel;
                writeRequest = new ReplaceRequest<T>(asBson(replaceOneModel.getFilter()), replaceOneModel.getReplacement())
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
                throw new UnsupportedOperationException(String.format("WriteModel of type %s is not supported", writeModel.getClass()));
            }

            requests.add(writeRequest);
        }

        return operationExecutor.execute(new MixedBulkWriteOperation<T>(namespace, requests, model.isOrdered(),
                                                                        options.getWriteConcern(), getCodec()));
    }

    @Override
    public InsertOneResult insertOne(final T document) {
        List<InsertRequest<T>> requests = new ArrayList<InsertRequest<T>>();
        requests.add(new InsertRequest<T>(document));
        operationExecutor.execute(new InsertOperation<T>(namespace, true, options.getWriteConcern(), requests, getCodec()));
        return new InsertOneResult(null, 1); // TODO: insertedId
    }

    @Override
    public InsertManyResult insertMany(final List<? extends T> documents) {
        return insertMany(new InsertManyModel<T>(documents));
    }

    @Override
    public InsertManyResult insertMany(final InsertManyModel<T> model) {
        List<InsertRequest<T>> requests = new ArrayList<InsertRequest<T>>();
        for (T document : model.getDocuments()) {
            requests.add(new InsertRequest<T>(document));
        }
        operationExecutor.execute(new InsertOperation<T>(namespace, model.isOrdered(), options.getWriteConcern(), requests, getCodec()));
        return new InsertManyResult(null, model.getDocuments().size()); // TODO: insertedId
    }

    @Override
    public RemoveResult removeOne(final Object filter) {
        WriteResult writeResult = operationExecutor.execute(new RemoveOperation(namespace, true, options.getWriteConcern(),
                                                                                asList(new RemoveRequest(asBson(filter)))));
        return new RemoveResult(writeResult.getCount());
    }

    @Override
    public RemoveResult removeMany(final Object filter) {
        WriteResult writeResult = operationExecutor.execute(new RemoveOperation(namespace, true, options.getWriteConcern(),
                                                                                asList(new RemoveRequest(asBson(filter)).multi(true))));
        return new RemoveResult(writeResult.getCount());
    }

    @Override
    public ReplaceOneResult replaceOne(final Object filter, final T replacement) {
        return replaceOne(new ReplaceOneModel<T>(filter, replacement));
    }

    @Override
    public ReplaceOneResult replaceOne(final ReplaceOneModel<T> model) {
        List<ReplaceRequest<T>> requests = new ArrayList<ReplaceRequest<T>>();
        requests.add(new ReplaceRequest<T>(asBson(model.getFilter()), model.getReplacement()));
        WriteResult writeResult = operationExecutor.execute(new ReplaceOperation<T>(namespace, true, options.getWriteConcern(),
                                                                                    requests, getCodec()));
        return new ReplaceOneResult(writeResult.getCount(), 0, writeResult.getUpsertedId());  // TODO matchedCount
    }

    @Override
    public UpdateResult updateOne(final Object filter, final Object update) {
        return updateOne(new UpdateOneModel<T>(filter, update));
    }

    @Override
    public UpdateResult updateOne(final UpdateOneModel<T> model) {
        WriteResult writeResult = operationExecutor
                                  .execute(new UpdateOperation(namespace, true, options.getWriteConcern(),
                                                               asList(new UpdateRequest(asBson(model.getFilter()),
                                                                                        asBson(model.getUpdate())))));
        return new UpdateResult(writeResult.getCount(), 0, writeResult.getUpsertedId());
    }

    @Override
    public UpdateResult updateMany(final Object filter, final Object update) {
        return updateMany(new UpdateManyModel<T>(filter, update));
    }

    @Override
    public UpdateResult updateMany(final UpdateManyModel<T> model) {
        WriteResult writeResult = operationExecutor
                                  .execute(new UpdateOperation(namespace, true, options.getWriteConcern(),
                                                               asList(new UpdateRequest(asBson(model.getFilter()),
                                                                                        asBson(model.getUpdate())).multi(true))));
        return new UpdateResult(writeResult.getCount(), 0, writeResult.getUpsertedId());
    }

    @Override
    public Document explain(final ExplainableModel explainableModel, final ExplainVerbosity verbosity) {
        throw new UnsupportedOperationException("Not implemented yet!");
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

    private class OperationIterable<D> implements MongoIterable<D> {
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
