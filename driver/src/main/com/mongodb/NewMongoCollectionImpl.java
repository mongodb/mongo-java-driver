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
import com.mongodb.client.NewMongoCollection;
import com.mongodb.client.model.BulkWriteModel;
import com.mongodb.client.model.CountModel;
import com.mongodb.client.model.DistinctModel;
import com.mongodb.client.model.ExplainableModel;
import com.mongodb.client.model.FindModel;
import com.mongodb.client.model.InsertManyModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.RemoveResult;
import com.mongodb.client.result.ReplaceOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.operation.CountOperation;
import com.mongodb.operation.Find;
import com.mongodb.operation.InsertOperation;
import com.mongodb.operation.InsertRequest;
import com.mongodb.operation.OperationExecutor;
import com.mongodb.operation.QueryOperation;
import com.mongodb.operation.RemoveOperation;
import com.mongodb.operation.RemoveRequest;
import com.mongodb.operation.ReplaceOperation;
import com.mongodb.operation.ReplaceRequest;
import com.mongodb.operation.UpdateOperation;
import com.mongodb.operation.UpdateRequest;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonValue;
import org.bson.codecs.Codec;
import org.mongodb.Document;
import org.mongodb.WriteResult;

import java.util.ArrayList;
import java.util.List;

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
    public long count() {
        return count(new CountModel());
    }

    @Override
    public long count(final CountModel model) {
        return operationExecutor.execute(new CountOperation(namespace, new Find()), options.getReadPreference());
    }

    @Override
    public MongoCursor<BsonValue> distinct(final String fieldName) {
        return distinct(new DistinctModel(fieldName));
    }

    @Override
    public MongoCursor<BsonValue> distinct(final DistinctModel model) {
        // TODO:
        throw new UnsupportedOperationException();
        //return operationExecutor.execute(new DistinctOperation(namespace, model.getFieldName(), new Find()), options.getReadPreference());
    }

    @Override
    public MongoCursor<T> find(final Object filter) {
        return find(new FindModel(filter));
    }

    @Override
    public MongoCursor<T> find(final FindModel model) {
        return operationExecutor.execute(new QueryOperation<T>(namespace, new Find(), getCodec()), options.getReadPreference());
    }

    @Override
    public BulkWriteResult bulkWrite(final List<? extends WriteModel<? extends T>> operations) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public BulkWriteResult bulkWrite(final BulkWriteModel<? extends T> model) {
        throw new UnsupportedOperationException("Not implemented yet!");
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
}
