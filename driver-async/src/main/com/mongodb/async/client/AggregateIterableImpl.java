/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.async.client;

import com.mongodb.Block;
import com.mongodb.Function;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.client.model.FindOptions;
import com.mongodb.operation.AggregateOperation;
import com.mongodb.operation.AggregateToCollectionOperation;
import com.mongodb.operation.AsyncOperationExecutor;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;


class AggregateIterableImpl<T> implements AggregateIterable<T> {
    private final MongoNamespace namespace;
    private final Class<T> clazz;
    private final ReadPreference readPreference;
    private final CodecRegistry codecRegistry;
    private final AsyncOperationExecutor executor;
    private final List<?> pipeline;

    private Boolean allowDiskUse;
    private Integer batchSize;
    private long maxTimeMS;
    private Boolean useCursor;

    AggregateIterableImpl(final MongoNamespace namespace, final Class<T> clazz, final CodecRegistry codecRegistry,
                          final ReadPreference readPreference, final AsyncOperationExecutor executor, final List<?> pipeline) {
        this.namespace = notNull("namespace", namespace);
        this.clazz = notNull("clazz", clazz);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.executor = notNull("executor", executor);
        this.pipeline = notNull("pipeline", pipeline);
    }

    @Override
    public AggregateIterable<T> allowDiskUse(final Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    @Override
    public AggregateIterable<T> batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    @Override
    public AggregateIterable<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public AggregateIterable<T> useCursor(final Boolean useCursor) {
        this.useCursor = useCursor;
        return this;
    }

    @Override
    public void toCollection(final SingleResultCallback<Void> callback) {
        List<BsonDocument> aggregateList = createBsonDocumentList(pipeline);
        BsonValue outCollection = getAggregateOutCollection(aggregateList);

        if (outCollection == null) {
            throw new IllegalArgumentException("The last stage of the aggregation pipeline must be $out");
        }

        executor.execute(new AggregateToCollectionOperation(namespace, aggregateList)
                .maxTime(maxTimeMS, MILLISECONDS)
                .allowDiskUse(allowDiskUse), callback);
    }

    @Override
    public void first(final SingleResultCallback<T> callback) {
        execute().first(callback);
    }

    @Override
    public void forEach(final Block<? super T> block, final SingleResultCallback<Void> callback) {
        execute().forEach(block, callback);
    }

    @Override
    public <A extends Collection<? super T>> void into(final A target, final SingleResultCallback<A> callback) {
        execute().into(target, callback);
    }

    @Override
    public <U> MongoIterable<U> map(final Function<T, U> mapper) {
        return new MappingIterable<T, U>(this, mapper);
    }

    @Override
    public void batchCursor(final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        execute().batchCursor(callback);
    }

    private MongoIterable<T> execute() {
        List<BsonDocument> aggregateList = createBsonDocumentList(pipeline);
        BsonValue outCollection = getAggregateOutCollection(aggregateList);

        if (outCollection != null) {
            AggregateToCollectionOperation operation = new AggregateToCollectionOperation(namespace, aggregateList)
                    .maxTime(maxTimeMS, MILLISECONDS)
                    .allowDiskUse(allowDiskUse);
            MongoIterable<T> delegated = new FindIterableImpl<T>(new MongoNamespace(namespace.getDatabaseName(),
                    outCollection.asString().getValue()),
                    clazz, codecRegistry, primary(), executor, new BsonDocument(),
                    new FindOptions());
            if (batchSize != null) {
                delegated.batchSize(batchSize);
            }
            return new AwaitingWriteOperationIterable<T, Void>(operation, executor, delegated);
        } else {
            return new OperationIterable<T>(new AggregateOperation<T>(namespace, aggregateList, codecRegistry.get(clazz))
                    .maxTime(maxTimeMS, MILLISECONDS)
                    .allowDiskUse(allowDiskUse)
                    .batchSize(batchSize)
                    .useCursor(useCursor),
                    readPreference,
                    executor);
        }
    }

    private BsonValue getAggregateOutCollection(final List<BsonDocument> aggregateList) {
        return aggregateList.size() == 0 ? null : aggregateList.get(aggregateList.size() - 1).get("$out");
    }

    private <D> List<BsonDocument> createBsonDocumentList(final List<D> pipeline) {
        List<BsonDocument> aggregateList = new ArrayList<BsonDocument>(pipeline.size());
        for (D obj : pipeline) {
            aggregateList.add(BsonDocumentWrapper.asBsonDocument(obj, codecRegistry));
        }
        return aggregateList;
    }
}
