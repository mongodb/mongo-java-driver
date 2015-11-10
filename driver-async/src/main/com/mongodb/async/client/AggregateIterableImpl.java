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
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.client.model.FindOptions;
import com.mongodb.operation.AggregateOperation;
import com.mongodb.operation.AggregateToCollectionOperation;
import com.mongodb.operation.AsyncOperationExecutor;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;


class AggregateIterableImpl<TDocument, TResult> implements AggregateIterable<TResult> {
    private final MongoNamespace namespace;
    private final Class<TDocument> documentClass;
    private final Class<TResult> resultClass;
    private final ReadPreference readPreference;
    private final ReadConcern readConcern;
    private final CodecRegistry codecRegistry;
    private final AsyncOperationExecutor executor;
    private final List<? extends Bson> pipeline;

    private Boolean allowDiskUse;
    private Integer batchSize;
    private long maxTimeMS;
    private Boolean useCursor;
    private Boolean bypassDocumentValidation;

    AggregateIterableImpl(final MongoNamespace namespace, final Class<TDocument> documentClass, final Class<TResult> resultClass,
                          final CodecRegistry codecRegistry, final ReadPreference readPreference, final ReadConcern readConcern,
                          final AsyncOperationExecutor executor, final List<? extends Bson> pipeline) {
        this.namespace = notNull("namespace", namespace);
        this.documentClass = notNull("documentClass", documentClass);
        this.resultClass = notNull("resultClass", resultClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.readConcern = notNull("readConcern", readConcern);
        this.executor = notNull("executor", executor);
        this.pipeline = notNull("pipeline", pipeline);
    }

    @Override
    public AggregateIterable<TResult> allowDiskUse(final Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    @Override
    public AggregateIterable<TResult> batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    @Override
    public AggregateIterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public AggregateIterable<TResult> useCursor(final Boolean useCursor) {
        this.useCursor = useCursor;
        return this;
    }

    @Override
    public void toCollection(final SingleResultCallback<Void> callback) {
        List<BsonDocument> aggregateList = createBsonDocumentList();
        BsonValue outCollection = getAggregateOutCollection(aggregateList);

        if (outCollection == null) {
            throw new IllegalArgumentException("The last stage of the aggregation pipeline must be $out");
        }

        executor.execute(new AggregateToCollectionOperation(namespace, aggregateList)
                .maxTime(maxTimeMS, MILLISECONDS)
                .allowDiskUse(allowDiskUse), callback);
    }

    @Override
    public void first(final SingleResultCallback<TResult> callback) {
        notNull("callback", callback);
        execute().first(callback);
    }

    @Override
    public void forEach(final Block<? super TResult> block, final SingleResultCallback<Void> callback) {
        notNull("block", block);
        notNull("callback", callback);
        execute().forEach(block, callback);
    }

    @Override
    public <A extends Collection<? super TResult>> void into(final A target, final SingleResultCallback<A> callback) {
        notNull("target", target);
        notNull("callback", callback);
        execute().into(target, callback);
    }

    @Override
    public <U> MongoIterable<U> map(final Function<TResult, U> mapper) {
        return new MappingIterable<TResult, U>(this, mapper);
    }

    @Override
    public void batchCursor(final SingleResultCallback<AsyncBatchCursor<TResult>> callback) {
        notNull("callback", callback);
        execute().batchCursor(callback);
    }

    @Override
    public AggregateIterable<TResult> bypassDocumentValidation(final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    private MongoIterable<TResult> execute() {
        List<BsonDocument> aggregateList = createBsonDocumentList();
        BsonValue outCollection = getAggregateOutCollection(aggregateList);

        if (outCollection != null) {
            AggregateToCollectionOperation operation = new AggregateToCollectionOperation(namespace, aggregateList)
                    .maxTime(maxTimeMS, MILLISECONDS)
                    .allowDiskUse(allowDiskUse)
                    .bypassDocumentValidation(bypassDocumentValidation);
            MongoIterable<TResult> delegated = new FindIterableImpl<TDocument, TResult>(new MongoNamespace(namespace.getDatabaseName(),
                    outCollection.asString().getValue()), documentClass, resultClass, codecRegistry, primary(), readConcern,
                    executor, new BsonDocument(), new FindOptions());
            if (batchSize != null) {
                delegated.batchSize(batchSize);
            }
            return new AwaitingWriteOperationIterable<TResult, Void>(operation, executor, delegated);
        } else {
            return new OperationIterable<TResult>(new AggregateOperation<TResult>(namespace, aggregateList, codecRegistry.get(resultClass))
                    .maxTime(maxTimeMS, MILLISECONDS)
                    .allowDiskUse(allowDiskUse)
                    .batchSize(batchSize)
                    .useCursor(useCursor)
                    .readConcern(readConcern),
                    readPreference,
                    executor);
        }
    }

    private BsonValue getAggregateOutCollection(final List<BsonDocument> aggregateList) {
        return aggregateList.size() == 0 ? null : aggregateList.get(aggregateList.size() - 1).get("$out");
    }

    private List<BsonDocument> createBsonDocumentList() {
        List<BsonDocument> aggregateList = new ArrayList<BsonDocument>(pipeline.size());
        for (Bson document : pipeline) {
            aggregateList.add(document.toBsonDocument(documentClass, codecRegistry));
        }
        return aggregateList;
    }
}
