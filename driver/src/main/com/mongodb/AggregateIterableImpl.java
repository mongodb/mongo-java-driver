/*
 * Copyright 2015-2016 MongoDB, Inc.
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

package com.mongodb;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.model.Collation;
import com.mongodb.operation.AggregateOperation;
import com.mongodb.operation.AggregateToCollectionOperation;
import com.mongodb.operation.BatchCursor;
import com.mongodb.operation.FindOperation;
import com.mongodb.operation.ReadOperation;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class AggregateIterableImpl<TDocument, TResult> extends MongoIterableImpl<TResult> implements AggregateIterable<TResult> {
    private final MongoNamespace namespace;
    private final Class<TDocument> documentClass;
    private final Class<TResult> resultClass;
    private final WriteConcern writeConcern;
    private final CodecRegistry codecRegistry;
    private final List<? extends Bson> pipeline;

    private Boolean allowDiskUse;
    private long maxTimeMS;
    private long maxAwaitTimeMS;
    private Boolean useCursor;
    private Boolean bypassDocumentValidation;
    private Collation collation;

    AggregateIterableImpl(final ClientSession clientSession, final MongoNamespace namespace, final Class<TDocument> documentClass,
                          final Class<TResult> resultClass, final CodecRegistry codecRegistry, final ReadPreference readPreference,
                          final ReadConcern readConcern, final WriteConcern writeConcern, final OperationExecutor executor,
                          final List<? extends Bson> pipeline) {
        super(clientSession, executor, readConcern, readPreference);
        this.namespace = notNull("namespace", namespace);
        this.documentClass = notNull("documentClass", documentClass);
        this.resultClass = notNull("resultClass", resultClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.pipeline = notNull("pipeline", pipeline);
    }

    @Override
    public void toCollection() {
        List<BsonDocument> aggregateList = createBsonDocumentList(pipeline);

        if (getOutCollection(aggregateList) == null) {
            throw new IllegalStateException("The last stage of the aggregation pipeline must be $out");
        }

        getExecutor().execute(createAggregateToCollectionOperation(aggregateList), getClientSession());
    }

    @Override
    public AggregateIterable<TResult> allowDiskUse(final Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    @Override
    public AggregateIterable<TResult> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public AggregateIterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    @Deprecated
    public AggregateIterable<TResult> useCursor(final Boolean useCursor) {
        this.useCursor = useCursor;
        return this;
    }

    @Override
    public AggregateIterable<TResult> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxAwaitTimeMS = TimeUnit.MILLISECONDS.convert(maxAwaitTime, timeUnit);
        return this;
    }

    @Override
    public AggregateIterable<TResult> bypassDocumentValidation(final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    @Override
    public AggregateIterable<TResult> collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    @SuppressWarnings("deprecation")
    ReadOperation<BatchCursor<TResult>> asReadOperation() {
        List<BsonDocument> aggregateList = createBsonDocumentList(pipeline);

        BsonValue outCollection = getOutCollection(aggregateList);

        if (outCollection != null) {
            getExecutor().execute(createAggregateToCollectionOperation(aggregateList), getClientSession());
            FindOperation<TResult> findOperation =
                    new FindOperation<TResult>(new MongoNamespace(namespace.getDatabaseName(), outCollection.asString().getValue()),
                                                      codecRegistry.get(resultClass))
                            .readConcern(getReadConcern())
                            .collation(collation);
            if (getBatchSize() != null) {
                findOperation.batchSize(getBatchSize());
            }
            return findOperation;
        } else {
            return new AggregateOperation<TResult>(namespace, aggregateList, codecRegistry.get(resultClass))
                    .maxTime(maxTimeMS, MILLISECONDS)
                    .maxAwaitTime(maxAwaitTimeMS, MILLISECONDS)
                    .allowDiskUse(allowDiskUse)
                    .batchSize(getBatchSize())
                    .useCursor(useCursor)
                    .readConcern(getReadConcern())
                    .collation(collation);
        }
    }

    private BsonValue getOutCollection(final List<BsonDocument> aggregateList) {
        return aggregateList.size() == 0 ? null : aggregateList.get(aggregateList.size() - 1).get("$out");
    }

    private AggregateToCollectionOperation createAggregateToCollectionOperation(final List<BsonDocument> aggregateList) {
        return new AggregateToCollectionOperation(namespace, aggregateList, writeConcern)
                        .maxTime(maxTimeMS, MILLISECONDS)
                        .allowDiskUse(allowDiskUse)
                        .bypassDocumentValidation(bypassDocumentValidation)
                        .collation(collation);
    }

    private List<BsonDocument> createBsonDocumentList(final List<? extends Bson> pipeline) {
        List<BsonDocument> aggregateList = new ArrayList<BsonDocument>(pipeline.size());
        for (Bson obj : pipeline) {
            if (obj == null) {
                throw new IllegalArgumentException("pipeline can not contain a null value");
            }
            aggregateList.add(obj.toBsonDocument(documentClass, codecRegistry));
        }
        return aggregateList;
    }
}
