/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.client.model.FindOptions;
import com.mongodb.internal.operation.AsyncExplainableReadOperation;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

final class AggregatePublisherImpl<T> extends BatchCursorPublisher<T> implements AggregatePublisher<T> {
    private final List<? extends Bson> pipeline;
    private final AggregationLevel aggregationLevel;
    private Boolean allowDiskUse;
    private long maxTimeMS;
    private long maxAwaitTimeMS;
    private Boolean bypassDocumentValidation;
    private Collation collation;
    private BsonValue comment;
    private Bson hint;
    private String hintString;
    private Bson variables;

    AggregatePublisherImpl(
            @Nullable final ClientSession clientSession,
            final MongoOperationPublisher<T> mongoOperationPublisher,
            final List<? extends Bson> pipeline,
            final AggregationLevel aggregationLevel) {
        super(clientSession, mongoOperationPublisher);
        this.pipeline = notNull("pipeline", pipeline);
        this.aggregationLevel = notNull("aggregationLevel", aggregationLevel);
    }

    @Override
    public AggregatePublisher<T> allowDiskUse(@Nullable final Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    @Override
    public AggregatePublisher<T> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public AggregatePublisher<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public AggregatePublisher<T> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxAwaitTimeMS = TimeUnit.MILLISECONDS.convert(maxAwaitTime, timeUnit);
        return this;
    }

    @Override
    public AggregatePublisher<T> bypassDocumentValidation(@Nullable final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    @Override
    public AggregatePublisher<T> collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    public AggregatePublisher<T> comment(@Nullable final String comment) {
        this.comment = comment != null ? new BsonString(comment) : null;
        return this;
    }

    @Override
    public AggregatePublisher<T> comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public AggregatePublisher<T> hint(@Nullable final Bson hint) {
        this.hint = hint;
        return this;
    }

    @Override
    public AggregatePublisher<T> hintString(@Nullable final String hint) {
        this.hintString = hint;
        return this;
    }

    @Override
    public AggregatePublisher<T> let(@Nullable final Bson variables) {
        this.variables = variables;
        return this;
    }

    @Override
    public Publisher<Void> toCollection() {
        BsonDocument lastPipelineStage = getLastPipelineStage();
        if (lastPipelineStage == null || !lastPipelineStage.containsKey("$out") && !lastPipelineStage.containsKey("$merge")) {
            throw new IllegalStateException("The last stage of the aggregation pipeline must be $out or $merge");
        }
        return getMongoOperationPublisher().createReadOperationMono(this::getAggregateToCollectionOperation, getClientSession());
    }

    @Override
    public Publisher<Document> explain() {
        return publishExplain(Document.class, null);
    }

    @Override
    public Publisher<Document> explain(final ExplainVerbosity verbosity) {
        return publishExplain(Document.class, notNull("verbosity", verbosity));
    }

    @Override
    public <E> Publisher<E> explain(final Class<E> explainResultClass) {
        return publishExplain(explainResultClass, null);
    }

    @Override
    public <E> Publisher<E> explain(final Class<E> explainResultClass, final ExplainVerbosity verbosity) {
        return publishExplain(explainResultClass, notNull("verbosity", verbosity));
    }

    private <E> Publisher<E> publishExplain(final Class<E> explainResultClass, @Nullable final ExplainVerbosity verbosity) {
        notNull("explainDocumentClass", explainResultClass);
        return getMongoOperationPublisher().createReadOperationMono(() ->
                        asAggregateOperation(1).asAsyncExplainableOperation(verbosity,
                                getCodecRegistry().get(explainResultClass)),
                getClientSession());
    }

    @Override
    AsyncReadOperation<AsyncBatchCursor<T>> asAsyncReadOperation(final int initialBatchSize) {
        MongoNamespace outNamespace = getOutNamespace();

        if (outNamespace != null) {
            AsyncReadOperation<Void> aggregateToCollectionOperation = getAggregateToCollectionOperation();

            FindOptions findOptions = new FindOptions().collation(collation).comment(comment).batchSize(initialBatchSize);

            AsyncReadOperation<AsyncBatchCursor<T>> findOperation =
                    getOperations().find(outNamespace, new BsonDocument(), getDocumentClass(), findOptions);

            return new VoidReadOperationThenCursorReadOperation<>(aggregateToCollectionOperation, findOperation);
        } else {
            return asAggregateOperation(initialBatchSize);
        }
    }

    private AsyncExplainableReadOperation<AsyncBatchCursor<T>> asAggregateOperation(final int initialBatchSize) {
        return getOperations()
                .aggregate(pipeline, getDocumentClass(), maxTimeMS, maxAwaitTimeMS,
                           initialBatchSize, collation, hint, hintString, comment, variables, allowDiskUse, aggregationLevel);
    }

    private AsyncReadOperation<Void> getAggregateToCollectionOperation() {
        return getOperations().aggregateToCollection(pipeline, maxTimeMS, allowDiskUse, bypassDocumentValidation, collation, hint, hintString, comment,
                                                     variables, aggregationLevel);
    }

    @Nullable
    private BsonDocument getLastPipelineStage() {
        if (pipeline.isEmpty()) {
            return null;
        } else {
            Bson lastStage = notNull("last pipeline stage", pipeline.get(pipeline.size() - 1));
            return lastStage.toBsonDocument(getDocumentClass(), getCodecRegistry());
        }
    }

    @Nullable
    private MongoNamespace getOutNamespace() {
        BsonDocument lastPipelineStage = getLastPipelineStage();
        if (lastPipelineStage == null) {
            return null;
        }
        String databaseName = getNamespace().getDatabaseName();
        if (lastPipelineStage.containsKey("$out")) {
            if (lastPipelineStage.get("$out").isString()) {
                return new MongoNamespace(databaseName, lastPipelineStage.getString("$out").getValue());
            } else if (lastPipelineStage.get("$out").isDocument()) {
                BsonDocument outDocument = lastPipelineStage.getDocument("$out");
                if (!outDocument.containsKey("db") || !outDocument.containsKey("coll")) {
                    throw new IllegalStateException("Cannot return a cursor when the value for $out stage is not a namespace document");
                }
                return new MongoNamespace(outDocument.getString("db").getValue(), outDocument.getString("coll").getValue());
            } else {
                throw new IllegalStateException("Cannot return a cursor when the value for $out stage "
                                                        + "is not a string or namespace document");
            }
        } else if (lastPipelineStage.containsKey("$merge")) {
            if (lastPipelineStage.isString("$merge")) {
                return new MongoNamespace(databaseName, lastPipelineStage.getString("$merge").getValue());
            } else if (lastPipelineStage.isDocument("$merge")) {
                BsonDocument mergeDocument = lastPipelineStage.getDocument("$merge");
                if (mergeDocument.isDocument("into")) {
                    BsonDocument intoDocument = mergeDocument.getDocument("into");
                    return new MongoNamespace(intoDocument.getString("db", new BsonString(databaseName)).getValue(),
                            intoDocument.getString("coll").getValue());
                } else if (mergeDocument.isString("into")) {
                    return new MongoNamespace(databaseName, mergeDocument.getString("into").getValue());
                }
            } else {
                throw new IllegalStateException("Cannot return a cursor when the value for $merge stage is not a string or a document");
            }
        }

        return null;
    }
}
