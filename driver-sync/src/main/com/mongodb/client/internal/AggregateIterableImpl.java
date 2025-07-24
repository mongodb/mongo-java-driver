/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.client.internal;

import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.client.model.FindOptions;
import com.mongodb.internal.operation.Operations;
import com.mongodb.internal.operation.ReadOperationCursor;
import com.mongodb.internal.operation.ReadOperationExplainable;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

class AggregateIterableImpl<TDocument, TResult> extends MongoIterableImpl<TResult> implements AggregateIterable<TResult> {
    private final Operations<TDocument> operations;
    private final MongoNamespace namespace;
    private final Class<TDocument> documentClass;
    private final Class<TResult> resultClass;
    private final CodecRegistry codecRegistry;
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

    @SuppressWarnings("checkstyle:ParameterNumber")
    AggregateIterableImpl(@Nullable final ClientSession clientSession, final String databaseName, final Class<TDocument> documentClass,
            final Class<TResult> resultClass, final CodecRegistry codecRegistry, final ReadPreference readPreference,
            final ReadConcern readConcern, final WriteConcern writeConcern, final OperationExecutor executor,
            final List<? extends Bson> pipeline, final AggregationLevel aggregationLevel, final boolean retryReads,
            final TimeoutSettings timeoutSettings) {
        this(clientSession, new MongoNamespace(databaseName, "ignored"), documentClass, resultClass, codecRegistry, readPreference,
                readConcern, writeConcern, executor, pipeline, aggregationLevel, retryReads, timeoutSettings);
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    AggregateIterableImpl(@Nullable final ClientSession clientSession, final MongoNamespace namespace, final Class<TDocument> documentClass,
            final Class<TResult> resultClass, final CodecRegistry codecRegistry, final ReadPreference readPreference,
            final ReadConcern readConcern, final WriteConcern writeConcern, final OperationExecutor executor,
            final List<? extends Bson> pipeline, final AggregationLevel aggregationLevel, final boolean retryReads,
            final TimeoutSettings timeoutSettings) {
        super(clientSession, executor, readConcern, readPreference, retryReads, timeoutSettings);
        this.operations = new Operations<>(namespace, documentClass, readPreference, codecRegistry, readConcern, writeConcern,
                true, retryReads, timeoutSettings);
        this.namespace = notNull("namespace", namespace);
        this.documentClass = notNull("documentClass", documentClass);
        this.resultClass = notNull("resultClass", resultClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.pipeline = notNull("pipeline", pipeline);
        this.aggregationLevel = notNull("aggregationLevel", aggregationLevel);
    }

    @Override
    public void toCollection() {
        BsonDocument lastPipelineStage = getLastPipelineStage();
        if (lastPipelineStage == null || !lastPipelineStage.containsKey("$out") && !lastPipelineStage.containsKey("$merge")) {
            throw new IllegalStateException("The last stage of the aggregation pipeline must be $out or $merge");
        }

        getExecutor().execute(
                operations.aggregateToCollection(pipeline, getTimeoutMode(), allowDiskUse,
                        bypassDocumentValidation, collation, hint, hintString, comment, variables, aggregationLevel),
                getReadPreference(), getReadConcern(), getClientSession());
    }

    @Override
    public AggregateIterable<TResult> allowDiskUse(@Nullable final Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    @Override
    public AggregateIterable<TResult> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public AggregateIterable<TResult> timeoutMode(final TimeoutMode timeoutMode) {
        super.timeoutMode(timeoutMode);
        return this;
    }

    @Override
    public AggregateIterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public AggregateIterable<TResult> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        this.maxAwaitTimeMS = validateMaxAwaitTime(maxAwaitTime, timeUnit);
        return this;
    }

    @Override
    public AggregateIterable<TResult> bypassDocumentValidation(@Nullable final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    @Override
    public AggregateIterable<TResult> collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    public AggregateIterable<TResult> comment(@Nullable final String comment) {
        this.comment = comment == null ? null : new BsonString(comment);
        return this;
    }

    @Override
    public AggregateIterable<TResult> comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public AggregateIterable<TResult> hint(@Nullable final Bson hint) {
        this.hint = hint;
        return this;
    }

    @Override
    public AggregateIterable<TResult> hintString(@Nullable final String hint) {
        this.hintString = hint;
        return this;
    }

    @Override
    public AggregateIterable<TResult> let(@Nullable final Bson variables) {
        this.variables = variables;
        return this;
    }

    @Override
    public Document explain() {
        return executeExplain(Document.class, null);
    }

    @Override
    public Document explain(final ExplainVerbosity verbosity) {
        return executeExplain(Document.class, notNull("verbosity", verbosity));
    }

    @Override
    public <E> E explain(final Class<E> explainDocumentClass) {
        return executeExplain(explainDocumentClass, null);
    }

    @Override
    public <E> E explain(final Class<E> explainResultClass, final ExplainVerbosity verbosity) {
        return executeExplain(explainResultClass, notNull("verbosity", verbosity));
    }

    private <E> E executeExplain(final Class<E> explainResultClass, @Nullable final ExplainVerbosity verbosity) {
        notNull("explainDocumentClass", explainResultClass);
        return getExecutor().execute(
                asAggregateOperation().asExplainableOperation(verbosity, codecRegistry.get(explainResultClass)), getReadPreference(),
                getReadConcern(), getClientSession());
    }

    @Override
    public ReadOperationCursor<TResult> asReadOperation() {
        MongoNamespace outNamespace = getOutNamespace();
        if (outNamespace != null) {
            validateTimeoutMode();
            getExecutor().execute(
                    operations.aggregateToCollection(pipeline, getTimeoutMode(), allowDiskUse,
                            bypassDocumentValidation, collation, hint, hintString, comment, variables, aggregationLevel),
                    getReadPreference(), getReadConcern(), getClientSession());

            FindOptions findOptions = new FindOptions().collation(collation);
            Integer batchSize = getBatchSize();
            if (batchSize != null) {
                findOptions.batchSize(batchSize);
            }
            return operations.find(outNamespace, new BsonDocument(), resultClass, findOptions);
        } else {
            return asAggregateOperation();
        }
    }

    protected OperationExecutor getExecutor() {
        return getExecutor(operations.createTimeoutSettings(maxTimeMS, maxAwaitTimeMS));
    }

    private ReadOperationExplainable<TResult> asAggregateOperation() {
        return operations.aggregate(pipeline, resultClass, getTimeoutMode(), getBatchSize(), collation, hint, hintString, comment,
                variables, allowDiskUse, aggregationLevel);
    }

    @Nullable
    private BsonDocument getLastPipelineStage() {
        if (pipeline.isEmpty()) {
            return null;
        } else {
            Bson lastStage = notNull("last pipeline stage", pipeline.get(pipeline.size() - 1));
            return lastStage.toBsonDocument(documentClass, codecRegistry);
        }
    }

    @Nullable
    private MongoNamespace getOutNamespace() {
        BsonDocument lastPipelineStage = getLastPipelineStage();
        if (lastPipelineStage == null) {
            return null;
        }
        if (lastPipelineStage.containsKey("$out")) {
            if (lastPipelineStage.get("$out").isString()) {
                return new MongoNamespace(namespace.getDatabaseName(), lastPipelineStage.getString("$out").getValue());
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
                return new MongoNamespace(namespace.getDatabaseName(), lastPipelineStage.getString("$merge").getValue());
            } else if (lastPipelineStage.isDocument("$merge")) {
                BsonDocument mergeDocument = lastPipelineStage.getDocument("$merge");
                if (mergeDocument.isDocument("into")) {
                    BsonDocument intoDocument = mergeDocument.getDocument("into");
                    return new MongoNamespace(intoDocument.getString("db", new BsonString(namespace.getDatabaseName())).getValue(),
                            intoDocument.getString("coll").getValue());
                } else if (mergeDocument.isString("into")) {
                    return new MongoNamespace(namespace.getDatabaseName(), mergeDocument.getString("into").getValue());
                }
            } else {
                throw new IllegalStateException("Cannot return a cursor when the value for $merge stage is not a string or a document");
            }
        }

        return null;
    }

    private void validateTimeoutMode() {
        if (getTimeoutMode() == TimeoutMode.ITERATION) {
            throw new IllegalArgumentException("Aggregations that output to a collection do not support the ITERATION value for the "
                    + "timeoutMode option.");
        }
    }
}
