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
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.ExplainableReadOperation;
import com.mongodb.internal.operation.ReadOperation;
import com.mongodb.internal.operation.SyncOperations;
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

/**
 * Implementation of MongoDB's aggregation framework that supports both cursor-based results
 * and output to collections. This class handles complex pipeline configurations including
 * $out and $merge stages, with support for various execution modes and optimizations.
 *
 * <p>Key features:</p>
 * <ul>
 *     <li>Flexible pipeline execution with cursor or collection output</li>
 *     <li>Support for $out and $merge stages with various configurations</li>
 *     <li>Configurable resource management (disk usage, timeouts)</li>
 *     <li>Operation monitoring through comments and explain plans</li>
 * </ul>
 *
 * <p>Pipeline handling:</p>
 * <ul>
 *     <li>Validates pipeline structure and output stages</li>
 *     <li>Supports both database and collection-level aggregations</li>
 *     <li>Handles pipeline optimization hints</li>
 *     <li>Manages variable contexts for pipeline execution</li>
 * </ul>
 *
 * <p>Output stage behavior ($out/$merge):</p>
 * <ul>
 *     <li>$out: Creates new collections or replaces existing ones</li>
 *     <li>$merge: Supports incremental updates with custom strategies</li>
 *     <li>Handles cross-database output operations</li>
 *     <li>Validates output specifications and permissions</li>
 * </ul>
 *
 * <p>Performance considerations:</p>
 * <ul>
 *     <li>Disk usage can be controlled via allowDiskUse option</li>
 *     <li>Batch size tuning affects memory usage and network efficiency</li>
 *     <li>Index hints can optimize pipeline execution</li>
 *     <li>Timeout controls prevent runaway operations</li>
 * </ul>
 *
 * <p>Thread safety: This class is not thread-safe and should not be used concurrently
 * from multiple threads.</p>
 */
class AggregateIterableImpl<TDocument, TResult> extends MongoIterableImpl<TResult> implements AggregateIterable<TResult> {
    private final SyncOperations<TDocument> operations;
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

    /**
     * Creates a new aggregation iterable for database-level operations.
     *
     * @param clientSession optional client session for the operation
     * @param databaseName the database to aggregate on
     * @param documentClass the class of the input documents
     * @param resultClass the class to decode each result document into
     * @param codecRegistry the codec registry for encoding/decoding documents
     * @param readPreference the read preference to use
     * @param readConcern the read concern to use
     * @param writeConcern the write concern for output operations
     * @param executor the operation executor
     * @param pipeline the aggregation pipeline
     * @param aggregationLevel whether this is a database or collection level aggregation
     * @param retryReads whether to retry read operations
     * @param timeoutSettings the timeout settings for the operation
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    AggregateIterableImpl(@Nullable final ClientSession clientSession, final String databaseName, final Class<TDocument> documentClass,
            final Class<TResult> resultClass, final CodecRegistry codecRegistry, final ReadPreference readPreference,
            final ReadConcern readConcern, final WriteConcern writeConcern, final OperationExecutor executor,
            final List<? extends Bson> pipeline, final AggregationLevel aggregationLevel, final boolean retryReads,
            final TimeoutSettings timeoutSettings) {
        this(clientSession, new MongoNamespace(databaseName, "ignored"), documentClass, resultClass, codecRegistry, readPreference,
                readConcern, writeConcern, executor, pipeline, aggregationLevel, retryReads, timeoutSettings);
    }

    /**
     * Creates a new aggregation iterable for collection-level operations.
     *
     * @param clientSession optional client session for the operation
     * @param namespace the collection namespace to aggregate on
     * @param documentClass the class of the input documents
     * @param resultClass the class to decode each result document into
     * @param codecRegistry the codec registry for encoding/decoding documents
     * @param readPreference the read preference to use
     * @param readConcern the read concern to use
     * @param writeConcern the write concern for output operations
     * @param executor the operation executor
     * @param pipeline the aggregation pipeline
     * @param aggregationLevel whether this is a database or collection level aggregation
     * @param retryReads whether to retry read operations
     * @param timeoutSettings the timeout settings for the operation
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    AggregateIterableImpl(@Nullable final ClientSession clientSession, final MongoNamespace namespace, final Class<TDocument> documentClass,
            final Class<TResult> resultClass, final CodecRegistry codecRegistry, final ReadPreference readPreference,
            final ReadConcern readConcern, final WriteConcern writeConcern, final OperationExecutor executor,
            final List<? extends Bson> pipeline, final AggregationLevel aggregationLevel, final boolean retryReads,
            final TimeoutSettings timeoutSettings) {
        super(clientSession, executor, readConcern, readPreference, retryReads, timeoutSettings);
        this.operations = new SyncOperations<>(namespace, documentClass, readPreference, codecRegistry, readConcern, writeConcern,
                true, retryReads, timeoutSettings);
        this.namespace = notNull("namespace", namespace);
        this.documentClass = notNull("documentClass", documentClass);
        this.resultClass = notNull("resultClass", resultClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.pipeline = notNull("pipeline", pipeline);
        this.aggregationLevel = notNull("aggregationLevel", aggregationLevel);
    }

    /**
     * Executes the aggregation pipeline and writes the results to a collection.
     * This method requires the last stage to be either $out or $merge.
     *
     * @throws IllegalStateException if the last stage is not $out or $merge
     */
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

    /**
     * Enables writing to temporary files during aggregation.
     * Useful for pipelines that exceed memory limits.
     *
     * @param allowDiskUse whether to allow disk use
     * @return this
     */
    @Override
    public AggregateIterable<TResult> allowDiskUse(@Nullable final Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     */
    @Override
    public AggregateIterable<TResult> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    /**
     * Sets the timeout mode for the operation.
     *
     * @param timeoutMode the timeout mode
     * @return this
     */
    @Override
    public AggregateIterable<TResult> timeoutMode(final TimeoutMode timeoutMode) {
        super.timeoutMode(timeoutMode);
        return this;
    }

    /**
     * Sets the maximum execution time on the server.
     *
     * @param maxTime the max time
     * @param timeUnit the time unit
     * @return this
     * @throws IllegalArgumentException if timeUnit is null
     */
    @Override
    public AggregateIterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    /**
     * Sets the maximum await execution time on the server for change streams.
     *
     * @param maxAwaitTime the max await time
     * @param timeUnit the time unit
     * @return this
     */
    @Override
    public AggregateIterable<TResult> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        this.maxAwaitTimeMS = validateMaxAwaitTime(maxAwaitTime, timeUnit);
        return this;
    }

    /**
     * Sets whether to bypass document validation for $out and $merge operations.
     *
     * @param bypassDocumentValidation whether to bypass document validation
     * @return this
     */
    @Override
    public AggregateIterable<TResult> bypassDocumentValidation(@Nullable final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    /**
     * Sets the collation to use for string comparisons.
     *
     * @param collation the collation
     * @return this
     */
    @Override
    public AggregateIterable<TResult> collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    /**
     * Sets a comment to help trace this operation through the database profiler,
     * currentOp, and logs.
     *
     * @param comment the comment
     * @return this
     */
    @Override
    public AggregateIterable<TResult> comment(@Nullable final String comment) {
        this.comment = comment == null ? null : new BsonString(comment);
        return this;
    }

    /**
     * Sets a comment to help trace this operation through the database profiler,
     * currentOp, and logs.
     *
     * @param comment the comment
     * @return this
     */
    @Override
    public AggregateIterable<TResult> comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    /**
     * Sets the index hint for the aggregation.
     *
     * @param hint the index hint
     * @return this
     */
    @Override
    public AggregateIterable<TResult> hint(@Nullable final Bson hint) {
        this.hint = hint;
        return this;
    }

    /**
     * Sets the index hint for the aggregation.
     *
     * @param hint the index hint
     * @return this
     */
    @Override
    public AggregateIterable<TResult> hintString(@Nullable final String hint) {
        this.hintString = hint;
        return this;
    }

    /**
     * Sets the variables to use in the aggregation pipeline.
     *
     * @param variables the variables
     * @return this
     */
    @Override
    public AggregateIterable<TResult> let(@Nullable final Bson variables) {
        this.variables = variables;
        return this;
    }

    /**
     * Returns an explanation of the pipeline execution plan.
     *
     * @return the execution plan
     */
    @Override
    public Document explain() {
        return executeExplain(Document.class, null);
    }

    /**
     * Returns an explanation of the pipeline execution plan.
     *
     * @param verbosity the explain verbosity
     * @return the execution plan
     */
    @Override
    public Document explain(final ExplainVerbosity verbosity) {
        return executeExplain(Document.class, notNull("verbosity", verbosity));
    }

    /**
     * Returns an explanation of the pipeline execution plan.
     *
     * @param explainDocumentClass the class to decode the explain document into
     * @return the execution plan
     */
    @Override
    public <E> E explain(final Class<E> explainDocumentClass) {
        return executeExplain(explainDocumentClass, null);
    }

    /**
     * Returns an explanation of the pipeline execution plan.
     *
     * @param explainResultClass the class to decode the explain document into
     * @param verbosity the explain verbosity
     * @return the execution plan
     */
    @Override
    public <E> E explain(final Class<E> explainResultClass, final ExplainVerbosity verbosity) {
        return executeExplain(explainResultClass, notNull("verbosity", verbosity));
    }

    /**
     * Executes an explain command for this pipeline.
     *
     * @param explainResultClass the class to decode the explain document into
     * @param verbosity the explain verbosity
     * @return the execution plan
     */
    private <E> E executeExplain(final Class<E> explainResultClass, @Nullable final ExplainVerbosity verbosity) {
        notNull("explainDocumentClass", explainResultClass);
        return getExecutor().execute(
                asAggregateOperation().asExplainableOperation(verbosity, codecRegistry.get(explainResultClass)), getReadPreference(),
                getReadConcern(), getClientSession());
    }

    /**
     * Creates a read operation for this aggregation pipeline.
     * Handles both cursor-based results and output to collections.
     *
     * @return the read operation
     */
    @Override
    public ReadOperation<BatchCursor<TResult>> asReadOperation() {
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

    /**
     * Gets an executor configured with the current timeout settings.
     *
     * @return the operation executor
     */
    protected OperationExecutor getExecutor() {
        return getExecutor(operations.createTimeoutSettings(maxTimeMS, maxAwaitTimeMS));
    }

    /**
     * Creates an aggregate operation for this pipeline.
     *
     * @return the aggregate operation
     */
    private ExplainableReadOperation<BatchCursor<TResult>> asAggregateOperation() {
        return operations.aggregate(pipeline, resultClass, getTimeoutMode(), getBatchSize(), collation, hint, hintString, comment,
                variables, allowDiskUse, aggregationLevel);
    }

    /**
     * Gets the last stage of the pipeline.
     *
     * @return the last pipeline stage or null if the pipeline is empty
     */
    @Nullable
    private BsonDocument getLastPipelineStage() {
        if (pipeline.isEmpty()) {
            return null;
        } else {
            Bson lastStage = notNull("last pipeline stage", pipeline.get(pipeline.size() - 1));
            return lastStage.toBsonDocument(documentClass, codecRegistry);
        }
    }

    /**
     * Gets the output namespace for $out or $merge stages.
     * Handles both string and document forms of these stages.
     *
     * @return the output namespace or null if no output stage is present
     * @throws IllegalStateException if the output stage specification is invalid
     */
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

    /**
     * Validates that the timeout mode is appropriate for output operations.
     *
     * @throws IllegalArgumentException if using ITERATION timeout mode with output operations
     */
    private void validateTimeoutMode() {
        if (getTimeoutMode() == TimeoutMode.ITERATION) {
            throw new IllegalArgumentException("Aggregations that output to a collection do not support the ITERATION value for the "
                    + "timeoutMode option.");
        }
    }
}
