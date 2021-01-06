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

package com.mongodb.internal.operation;

import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.codecs.Decoder;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.internal.operation.ExplainHelper.asExplainCommand;

/**
 * An operation that executes an aggregation query.
 *
 * @param <T> the operations result type.
 * @mongodb.driver.manual aggregation/ Aggregation
 * @since 3.0
 */
public class AggregateOperation<T> implements AsyncExplainableReadOperation<AsyncBatchCursor<T>>, ExplainableReadOperation<BatchCursor<T>> {
    private final AggregateOperationImpl<T> wrapped;
    /**
     * Construct a new instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param pipeline the aggregation pipeline.
     * @param decoder the decoder for the result documents.
     */
    public AggregateOperation(final MongoNamespace namespace, final List<BsonDocument> pipeline, final Decoder<T> decoder) {
        this(namespace, pipeline, decoder, AggregationLevel.COLLECTION);
    }

    /**
     * Construct a new instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param pipeline the aggregation pipeline.
     * @param decoder the decoder for the result documents.
     * @param aggregationLevel the aggregation level
     * @since 3.10
     */
    public AggregateOperation(final MongoNamespace namespace, final List<BsonDocument> pipeline, final Decoder<T> decoder,
                              final AggregationLevel aggregationLevel) {
        this.wrapped = new AggregateOperationImpl<T>(namespace, pipeline, decoder, aggregationLevel);
    }

    /**
     * Gets the aggregation pipeline.
     *
     * @return the pipeline
     * @mongodb.driver.manual core/aggregation-introduction/#aggregation-pipelines Aggregation Pipeline
     */
    public List<BsonDocument> getPipeline() {
        return wrapped.getPipeline();
    }

    /**
     * Whether writing to temporary files is enabled. A null value indicates that it's unspecified.
     *
     * @return true if writing to temporary files is enabled
     * @mongodb.driver.manual reference/command/aggregate/ Aggregation
     */
    public Boolean getAllowDiskUse() {
        return wrapped.getAllowDiskUse();
    }

    /**
     * Enables writing to temporary files. A null value indicates that it's unspecified.
     *
     * @param allowDiskUse true if writing to temporary files is enabled
     * @return this
     * @mongodb.driver.manual reference/command/aggregate/ Aggregation
     */
    public AggregateOperation<T> allowDiskUse(final Boolean allowDiskUse) {
        wrapped.allowDiskUse(allowDiskUse);
        return this;
    }

    /**
     * Gets the number of documents to return per batch.  Default to 0, which indicates that the server chooses an appropriate batch size.
     *
     * @return the batch size, which may be null
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public Integer getBatchSize() {
        return wrapped.getBatchSize();
    }

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public AggregateOperation<T> batchSize(final Integer batchSize) {
        wrapped.batchSize(batchSize);
        return this;
    }

    /**
     * The maximum amount of time for the server to wait on new documents to satisfy a tailable cursor
     * query. This only applies to a TAILABLE_AWAIT cursor. When the cursor is not a TAILABLE_AWAIT cursor,
     * this option is ignored.
     *
     * A zero value will be ignored.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum await execution time in the given time unit
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    public long getMaxAwaitTime(final TimeUnit timeUnit) {
        return wrapped.getMaxAwaitTime(timeUnit);
    }

    /**
     * Sets the maximum await execution time on the server for this operation.
     *
     * @param maxAwaitTime  the max await time.  A value less than one will be ignored, and indicates that the driver should respect the
     *                      server's default value
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @mongodb.server.release 3.6
     */
    public AggregateOperation<T> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        wrapped.maxAwaitTime(maxAwaitTime, timeUnit);
        return this;
    }

    /**
     * Gets the maximum execution time on the server for this operation.  The default is 0, which places no limit on the execution time.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum execution time in the given time unit
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    public long getMaxTime(final TimeUnit timeUnit) {
        return wrapped.getMaxTime(timeUnit);
    }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    public AggregateOperation<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        wrapped.maxTime(maxTime, timeUnit);
        return this;
    }

    /**
     * Returns the collation options
     *
     * @return the collation options
     * @since 3.4
     * @mongodb.driver.manual reference/command/aggregate/ Aggregation
     * @mongodb.server.release 3.4
     */
    public Collation getCollation() {
        return wrapped.getCollation();
    }

    /**
     * Sets the collation options
     *
     * <p>A null value represents the server default.</p>
     * @param collation the collation options to use
     * @return this
     * @since 3.4
     * @mongodb.driver.manual reference/command/aggregate/ Aggregation
     * @mongodb.server.release 3.4
     */
    public AggregateOperation<T> collation(final Collation collation) {
        wrapped.collation(collation);
        return this;
    }

    /**
     * Returns the comment to send with the aggregate. The default is not to include a comment with the aggregation.
     *
     * @return the comment
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public String getComment() {
        return wrapped.getComment();
    }

    /**
     * Sets the comment to the aggregation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public AggregateOperation<T> comment(final String comment) {
        wrapped.comment(comment);
        return this;
    }

    /**
     * Enables retryable reads if a read fails due to a network error.
     *
     * @param retryReads true if reads should be retried
     * @return this
     * @since 3.11
     * @mongodb.server.release 3.6
     */
    public AggregateOperation<T> retryReads(final boolean retryReads) {
        wrapped.retryReads(retryReads);
        return this;
    }

    /**
     * Gets the value for retryable reads. The default is true.
     *
     * @return the retryable reads value
     * @since 3.11
     */
    public boolean getRetryReads() {
        return wrapped.getRetryReads();
    }

    /**
     * Returns the hint for which index to use. The default is not to set a hint.
     *
     * @return the hint
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public BsonDocument getHint() {
        BsonValue hint = wrapped.getHint();
        if (hint == null) {
            return null;
        }
        if (!hint.isDocument()) {
            throw new IllegalArgumentException("Hint is not a BsonDocument please use the #getHintBsonValue() method. ");
        }
        return hint.asDocument();
    }

    /**
     * Returns the hint BsonValue for which index to use. The default is not to set a hint.
     *
     * <p>Hints can either be a BsonString or a BsonDocument.</p>
     *
     * @return the hint
     * @since 3.8
     * @mongodb.server.release 3.6
     */
    public BsonValue getHintBsonValue() {
        return wrapped.getHint();
    }

    /**
     * Sets the hint for which index to use. A null value means no hint is set.
     *
     * @param hint the hint
     * @return this
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public AggregateOperation<T> hint(final BsonValue hint) {
        wrapped.hint(hint);
        return this;
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        return wrapped.execute(binding);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        wrapped.executeAsync(binding, callback);
    }

    /**
     * Gets an operation whose execution explains this operation.
     *
     * @param verbosity the explain verbosity
     * @return a read operation that when executed will explain this operation
     */
    public <R> ReadOperation<R> asExplainableOperation(@Nullable final ExplainVerbosity verbosity, final Decoder<R> resultDecoder) {
        return new CommandReadOperation<R>(getNamespace().getDatabaseName(),
                asExplainCommand(wrapped.getCommand(NoOpSessionContext.INSTANCE), verbosity),
                resultDecoder);
    }

    /**
     * Gets an operation whose execution explains this operation.
     *
     * @param verbosity the explain verbosity
     * @return a read operation that when executed will explain this operation
     */
    public <R> AsyncReadOperation<R> asAsyncExplainableOperation(@Nullable final ExplainVerbosity verbosity,
                                                                 final Decoder<R> resultDecoder) {
        return new CommandReadOperation<R>(getNamespace().getDatabaseName(),
                asExplainCommand(wrapped.getCommand(NoOpSessionContext.INSTANCE), verbosity),
                resultDecoder);
    }


    MongoNamespace getNamespace() {
        return wrapped.getNamespace();
    }

    Decoder<T> getDecoder() {
        return wrapped.getDecoder();
    }

    @Override
    public String toString() {
        return "AggregateOperation{"
                + "namespace=" + getNamespace()
                + ", pipeline=" + getPipeline()
                + ", decoder=" + getDecoder()
                + ", allowDiskUse=" + getAllowDiskUse()
                + ", batchSize=" + getBatchSize()
                + ", collation=" + getCollation()
                + ", comment=" + getComment()
                + ", hint=" + getHint()
                + ", maxAwaitTimeMS=" + getMaxAwaitTime(TimeUnit.MILLISECONDS)
                + ", maxTimeMS=" + getMaxTime(TimeUnit.MILLISECONDS)
                + "}";
    }
}
