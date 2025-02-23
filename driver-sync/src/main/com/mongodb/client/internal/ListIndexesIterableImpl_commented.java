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

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.ReadOperation;
import com.mongodb.internal.operation.SyncOperations;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Implementation of an iterable for listing MongoDB indexes with configurable timeout and batch processing.
 * This class provides a fluent API for listing indexes with support for operation timeouts, batch sizes,
 * and read preferences.
 *
 * <p>Key features:</p>
 * <ul>
 *     <li>Configurable operation timeouts to prevent long-running index listings</li>
 *     <li>Batch size control for memory and network optimization</li>
 *     <li>Support for operation comments for monitoring and debugging</li>
 *     <li>Integration with MongoDB's operation execution framework</li>
 * </ul>
 *
 * <p>Timeout handling:</p>
 * <ul>
 *     <li>Supports both maxTime and timeoutMode for operation timeout control</li>
 *     <li>maxTime sets a specific timeout duration for the operation</li>
 *     <li>timeoutMode allows for more granular timeout behavior configuration</li>
 * </ul>
 *
 * <p>Batch processing:</p>
 * <ul>
 *     <li>Configurable batch sizes for efficient network utilization</li>
 *     <li>Batches are processed lazily as the cursor is iterated</li>
 *     <li>Default batch size is determined by the server</li>
 * </ul>
 *
 * <p>Performance considerations:</p>
 * <ul>
 *     <li>Index listing operations are typically fast but may slow with many indexes</li>
 *     <li>Batch size tuning can optimize memory usage and network round trips</li>
 *     <li>Timeout configuration prevents resource exhaustion</li>
 * </ul>
 *
 * <p>Thread safety: This class is not thread-safe and should not be used concurrently
 * from multiple threads.</p>
 */
class ListIndexesIterableImpl<TResult> extends MongoIterableImpl<TResult> implements ListIndexesIterable<TResult> {
    private final SyncOperations<BsonDocument> operations;
    private final Class<TResult> resultClass;
    private long maxTimeMS;
    private BsonValue comment;

    /**
     * Creates a new iterable for listing indexes with the specified configuration.
     *
     * @param clientSession optional client session for the operation
     * @param namespace the namespace (database.collection) to list indexes from
     * @param resultClass the class to decode each index document into
     * @param codecRegistry the codec registry for decoding index documents
     * @param readPreference the read preference to use for the operation
     * @param executor the operation executor
     * @param retryReads whether to retry read operations
     * @param timeoutSettings the timeout settings for the operation
     */
    ListIndexesIterableImpl(@Nullable final ClientSession clientSession, final MongoNamespace namespace, final Class<TResult> resultClass,
            final CodecRegistry codecRegistry, final ReadPreference readPreference, final OperationExecutor executor,
            final boolean retryReads, final TimeoutSettings timeoutSettings) {
        super(clientSession, executor, ReadConcern.DEFAULT, readPreference, retryReads, timeoutSettings);
        this.operations = new SyncOperations<>(namespace, BsonDocument.class, readPreference, codecRegistry, retryReads, timeoutSettings);
        this.resultClass = notNull("resultClass", resultClass);
    }

    /**
     * Sets the maximum time for this operation to run on the server.
     *
     * @param maxTime the maximum time to allow the operation to run
     * @param timeUnit the time unit for maxTime
     * @return this
     * @throws IllegalArgumentException if timeUnit is null
     */
    @Override
    public ListIndexesIterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     */
    @Override
    public ListIndexesIterable<TResult> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    /**
     * Sets the timeout mode for the operation.
     * This provides more granular control over timeout behavior than maxTime.
     *
     * @param timeoutMode the timeout mode to use
     * @return this
     */
    @Override
    public ListIndexesIterable<TResult> timeoutMode(final TimeoutMode timeoutMode) {
        super.timeoutMode(timeoutMode);
        return this;
    }

    /**
     * Sets a string comment to help trace this operation through the database profiler, currentOp,
     * and logs.
     *
     * @param comment the comment to set
     * @return this
     */
    @Override
    public ListIndexesIterable<TResult> comment(@Nullable final String comment) {
        this.comment = comment != null ? new BsonString(comment) : null;
        return this;
    }

    /**
     * Sets a BSON comment to help trace this operation through the database profiler, currentOp,
     * and logs.
     *
     * @param comment the comment to set
     * @return this
     */
    @Override
    public ListIndexesIterable<TResult> comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    /**
     * Creates a read operation that will execute the index listing.
     * This method integrates with MongoDB's operation execution framework.
     *
     * @return the read operation
     */
    @Override
    public ReadOperation<BatchCursor<TResult>> asReadOperation() {
        return operations.listIndexes(resultClass, getBatchSize(), comment, getTimeoutMode());
    }

    /**
     * Gets an executor configured with the current timeout settings.
     *
     * @return the operation executor
     */
    protected OperationExecutor getExecutor() {
        return getExecutor(operations.createTimeoutSettings(maxTimeMS));
    }
}
