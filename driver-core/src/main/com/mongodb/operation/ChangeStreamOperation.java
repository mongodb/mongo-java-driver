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

package com.mongodb.operation;

import com.mongodb.MongoNamespace;
import com.mongodb.async.AsyncAggregateResponseBatchCursor;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.operation.OperationHelper.AsyncCallableWithSource;
import com.mongodb.operation.OperationHelper.CallableWithSource;
import com.mongodb.session.SessionContext;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.codecs.Decoder;
import org.bson.codecs.RawBsonDocumentCodec;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.OperationHelper.withAsyncReadConnection;
import static com.mongodb.operation.OperationHelper.withReadConnectionSource;

/**
 * An operation that executes an {@code $changeStream} aggregation.
 *
 * @param <T> the operations result type.
 * @mongodb.driver.manual aggregation/ Aggregation
 * @mongodb.server.release 2.6
 * @since 3.6
 */
@Deprecated
public class ChangeStreamOperation<T> implements AsyncReadOperation<AsyncBatchCursor<T>>, ReadOperation<BatchCursor<T>> {
    private static final RawBsonDocumentCodec RAW_BSON_DOCUMENT_CODEC = new RawBsonDocumentCodec();
    private final AggregateOperationImpl<RawBsonDocument> wrapped;
    private final FullDocument fullDocument;
    private final Decoder<T> decoder;
    private final ChangeStreamLevel changeStreamLevel;

    private BsonDocument resumeAfter;
    private BsonDocument startAfter;
    private BsonTimestamp startAtOperationTime;

    /**
     * Construct a new instance.
     *
     * @param namespace    the database and collection namespace for the operation.
     * @param fullDocument the fullDocument value
     * @param pipeline     the aggregation pipeline.
     * @param decoder      the decoder for the result documents.
     */
    public ChangeStreamOperation(final MongoNamespace namespace, final FullDocument fullDocument, final List<BsonDocument> pipeline,
                                 final Decoder<T> decoder) {
        this(namespace, fullDocument, pipeline, decoder, ChangeStreamLevel.COLLECTION);
    }

    /**
     * Construct a new instance.
     *
     * @param namespace         the database and collection namespace for the operation.
     * @param fullDocument      the fullDocument value
     * @param pipeline          the aggregation pipeline.
     * @param decoder           the decoder for the result documents.
     * @param changeStreamLevel the level at which the change stream is observing
     *
     * @since 3.8
     */
    public ChangeStreamOperation(final MongoNamespace namespace, final FullDocument fullDocument, final List<BsonDocument> pipeline,
                                 final Decoder<T> decoder, final ChangeStreamLevel changeStreamLevel) {
        this.wrapped = new AggregateOperationImpl<RawBsonDocument>(namespace, pipeline, RAW_BSON_DOCUMENT_CODEC,
                getAggregateTarget(), getPipelineCreator());
        this.fullDocument = notNull("fullDocument", fullDocument);
        this.decoder = notNull("decoder", decoder);
        this.changeStreamLevel = notNull("changeStreamLevel", changeStreamLevel);
    }

    /**
     * @return the namespace for this operation
     */
    public MongoNamespace getNamespace() {
        return wrapped.getNamespace();
    }

    /**
     * @return the decoder for this operation
     */
    public Decoder<T> getDecoder() {
        return decoder;
    }

    /**
     * Returns the fullDocument value, in 3.6
     *
     * @return the fullDocument value
     */
    public FullDocument getFullDocument() {
        return fullDocument;
    }

    /**
     * Returns the logical starting point for the new change stream.
     *
     * <p>A null value represents the server default.</p>
     *
     * @return the resumeAfter
     * @deprecated use {@link #getResumeAfter()} instead
     */
    @Deprecated
    public BsonDocument getResumeToken() {
        return resumeAfter;
    }

    /**
     * Returns the logical starting point for the new change stream.
     *
     * <p>A null value represents the server default.</p>
     *
     * @return the resumeAfter resumeToken
     * @since 3.11
     * @mongodb.server.release 4.2
     */
    public BsonDocument getResumeAfter() {
        return resumeAfter;
    }

    /**
     * Sets the logical starting point for the new change stream.
     *
     * @param resumeAfter the resumeToken
     * @return this
     */
    public ChangeStreamOperation<T> resumeAfter(final BsonDocument resumeAfter) {
        this.resumeAfter = resumeAfter;
        return this;
    }

    /**
     * Returns the logical starting point for the new change stream returning the first notification after the token.
     *
     * <p>A null value represents the server default.</p>
     *
     * @return the startAfter resumeToken
     * @since 3.11
     * @mongodb.server.release 4.2
     */
    public BsonDocument getStartAfter() {
        return startAfter;
    }

    /**
     * Similar to {@code resumeAfter}, this option takes a resume token and starts a
     * new change stream returning the first notification after the token.
     *
     * <p>This will allow users to watch collections that have been dropped and recreated
     * or newly renamed collections without missing any notifications.</p>
     *
     * <p>Note: The server will report an error if both {@code startAfter} and {@code resumeAfter} are specified.</p>
     *
     * @param startAfter the startAfter resumeToken
     * @return this
     * @since 3.11
     * @mongodb.server.release 4.2
     * @mongodb.driver.manual changeStreams/#change-stream-start-after
     */
    public ChangeStreamOperation<T> startAfter(final BsonDocument startAfter) {
        this.startAfter = startAfter;
        return this;
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
    public ChangeStreamOperation<T> batchSize(final Integer batchSize) {
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
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    public long getMaxAwaitTime(final TimeUnit timeUnit) {
        return wrapped.getMaxAwaitTime(timeUnit);
    }

    /**
     * Sets the maximum await execution time on the server for this operation.
     *
     * @param maxAwaitTime the max await time.  A value less than one will be ignored, and indicates that the driver should respect the
     *                     server's default value
     * @param timeUnit     the time unit, which may not be null
     * @return this
     */
    public ChangeStreamOperation<T> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        wrapped.maxAwaitTime(maxAwaitTime, timeUnit);
        return this;
    }

    /**
     * Returns the collation options
     *
     * @return the collation options
     * @mongodb.driver.manual reference/command/aggregate/ Aggregation
     */
    public Collation getCollation() {
        return wrapped.getCollation();
    }

    /**
     * Sets the collation options
     *
     * <p>A null value represents the server default.</p>
     *
     * @param collation the collation options to use
     * @return this
     * @mongodb.driver.manual reference/command/aggregate/ Aggregation
     */
    public ChangeStreamOperation<T> collation(final Collation collation) {
        wrapped.collation(collation);
        return this;
    }

    /**
     * The change stream will only provides changes that occurred after the specified timestamp.
     *
     * <p>Any command run against the server will return an operation time that can be used here.</p>
     * <p>The default value is an operation time obtained from the server before the change stream was created.</p>
     *
     * @param startAtOperationTime the start at operation time
     * @return this
     * @since 3.8
     * @mongodb.server.release 4.0
     * @mongodb.driver.manual reference/method/db.runCommand/
     */
    public ChangeStreamOperation<T> startAtOperationTime(final BsonTimestamp startAtOperationTime) {
        this.startAtOperationTime = startAtOperationTime;
        return this;
    }

    /**
     * Returns the start at operation time
     *
     * @return the start at operation time
     * @since 3.8
     * @mongodb.server.release 4.0
     */
    public BsonTimestamp getStartAtOperationTime() {
        return startAtOperationTime;
    }

    /**
     * Enables retryable reads if a read fails due to a network error.
     *
     * @param retryReads true if reads should be retried
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     * @since 3.11
     */
    public ChangeStreamOperation<T> retryReads(final boolean retryReads) {
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

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        return withReadConnectionSource(binding, new CallableWithSource<BatchCursor<T>>() {
            @Override
            public BatchCursor<T> call(final ConnectionSource source) {
                AggregateResponseBatchCursor<RawBsonDocument> cursor =
                        (AggregateResponseBatchCursor<RawBsonDocument>) wrapped.execute(binding);
                return new ChangeStreamBatchCursor<T>(ChangeStreamOperation.this, cursor, binding,
                        setChangeStreamOptions(cursor.getPostBatchResumeToken(), cursor.getOperationTime(),
                                source.getServerDescription().getMaxWireVersion(), cursor.isFirstBatchEmpty()));
            }
        });
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        wrapped.executeAsync(binding, new SingleResultCallback<AsyncBatchCursor<RawBsonDocument>>() {
            @Override
            public void onResult(final AsyncBatchCursor<RawBsonDocument> result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    final AsyncAggregateResponseBatchCursor<RawBsonDocument> cursor =
                            (AsyncAggregateResponseBatchCursor<RawBsonDocument>) result;
                    withAsyncReadConnection(binding, new AsyncCallableWithSource() {
                        @Override
                        public void call(final AsyncConnectionSource source, final Throwable t) {
                            if (t != null) {
                                callback.onResult(null, t);
                            } else {
                                callback.onResult(new AsyncChangeStreamBatchCursor<T>(ChangeStreamOperation.this, cursor, binding,
                                        setChangeStreamOptions(cursor.getPostBatchResumeToken(), cursor.getOperationTime(),
                                                source.getServerDescription().getMaxWireVersion(), cursor.isFirstBatchEmpty())), null);
                            }
                            source.release();
                        }
                    });
                }
            }
        });
    }

    private BsonDocument setChangeStreamOptions(final BsonDocument postBatchResumeToken, final BsonTimestamp operationTime,
                                                final int maxWireVersion, final boolean firstBatchEmpty) {
        BsonDocument resumeToken = null;
        if (startAfter != null) {
            resumeToken = startAfter;
        } else if (resumeAfter != null) {
            resumeToken = resumeAfter;
        } else if (startAtOperationTime == null && postBatchResumeToken == null && firstBatchEmpty && maxWireVersion >= 7) {
            startAtOperationTime = operationTime;
        }
        return resumeToken;
    }

    /**
     * Set the change stream operation options for a resumeable operation.
     *
     * @param resumeToken the resume token cached prior to resume
     * @param maxWireVersion the max wire version reported by the server description
     * @since 3.11
     */
    public void setChangeStreamOptionsForResume(final BsonDocument resumeToken, final int maxWireVersion) {
        startAfter = null;
        if (resumeToken != null) {
            startAtOperationTime = null;
            resumeAfter = resumeToken;
        } else if (startAtOperationTime != null && maxWireVersion >= 7) {
            resumeAfter = null;
        } else {
            resumeAfter = null;
            startAtOperationTime = null;
        }
    }

    private AggregateOperationImpl.AggregateTarget getAggregateTarget() {
        return new AggregateOperationImpl.AggregateTarget() {
            @Override
            public BsonValue create() {
                return changeStreamLevel == ChangeStreamLevel.COLLECTION
                        ? new BsonString(getNamespace().getCollectionName()) : new BsonInt32(1);
            }
        };
    }

    private AggregateOperationImpl.PipelineCreator getPipelineCreator() {
        return new AggregateOperationImpl.PipelineCreator() {
            @Override
            public BsonArray create(final ConnectionDescription description, final SessionContext sessionContext) {
                List<BsonDocument> changeStreamPipeline = new ArrayList<BsonDocument>();
                BsonDocument changeStream = new BsonDocument("fullDocument", new BsonString(fullDocument.getValue()));

                if (changeStreamLevel == ChangeStreamLevel.CLIENT) {
                    changeStream.append("allChangesForCluster", BsonBoolean.TRUE);
                }

                if (resumeAfter != null) {
                    changeStream.append("resumeAfter", resumeAfter);
                }
                if (startAfter != null) {
                    changeStream.append("startAfter", startAfter);
                }
                if (startAtOperationTime != null) {
                    changeStream.append("startAtOperationTime", startAtOperationTime);
                }

                changeStreamPipeline.add(new BsonDocument("$changeStream", changeStream));
                changeStreamPipeline.addAll(getPipeline());
                return new BsonArray(changeStreamPipeline);
            }
        };
    }
}
