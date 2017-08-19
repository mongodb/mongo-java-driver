/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.operation;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ReadBinding;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.FullDocument;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.bson.codecs.Decoder;
import org.bson.codecs.RawBsonDocumentCodec;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * An operation that executes an {@code $changeStream} aggregation.
 *
 * @param <T> the operations result type.
 * @mongodb.driver.manual aggregation/ Aggregation
 * @mongodb.server.release 2.6
 * @since 3.6
 */
public class ChangeStreamOperation<T> implements AsyncReadOperation<AsyncBatchCursor<T>>, ReadOperation<BatchCursor<T>> {
    private static final RawBsonDocumentCodec RAW_BSON_DOCUMENT_CODEC = new RawBsonDocumentCodec();
    private final MongoNamespace namespace;
    private final FullDocument fullDocument;
    private final List<BsonDocument> pipeline;
    private final Decoder<T> decoder;

    private BsonDocument resumeToken;
    private Integer batchSize;
    private Collation collation;
    private long maxAwaitTimeMS;
    private ReadConcern readConcern = ReadConcern.DEFAULT;

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
        this.namespace = notNull("namespace", namespace);
        this.fullDocument = notNull("fullDocument", fullDocument);
        this.pipeline = notNull("pipeline", pipeline);
        this.decoder = notNull("decoder", decoder);
    }

    /**
     * @return the namespace for this operation
     */
    public MongoNamespace getNamespace() {
        return namespace;
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
     * <p>
     * <p>A null value represents the server default.</p>
     *
     * @return the resumeAfter
     */
    public BsonDocument getResumeToken() {
        return resumeToken;
    }

    /**
     * Sets the logical starting point for the new change stream.
     *
     * @param resumeToken the resumeToken
     * @return this
     */
    public ChangeStreamOperation<T> resumeAfter(final BsonDocument resumeToken) {
        this.resumeToken = resumeToken;
        return this;
    }

    /**
     * Gets the aggregation pipeline.
     *
     * @return the pipeline
     * @mongodb.driver.manual core/aggregation-introduction/#aggregation-pipelines Aggregation Pipeline
     */
    public List<BsonDocument> getPipeline() {
        return pipeline;
    }

    /**
     * Gets the number of documents to return per batch.  Default to 0, which indicates that the server chooses an appropriate batch size.
     *
     * @return the batch size, which may be null
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public Integer getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public ChangeStreamOperation<T> batchSize(final Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * The maximum amount of time for the server to wait on new documents to satisfy a tailable cursor
     * query. This only applies to a TAILABLE_AWAIT cursor. When the cursor is not a TAILABLE_AWAIT cursor,
     * this option is ignored.
     * <p>
     * A zero value will be ignored.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum await execution time in the given time unit
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    public long getMaxAwaitTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxAwaitTimeMS, MILLISECONDS);
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
        notNull("timeUnit", timeUnit);
        isTrueArgument("maxAwaitTime >= 0", maxAwaitTime >= 0);
        this.maxAwaitTimeMS = MILLISECONDS.convert(maxAwaitTime, timeUnit);
        return this;
    }

    /**
     * Gets the read concern
     *
     * @return the read concern
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    public ReadConcern getReadConcern() {
        return readConcern;
    }

    /**
     * Sets the read concern
     *
     * @param readConcern the read concern
     * @return this
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    public ChangeStreamOperation<T> readConcern(final ReadConcern readConcern) {
        this.readConcern = notNull("readConcern", readConcern);
        return this;
    }

    /**
     * Returns the collation options
     *
     * @return the collation options
     * @mongodb.driver.manual reference/command/aggregate/ Aggregation
     */
    public Collation getCollation() {
        return collation;
    }

    /**
     * Sets the collation options
     * <p>
     * <p>A null value represents the server default.</p>
     *
     * @param collation the collation options to use
     * @return this
     * @mongodb.driver.manual reference/command/aggregate/ Aggregation
     */
    public ChangeStreamOperation<T> collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        return new ChangeStreamBatchCursor<T>(this, createAggregateOperation().execute(binding), binding);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        createAggregateOperation().executeAsync(binding, new SingleResultCallback<AsyncBatchCursor<RawBsonDocument>>() {
            @Override
            public void onResult(final AsyncBatchCursor<RawBsonDocument> result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    callback.onResult(new AsyncChangeStreamBatchCursor<T>(ChangeStreamOperation.this, result, binding),
                            null);
                }
            }
        });
    }

    private AggregateOperation<RawBsonDocument> createAggregateOperation() {
        List<BsonDocument> changeStreamPipeline = new ArrayList<BsonDocument>();
        BsonDocument changeStream = new BsonDocument("fullDocument", new BsonString(fullDocument.getValue()));
        if (resumeToken != null) {
            changeStream.append("resumeAfter", resumeToken);
        }
        changeStreamPipeline.add(new BsonDocument("$changeStream", changeStream));
        changeStreamPipeline.addAll(pipeline);
        return new AggregateOperation<RawBsonDocument>(namespace, changeStreamPipeline, RAW_BSON_DOCUMENT_CODEC)
                .maxAwaitTime(maxAwaitTimeMS, MILLISECONDS)
                .readConcern(readConcern)
                .batchSize(batchSize)
                .collation(collation);
    }

}
