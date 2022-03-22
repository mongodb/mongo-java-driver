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

package com.mongodb.reactivestreams.client;

import com.mongodb.ExplainVerbosity;
import com.mongodb.client.model.Collation;
import com.mongodb.lang.Nullable;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.concurrent.TimeUnit;

/**
 * Publisher for aggregate.
 *
 * @param <TResult> The type of the result.
 * @since 1.0
 */
public interface AggregatePublisher<TResult> extends Publisher<TResult> {

    /**
     * Enables writing to temporary files. A null value indicates that it's unspecified.
     *
     * @param allowDiskUse true if writing to temporary files is enabled
     * @return this
     * @mongodb.driver.manual reference/command/aggregate/ Aggregation
     */
    AggregatePublisher<TResult> allowDiskUse(@Nullable Boolean allowDiskUse);

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    AggregatePublisher<TResult> maxTime(long maxTime, TimeUnit timeUnit);

    /**
     * The maximum amount of time for the server to wait on new documents to satisfy a {@code $changeStream} aggregation.
     *
     * A zero value will be ignored.
     *
     * @param maxAwaitTime  the max await time
     * @param timeUnit the time unit to return the result in
     * @return the maximum await execution time in the given time unit
     * @mongodb.server.release 3.6
     * @since 1.6
     */
    AggregatePublisher<TResult> maxAwaitTime(long maxAwaitTime, TimeUnit timeUnit);

    /**
     * Sets the bypass document level validation flag.
     *
     * <p>Note: This only applies when an $out stage is specified</p>.
     *
     * @param bypassDocumentValidation If true, allows the write to opt-out of document level validation.
     * @return this
     * @since 1.2
     * @mongodb.driver.manual reference/command/aggregate/ Aggregation
     * @mongodb.server.release 3.2
     */
    AggregatePublisher<TResult> bypassDocumentValidation(@Nullable Boolean bypassDocumentValidation);

    /**
     * Aggregates documents according to the specified aggregation pipeline, which must end with a $out stage.
     *
     * @return an empty publisher that indicates when the operation has completed
     * @mongodb.driver.manual aggregation/ Aggregation
     */
    Publisher<Void> toCollection();

    /**
     * Sets the collation options
     *
     * <p>A null value represents the server default.</p>
     * @param collation the collation options to use
     * @return this
     * @since 1.3
     * @mongodb.server.release 3.4
     */
    AggregatePublisher<TResult> collation(@Nullable Collation collation);

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    AggregatePublisher<TResult> comment(@Nullable String comment);

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * <p>The comment can be any valid BSON type for server versions 4.4 and above.
     * Server versions between 3.6 and 4.2 only support string as comment,
     * and providing a non-string type will result in a server-side error.
     *
     * @param comment the comment
     * @return this
     * @since 4.6
     * @mongodb.server.release 3.6
     */
    AggregatePublisher<TResult> comment(@Nullable BsonValue comment);

    /**
     * Sets the hint for which index to use. A null value means no hint is set.
     *
     * @param hint the hint
     * @return this
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    AggregatePublisher<TResult> hint(@Nullable Bson hint);

    /**
     * Sets the hint for which index to use. A null value means no hint is set.
     *
     * <p>Note: If {@link AggregatePublisher#hint(Bson)} is set that will be used instead of any hint string.</p>
     *
     * @param hint the name of the index which should be used for the operation
     * @return this
     * @since 4.4
     */
    AggregatePublisher<TResult> hintString(@Nullable String hint);

    /**
     * Add top-level variables to the aggregation.
     * <p>
     * For MongoDB 5.0+, the aggregate command accepts a {@code let} option. This option is a document consisting of zero or more
     * fields representing variables that are accessible to the aggregation pipeline.  The key is the name of the variable and the value is
     * a constant in the aggregate expression language. Each parameter name is then usable to access the value of the corresponding
     * expression with the "$$" syntax within aggregate expression contexts which may require the use of $expr or a pipeline.
     * </p>
     *
     * @param variables the variables
     * @return this
     * @since 4.3
     * @mongodb.server.release 5.0
     */
    AggregatePublisher<TResult> let(@Nullable Bson variables);

    /**
     * Sets the number of documents to return per batch.
     *
     * <p>Overrides the {@link org.reactivestreams.Subscription#request(long)} value for setting the batch size, allowing for fine-grained
     * control over the underlying cursor.</p>
     *
     * @param batchSize the batch size
     * @return this
     * @since 1.8
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    AggregatePublisher<TResult> batchSize(int batchSize);

    /**
     * Helper to return a publisher limited to the first result.
     *
     * @return a Publisher which will contain a single item.
     * @since 1.8
     */
    Publisher<TResult> first();

    /**
     * Explain the execution plan for this operation with the server's default verbosity level
     *
     * @return the execution plan
     * @since 4.2
     * @mongodb.driver.manual reference/command/explain/
     * @mongodb.server.release 3.6
     */
    Publisher<Document> explain();

    /**
     * Explain the execution plan for this operation with the given verbosity level
     *
     * @param verbosity the verbosity of the explanation
     * @return the execution plan
     * @since 4.2
     * @mongodb.driver.manual reference/command/explain/
     * @mongodb.server.release 3.6
     */
    Publisher<Document> explain(ExplainVerbosity verbosity);

    /**
     * Explain the execution plan for this operation with the server's default verbosity level
     *
     * @param <E> the type of the document class
     * @param explainResultClass the document class to decode into
     * @return the execution plan
     * @since 4.2
     * @mongodb.driver.manual reference/command/explain/
     * @mongodb.server.release 3.6
     */
    <E> Publisher<E> explain(Class<E> explainResultClass);

    /**
     * Explain the execution plan for this operation with the given verbosity level
     *
     * @param <E> the type of the document class
     * @param explainResultClass the document class to decode into
     * @param verbosity            the verbosity of the explanation
     * @return the execution plan
     * @since 4.2
     * @mongodb.driver.manual reference/command/explain/
     * @mongodb.server.release 3.6
     */
    <E> Publisher<E> explain(Class<E> explainResultClass, ExplainVerbosity verbosity);
}
