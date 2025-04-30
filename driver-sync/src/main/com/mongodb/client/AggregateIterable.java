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

package com.mongodb.client;

import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoNamespace;
import com.mongodb.annotations.Alpha;
import com.mongodb.annotations.Reason;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.MergeOptions;
import com.mongodb.lang.Nullable;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

/**
 * Iterable for aggregate.
 *
 * @param <TResult> The type of the result.
 * @mongodb.driver.manual reference/command/aggregate/ Aggregation
 * @since 3.0
 */
public interface AggregateIterable<TResult> extends MongoIterable<TResult> {

    /**
     * Aggregates documents according to the specified aggregation pipeline, which must end with an
     * {@link Aggregates#out(String, String) $out} or {@link Aggregates#merge(MongoNamespace, MergeOptions) $merge} stage.
     * This method is the preferred alternative to {@link #iterator()}, {@link #cursor()},
     * because this method does what is explicitly requested without executing implicit operations.
     *
     * @throws IllegalStateException if the pipeline does not end with an {@code $out} or {@code $merge} stage
     * @mongodb.driver.manual reference/operator/aggregation/out/ $out stage
     * @mongodb.driver.manual reference/operator/aggregation/merge/ $merge stage
     * @since 3.4
     */
    void toCollection();

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     * <ul>
     *     <li>
     *     If the aggregation pipeline ends with an {@link Aggregates#out(String, String) $out} or
     *     {@link Aggregates#merge(MongoNamespace, MergeOptions) $merge} stage,
     *     then {@linkplain MongoCollection#find() finds all} documents in the affected namespace and returns a {@link MongoCursor}
     *     over them. You may want to use {@link #toCollection()} instead.</li>
     *     <li>
     *     Otherwise, returns a {@link MongoCursor} producing no elements.</li>
     * </ul>
     */
    @Override
    MongoCursor<TResult> iterator();

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     * <ul>
     *     <li>
     *     If the aggregation pipeline ends with an {@link Aggregates#out(String, String) $out} or
     *     {@link Aggregates#merge(MongoNamespace, MergeOptions) $merge} stage,
     *     then {@linkplain MongoCollection#find() finds all} documents in the affected namespace and returns a {@link MongoCursor}
     *     over them. You may want to use {@link #toCollection()} instead.</li>
     *     <li>
     *     Otherwise, returns a {@link MongoCursor} producing no elements.</li>
     * </ul>
     */
    @Override
    MongoCursor<TResult> cursor();

    /**
     * Enables writing to temporary files. A null value indicates that it's unspecified.
     *
     * @param allowDiskUse true if writing to temporary files is enabled
     * @return this
     * @mongodb.driver.manual reference/command/aggregate/ Aggregation
     */
    AggregateIterable<TResult> allowDiskUse(@Nullable Boolean allowDiskUse);

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    AggregateIterable<TResult> batchSize(int batchSize);

    /**
     * Sets the timeoutMode for the cursor.
     *
     * <p>
     *     Requires the {@code timeout} to be set, either in the {@link com.mongodb.MongoClientSettings},
     *     via {@link MongoDatabase} or via {@link MongoCollection}
     * </p>
     * <p>
     *     If the {@code timeout} is set then:
     *     <ul>
     *      <li>For non-tailable cursors, the default value of timeoutMode is {@link TimeoutMode#CURSOR_LIFETIME}</li>
     *      <li>For tailable cursors, the default value of timeoutMode is {@link TimeoutMode#ITERATION} and its an error
     *      to configure it as: {@link TimeoutMode#CURSOR_LIFETIME}</li>
     *     </ul>
     * <p>
     *     Will error if the timeoutMode is set to {@link TimeoutMode#ITERATION} and the pipeline contains either
     *     an {@code $out} or a {@code $merge} stage.
     * </p>
     * @param timeoutMode the timeout mode
     * @return this
     * @since 5.2
     */
    @Alpha(Reason.CLIENT)
    AggregateIterable<TResult> timeoutMode(TimeoutMode timeoutMode);

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    AggregateIterable<TResult> maxTime(long maxTime, TimeUnit timeUnit);

    /**
     * The maximum amount of time for the server to wait on new documents to satisfy a {@code $changeStream} aggregation.
     * <p>
     * A zero value will be ignored.
     *
     * @param maxAwaitTime  the max await time
     * @param timeUnit the time unit to return the result in
     * @return the maximum await execution time in the given time unit
     * @mongodb.server.release 3.6
     * @since 3.6
     */
    AggregateIterable<TResult> maxAwaitTime(long maxAwaitTime, TimeUnit timeUnit);

    /**
     * Sets the bypass document level validation flag.
     *
     * <p>Note: This only applies when an $out or $merge stage is specified</p>.
     *
     * @param bypassDocumentValidation If true, allows the write to opt-out of document level validation.
     * @return this
     * @since 3.2
     * @mongodb.driver.manual reference/command/aggregate/ Aggregation
     * @mongodb.server.release 3.2
     */
    AggregateIterable<TResult> bypassDocumentValidation(@Nullable Boolean bypassDocumentValidation);

    /**
     * Sets the collation options
     *
     * <p>A null value represents the server default.</p>
     * @param collation the collation options to use
     * @return this
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    AggregateIterable<TResult> collation(@Nullable Collation collation);

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    AggregateIterable<TResult> comment(@Nullable String comment);

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
    AggregateIterable<TResult> comment(@Nullable BsonValue comment);

    /**
     * Sets the hint for which index to use. A null value means no hint is set.
     *
     * @param hint the hint
     * @return this
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    AggregateIterable<TResult> hint(@Nullable Bson hint);

    /**
     * Sets the hint to apply.
     *
     * <p>Note: If {@link AggregateIterable#hint(Bson)} is set that will be used instead of any hint string.</p>
     *
     * @param hint the name of the index which should be used for the operation
     * @return this
     * @since 4.4
     */
    AggregateIterable<TResult> hintString(@Nullable String hint);

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
    AggregateIterable<TResult> let(@Nullable Bson variables);

    /**
     * Explain the execution plan for this operation with the server's default verbosity level
     *
     * @return the execution plan
     * @since 4.2
     * @mongodb.driver.manual reference/command/explain/
     * @mongodb.server.release 3.6
     */
    Document explain();

    /**
     * Explain the execution plan for this operation with the given verbosity level
     *
     * @param verbosity the verbosity of the explanation
     * @return the execution plan
     * @since 4.2
     * @mongodb.driver.manual reference/command/explain/
     * @mongodb.server.release 3.6
     */
    Document explain(ExplainVerbosity verbosity);

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
    <E> E explain(Class<E> explainResultClass);

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
    <E> E explain(Class<E> explainResultClass, ExplainVerbosity verbosity);
}
