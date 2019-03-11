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

import com.mongodb.client.model.Collation;
import com.mongodb.lang.Nullable;
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
     * Aggregates documents according to the specified aggregation pipeline, which must end with a $out or $merge stage.
     *
     * @throws IllegalStateException if the pipeline does not end with a $out or $merge stage
     * @mongodb.driver.manual reference/operator/aggregation/out/ $out stage
     * @mongodb.driver.manual reference/operator/aggregation/merge/ $merge stage
     * @since 3.4
     */
    void toCollection();

    /**
     * Enables writing to temporary files. A null value indicates that it's unspecified.
     *
     * @param allowDiskUse true if writing to temporary files is enabled
     * @return this
     * @mongodb.driver.manual reference/command/aggregate/ Aggregation
     * @mongodb.server.release 2.6
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
     *
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
     * Sets the comment to the aggregation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    AggregateIterable<TResult> comment(@Nullable String comment);

    /**
     * Sets the hint for which index to use. A null value means no hint is set.
     *
     * @param hint the hint
     * @return this
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    AggregateIterable<TResult> hint(@Nullable Bson hint);
}
