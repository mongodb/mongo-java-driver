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
package com.mongodb.kotlin.client.coroutine

import com.mongodb.client.cursor.TimeoutMode
import com.mongodb.client.model.Collation
import com.mongodb.reactivestreams.client.DistinctPublisher
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.reactive.asFlow
import org.bson.BsonValue
import org.bson.conversions.Bson

/**
 * Flow implementation for distinct operations.
 *
 * @param T The type of the result.
 * @see [Distinct command](https://www.mongodb.com/docs/manual/reference/command/distinct/)
 */
public class DistinctFlow<T : Any>(private val wrapped: DistinctPublisher<T>) : Flow<T> by wrapped.asFlow() {

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @see [Batch Size](https://www.mongodb.com/docs/manual/reference/method/cursor.batchSize/#cursor.batchSize)
     */
    public fun batchSize(batchSize: Int): DistinctFlow<T> = apply { wrapped.batchSize(batchSize) }

    /**
     * Sets the timeoutMode for the cursor.
     *
     * Requires the `timeout` to be set, either in the [com.mongodb.MongoClientSettings], via [MongoDatabase] or via
     * [MongoCollection]
     *
     * @param timeoutMode the timeout mode
     * @return this
     * @since 4.x
     */
    public fun timeoutMode(timeoutMode: TimeoutMode): DistinctFlow<T> = apply { wrapped.timeoutMode(timeoutMode) }

    /**
     * Sets the query filter to apply to the query.
     *
     * @param filter the filter, which may be null.
     * @return this
     * @see [Filter results](https://www.mongodb.com/docs/manual/reference/method/db.collection.find/)
     */
    public fun filter(filter: Bson?): DistinctFlow<T> = apply { wrapped.filter(filter) }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * **NOTE**: The maximum execution time option is deprecated. Prefer using the operation execution timeout
     * configuration options available at the following levels:
     * - [com.mongodb.MongoClientSettings.Builder.timeout]
     * - [MongoDatabase.withTimeout]
     * - [MongoCollection.withTimeout]
     * - [ClientSession]
     *
     * When executing an operation, any explicitly set timeout at these levels takes precedence, rendering this maximum
     * execution time irrelevant. If no timeout is specified at these levels, the maximum execution time will be used.
     *
     * @param maxTime the max time
     * @param timeUnit the time unit, which defaults to Milliseconds
     * @return this
     */
    @Deprecated("Prefer using the operation execution timeout configuration option", level = DeprecationLevel.WARNING)
    @Suppress("DEPRECATION")
    public fun maxTime(maxTime: Long, timeUnit: TimeUnit = TimeUnit.MILLISECONDS): DistinctFlow<T> = apply {
        wrapped.maxTime(maxTime, timeUnit)
    }

    /**
     * Sets the collation options
     *
     * A null value represents the server default.
     *
     * @param collation the collation options to use
     * @return this
     */
    public fun collation(collation: Collation?): DistinctFlow<T> = apply { wrapped.collation(collation) }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     */
    public fun comment(comment: String?): DistinctFlow<T> = apply { wrapped.comment(comment) }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     */
    public fun comment(comment: BsonValue?): DistinctFlow<T> = apply { wrapped.comment(comment) }

    public override suspend fun collect(collector: FlowCollector<T>): Unit = wrapped.asFlow().collect(collector)
}
