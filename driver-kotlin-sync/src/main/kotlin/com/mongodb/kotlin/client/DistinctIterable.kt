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
package com.mongodb.kotlin.client

import com.mongodb.annotations.Alpha
import com.mongodb.annotations.Reason
import com.mongodb.client.DistinctIterable as JDistinctIterable
import com.mongodb.client.cursor.TimeoutMode
import com.mongodb.client.model.Collation
import com.mongodb.lang.Nullable
import java.util.concurrent.TimeUnit
import org.bson.BsonValue
import org.bson.conversions.Bson

/**
 * Iterable like implementation for distinct operations.
 *
 * @param T The type of the result.
 * @see [Distinct command](https://www.mongodb.com/docs/manual/reference/command/distinct/)
 */
public class DistinctIterable<T : Any?>(private val wrapped: JDistinctIterable<T>) : MongoIterable<T>(wrapped) {

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @see [Batch Size](https://www.mongodb.com/docs/manual/reference/method/cursor.batchSize/#cursor.batchSize)
     */
    public override fun batchSize(batchSize: Int): DistinctIterable<T> = apply { wrapped.batchSize(batchSize) }

    /**
     * Sets the timeoutMode for the cursor.
     *
     * Requires the `timeout` to be set, either in the [com.mongodb.MongoClientSettings], via [MongoDatabase] or via
     * [MongoCollection]
     *
     * @param timeoutMode the timeout mode
     * @return this
     * @since 5.2
     */
    @Alpha(Reason.CLIENT)
    public fun timeoutMode(timeoutMode: TimeoutMode): DistinctIterable<T> = apply { wrapped.timeoutMode(timeoutMode) }

    /**
     * Sets the query filter to apply to the query.
     *
     * @param filter the filter, which may be null.
     * @return this
     * @see [Filter results](https://www.mongodb.com/docs/manual/reference/method/db.collection.find/)
     */
    public fun filter(filter: Bson?): DistinctIterable<T> = apply { wrapped.filter(filter) }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime the max time
     * @param timeUnit the time unit, which defaults to Milliseconds
     * @return this
     */
    public fun maxTime(maxTime: Long, timeUnit: TimeUnit = TimeUnit.MILLISECONDS): DistinctIterable<T> = apply {
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
    public fun collation(collation: Collation?): DistinctIterable<T> = apply { wrapped.collation(collation) }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     */
    public fun comment(comment: String?): DistinctIterable<T> = apply { wrapped.comment(comment) }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     */
    public fun comment(comment: BsonValue?): DistinctIterable<T> = apply { wrapped.comment(comment) }

    /**
     * Sets the hint for which index to use. A null value means no hint is set.
     *
     * @param hint the hint
     * @return this
     */
    public fun hint(@Nullable hint: Bson?): DistinctIterable<T> = apply { wrapped.hint(hint) }

    /**
     * Sets the hint for which index to use. A null value means no hint is set.
     *
     * @param hint the name of the index which should be used for the operation
     * @return this
     */
    public fun hintString(@Nullable hint: String?): DistinctIterable<T> = apply { wrapped.hintString(hint) }
}
