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

import com.mongodb.client.ListIndexesIterable as JListIndexesIterable
import com.mongodb.client.cursor.TimeoutMode
import java.util.concurrent.TimeUnit
import org.bson.BsonValue

/**
 * Iterable like implementation for list index operations.
 *
 * @param T The type of the result.
 * @see [List indexes](https://www.mongodb.com/docs/manual/reference/command/listIndexes/)
 */
public class ListIndexesIterable<T : Any>(private val wrapped: JListIndexesIterable<T>) : MongoIterable<T>(wrapped) {

    public override fun batchSize(batchSize: Int): ListIndexesIterable<T> {
        super.batchSize(batchSize)
        return this
    }

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
    public fun timeoutMode(timeoutMode: TimeoutMode): ListIndexesIterable<T> {
        wrapped.timeoutMode(timeoutMode)
        return this
    }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime the max time
     * @param timeUnit the time unit, defaults to Milliseconds
     * @return this
     * @see [Max Time](https://www.mongodb.com/docs/manual/reference/operator/meta/maxTimeMS/)
     */
    public fun maxTime(maxTime: Long, timeUnit: TimeUnit = TimeUnit.MILLISECONDS): ListIndexesIterable<T> = apply {
        wrapped.maxTime(maxTime, timeUnit)
    }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     */
    public fun comment(comment: String?): ListIndexesIterable<T> = apply { wrapped.comment(comment) }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     */
    public fun comment(comment: BsonValue?): ListIndexesIterable<T> = apply { wrapped.comment(comment) }
}
