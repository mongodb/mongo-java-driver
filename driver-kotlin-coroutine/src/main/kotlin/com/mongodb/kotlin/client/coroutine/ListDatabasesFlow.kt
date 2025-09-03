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

import com.mongodb.annotations.Alpha
import com.mongodb.annotations.Reason
import com.mongodb.client.cursor.TimeoutMode
import com.mongodb.reactivestreams.client.ListDatabasesPublisher
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.reactive.asFlow
import org.bson.BsonValue
import org.bson.conversions.Bson

/**
 * Flow implementation for list database operations.
 *
 * @param T The type of the result.
 * @see [List databases](https://www.mongodb.com/docs/manual/reference/command/listDatabases/)
 */
public class ListDatabasesFlow<T : Any>(private val wrapped: ListDatabasesPublisher<T>) : Flow<T> by wrapped.asFlow() {

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @see [Batch Size](https://www.mongodb.com/docs/manual/reference/method/cursor.batchSize/#cursor.batchSize)
     */
    public fun batchSize(batchSize: Int): ListDatabasesFlow<T> = apply { wrapped.batchSize(batchSize) }

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
    public fun timeoutMode(timeoutMode: TimeoutMode): ListDatabasesFlow<T> = apply { wrapped.timeoutMode(timeoutMode) }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime the max time
     * @param timeUnit the time unit, defaults to Milliseconds
     * @return this
     * @see [Max Time](https://www.mongodb.com/docs/manual/reference/operator/meta/maxTimeMS/)
     */
    public fun maxTime(maxTime: Long, timeUnit: TimeUnit = TimeUnit.MILLISECONDS): ListDatabasesFlow<T> = apply {
        wrapped.maxTime(maxTime, timeUnit)
    }

    /**
     * Sets the query filter to apply to the returned database names.
     *
     * @param filter the filter, which may be null.
     * @return this
     */
    public fun filter(filter: Bson?): ListDatabasesFlow<T> = apply { wrapped.filter(filter) }
    /**
     * Sets the nameOnly flag that indicates whether the command should return just the database names or return the
     * database names and size information.
     *
     * @param nameOnly the nameOnly flag, which may be null
     * @return this
     */
    public fun nameOnly(nameOnly: Boolean?): ListDatabasesFlow<T> = apply { wrapped.nameOnly(nameOnly) }

    /**
     * Sets the authorizedDatabasesOnly flag that indicates whether the command should return just the databases which
     * the user is authorized to see.
     *
     * @param authorizedDatabasesOnly the authorizedDatabasesOnly flag, which may be null
     * @return this
     */
    public fun authorizedDatabasesOnly(authorizedDatabasesOnly: Boolean?): ListDatabasesFlow<T> = apply {
        wrapped.authorizedDatabasesOnly(authorizedDatabasesOnly)
    }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     */
    public fun comment(comment: String?): ListDatabasesFlow<T> = apply { wrapped.comment(comment) }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     */
    public fun comment(comment: BsonValue?): ListDatabasesFlow<T> = apply { wrapped.comment(comment) }

    public override suspend fun collect(collector: FlowCollector<T>): Unit = wrapped.asFlow().collect(collector)
}
