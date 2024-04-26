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
import com.mongodb.client.ListDatabasesIterable as JListDatabasesIterable
import com.mongodb.client.cursor.TimeoutMode
import java.util.concurrent.TimeUnit
import org.bson.BsonValue
import org.bson.conversions.Bson

/**
 * Iterable like implementation for list database operations.
 *
 * @param T The type of the result.
 * @see [List databases](https://www.mongodb.com/docs/manual/reference/command/listDatabases/)
 */
public class ListDatabasesIterable<T : Any>(private val wrapped: JListDatabasesIterable<T>) :
    MongoIterable<T>(wrapped) {

    public override fun batchSize(batchSize: Int): ListDatabasesIterable<T> {
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
     * @since CSOT
     */
    @Alpha(Reason.CLIENT)
    public fun timeoutMode(timeoutMode: TimeoutMode): ListDatabasesIterable<T> {
        wrapped.timeoutMode(timeoutMode)
        return this
    }

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
     * @param timeUnit the time unit, defaults to Milliseconds
     * @return this
     */
    @Deprecated("Prefer using the operation execution timeout configuration option", level = DeprecationLevel.WARNING)
    @Suppress("DEPRECATION")
    public fun maxTime(maxTime: Long, timeUnit: TimeUnit = TimeUnit.MILLISECONDS): ListDatabasesIterable<T> = apply {
        wrapped.maxTime(maxTime, timeUnit)
    }

    /**
     * Sets the query filter to apply to the returned database names.
     *
     * @param filter the filter, which may be null.
     * @return this
     */
    public fun filter(filter: Bson?): ListDatabasesIterable<T> = apply { wrapped.filter(filter) }
    /**
     * Sets the nameOnly flag that indicates whether the command should return just the database names or return the
     * database names and size information.
     *
     * @param nameOnly the nameOnly flag, which may be null
     * @return this
     */
    public fun nameOnly(nameOnly: Boolean?): ListDatabasesIterable<T> = apply { wrapped.nameOnly(nameOnly) }

    /**
     * Sets the authorizedDatabasesOnly flag that indicates whether the command should return just the databases which
     * the user is authorized to see.
     *
     * @param authorizedDatabasesOnly the authorizedDatabasesOnly flag, which may be null
     * @return this
     */
    public fun authorizedDatabasesOnly(authorizedDatabasesOnly: Boolean?): ListDatabasesIterable<T> = apply {
        wrapped.authorizedDatabasesOnly(authorizedDatabasesOnly)
    }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     */
    public fun comment(comment: String?): ListDatabasesIterable<T> = apply { wrapped.comment(comment) }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     */
    public fun comment(comment: BsonValue?): ListDatabasesIterable<T> = apply { wrapped.comment(comment) }
}
