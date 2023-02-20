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

import com.mongodb.client.ListDatabasesIterable as JListDatabasesIterable
import java.util.concurrent.TimeUnit
import org.bson.BsonValue
import org.bson.conversions.Bson

/**
 * Iterable like implementation for list database operations.
 *
 * @param T The type of the result.
 * @see [List databases](https://www.mongodb.com/docs/manual/reference/command/listDatabases/)
 */
public class ListDatabasesIterable<T : Any>(@PublishedApi internal val wrapped: JListDatabasesIterable<T>) :
    MongoIterable<T>(wrapped) {
    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime the max time
     * @param timeUnit the time unit, defaults to Milliseconds
     * @return this
     * @see [Max Time](https://www.mongodb.com/docs/manual/reference/operator/meta/maxTimeMS/)
     */
    public fun maxTime(maxTime: Long, timeUnit: TimeUnit = TimeUnit.MILLISECONDS): ListDatabasesIterable<T> = apply {
        wrapped.maxTime(maxTime, timeUnit)
    }

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @see [Batch Size](https://www.mongodb.com/docs/manual/reference/method/cursor.batchSize/#cursor.batchSize)
     */
    public override fun batchSize(batchSize: Int): ListDatabasesIterable<T> = apply { wrapped.batchSize(batchSize) }

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
