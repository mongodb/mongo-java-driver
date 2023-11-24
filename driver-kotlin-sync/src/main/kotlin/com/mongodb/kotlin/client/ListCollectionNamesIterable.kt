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

import com.mongodb.client.ListCollectionNamesIterable as JListCollectionNamesIterable
import java.util.concurrent.TimeUnit
import org.bson.BsonValue
import org.bson.conversions.Bson

/**
 * Iterable like implementation for list collection names operations.
 *
 * @see [List collections](https://www.mongodb.com/docs/manual/reference/command/listCollections/)
 */
public class ListCollectionNamesIterable(private val wrapped: JListCollectionNamesIterable) :
    MongoIterable<String>(wrapped) {
    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime the max time
     * @param timeUnit the time unit, defaults to Milliseconds
     * @return this
     * @see [Max Time](https://www.mongodb.com/docs/manual/reference/operator/meta/maxTimeMS/)
     */
    public fun maxTime(maxTime: Long, timeUnit: TimeUnit = TimeUnit.MILLISECONDS): ListCollectionNamesIterable = apply {
        wrapped.maxTime(maxTime, timeUnit)
    }

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @see [Batch Size](https://www.mongodb.com/docs/manual/reference/method/cursor.batchSize/#cursor.batchSize)
     */
    public override fun batchSize(batchSize: Int): ListCollectionNamesIterable = apply { wrapped.batchSize(batchSize) }

    /**
     * Sets the query filter to apply to the returned database names.
     *
     * @param filter the filter, which may be null.
     * @return this
     */
    public fun filter(filter: Bson?): ListCollectionNamesIterable = apply { wrapped.filter(filter) }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     */
    public fun comment(comment: String?): ListCollectionNamesIterable = apply { wrapped.comment(comment) }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     */
    public fun comment(comment: BsonValue?): ListCollectionNamesIterable = apply { wrapped.comment(comment) }

    /**
     * Sets the `authorizedCollections` field of the `listCollections` command.
     *
     * @param authorizedCollections If `true`, allows executing the `listCollections` command, which has the `nameOnly`
     *   field set to `true`, without having the
     *   [`listCollections` privilege](https://docs.mongodb.com/manual/reference/privilege-actions/#mongodb-authaction-listCollections)
     *   on the database resource.
     * @return `this`.
     * @since 5.0
     */
    public fun authorizedCollections(authorizedCollections: Boolean): ListCollectionNamesIterable = apply {
        wrapped.authorizedCollections(authorizedCollections)
    }
}
