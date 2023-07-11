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

import com.mongodb.ExplainVerbosity
import com.mongodb.client.ListSearchIndexesIterable as JListSearchIndexesIterable
import com.mongodb.client.model.Collation
import java.util.concurrent.TimeUnit
import org.bson.BsonValue
import org.bson.Document

/**
 * Iterable like implementation for list Atlas Search index operations.
 *
 * @param T The type of the result.
 * @see [List indexes](https://www.mongodb.com/docs/manual/reference/command/listIndexes/)
 */
public class ListSearchIndexesIterable<T : Any>(private val wrapped: JListSearchIndexesIterable<T>) :
    MongoIterable<T>(wrapped) {

    /**
     * Sets an Atlas Search index name for this operation. A null value means no index name is set.
     *
     * @param indexName Atlas Search index name.
     * @return this.
     */
    public fun name(indexName: String?): ListSearchIndexesIterable<T> = apply { wrapped.name(indexName) }

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size.
     * @return this.
     * @see [Batch Size](https://www.mongodb.com/docs/manual/reference/method/cursor.batchSize/#cursor.batchSize)
     */
    public override fun batchSize(batchSize: Int): ListSearchIndexesIterable<T> = apply { wrapped.batchSize(batchSize) }

    /**
     * Enables writing to temporary files. A null value indicates that it's unspecified.
     *
     * @param allowDiskUse true if writing to temporary files is enabled.
     * @return this.
     * @see [Aggregation command](https://www.mongodb.com/docs/manual/reference/command/aggregate/)
     */
    public fun allowDiskUse(allowDiskUse: Boolean?): ListSearchIndexesIterable<T> = apply {
        wrapped.allowDiskUse(allowDiskUse)
    }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime the max time.
     * @param timeUnit the time unit, defaults to Milliseconds.
     * @return this.
     * @see [Max Time](https://www.mongodb.com/docs/manual/reference/method/cursor.maxTimeMS/#cursor.maxTimeMS)
     */
    public fun maxTime(maxTime: Long, timeUnit: TimeUnit = TimeUnit.MILLISECONDS): ListSearchIndexesIterable<T> =
        apply {
            wrapped.maxTime(maxTime, timeUnit)
        }

    /**
     * The maximum amount of time for the server to wait on new documents to satisfy a `$changeStream` aggregation.
     *
     * A zero value will be ignored.
     *
     * @param maxAwaitTime the max await time.
     * @param timeUnit the time unit to return the result in, defaults to Milliseconds.
     * @return the maximum await execution time in the given time unit.
     */
    public fun maxAwaitTime(
        maxAwaitTime: Long,
        timeUnit: TimeUnit = TimeUnit.MILLISECONDS
    ): ListSearchIndexesIterable<T> = apply { wrapped.maxAwaitTime(maxAwaitTime, timeUnit) }

    /**
     * Sets the collation options.
     *
     * A null value represents the server default.
     *
     * @param collation the collation options to use.
     * @return this.
     */
    public fun collation(collation: Collation?): ListSearchIndexesIterable<T> = apply { wrapped.collation(collation) }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment.
     * @return this.
     */
    public fun comment(comment: String?): ListSearchIndexesIterable<T> = apply { wrapped.comment(comment) }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment.
     * @return this.
     */
    public fun comment(comment: BsonValue?): ListSearchIndexesIterable<T> = apply { wrapped.comment(comment) }

    /**
     * Explain the execution plan for this operation with the given verbosity level.
     *
     * @param verbosity the verbosity of the explanation.
     * @return the execution plan.
     * @see [Explain command](https://www.mongodb.com/docs/manual/reference/command/explain/)
     */
    public fun explain(verbosity: ExplainVerbosity? = null): Document = explain<Document>(verbosity)

    /**
     * Explain the execution plan for this operation with the given verbosity level.
     *
     * @param R the type of the document class.
     * @param resultClass the result document type.
     * @param verbosity the verbosity of the explanation.
     * @return the execution plan.
     * @see [Explain command](https://www.mongodb.com/docs/manual/reference/command/explain/)
     */
    public fun <R : Any> explain(resultClass: Class<R>, verbosity: ExplainVerbosity? = null): R =
        if (verbosity == null) wrapped.explain(resultClass) else wrapped.explain(resultClass, verbosity)

    /**
     * Explain the execution plan for this operation with the given verbosity level.
     *
     * @param R the type of the document class.
     * @param verbosity the verbosity of the explanation.
     * @return the execution plan.
     * @see [Explain command](https://www.mongodb.com/docs/manual/reference/command/explain/)
     */
    public inline fun <reified R : Any> explain(verbosity: ExplainVerbosity? = null): R =
        explain(R::class.java, verbosity)
}
