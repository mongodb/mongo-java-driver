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
import com.mongodb.client.cursor.TimeoutMode
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

    public override fun batchSize(batchSize: Int): ListSearchIndexesIterable<T> {
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
    public fun timeoutMode(timeoutMode: TimeoutMode): ListSearchIndexesIterable<T> {
        wrapped.timeoutMode(timeoutMode)
        return this
    }

    /**
     * Sets an Atlas Search index name for this operation.
     *
     * @param indexName Atlas Search index name.
     * @return this.
     */
    public fun name(indexName: String): ListSearchIndexesIterable<T> = apply { wrapped.name(indexName) }

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
     * @param maxTime the max time.
     * @param timeUnit the time unit, defaults to Milliseconds.
     * @return this.
     */
    @Deprecated("Prefer using the operation execution timeout configuration option", level = DeprecationLevel.WARNING)
    @Suppress("DEPRECATION")
    public fun maxTime(maxTime: Long, timeUnit: TimeUnit = TimeUnit.MILLISECONDS): ListSearchIndexesIterable<T> =
        apply {
            wrapped.maxTime(maxTime, timeUnit)
        }

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
