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

import com.mongodb.ExplainVerbosity
import com.mongodb.client.model.Collation
import com.mongodb.reactivestreams.client.ListSearchIndexesPublisher
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitSingle
import org.bson.BsonValue
import org.bson.Document

/**
 * Flow implementation for list Atlas Search index operations.
 *
 * @param T The type of the result.
 * @see [List Atlas Search indexes]
 *   (https://www.mongodb.com/docs/manual/reference/operator/aggregation/listSearchIndexes)
 */
public class ListSearchIndexesFlow<T : Any>(private val wrapped: ListSearchIndexesPublisher<T>) :
    Flow<T> by wrapped.asFlow() {

    /**
     * Sets an Atlas Search index name for this operation. A null value means no index name is set.
     *
     * @param indexName Atlas Search index name.
     * @return this.
     */
    public fun name(indexName: String?): ListSearchIndexesFlow<T> = apply { wrapped.name(indexName) }

    /**
     * Enables writing to temporary files. A null value indicates that it's unspecified.
     *
     * @param allowDiskUse true if writing to temporary files is enabled.
     * @return this.
     * @see [Aggregation command](https://www.mongodb.com/docs/manual/reference/command/aggregate/)
     */
    public fun allowDiskUse(allowDiskUse: Boolean?): ListSearchIndexesFlow<T> = apply {
        wrapped.allowDiskUse(allowDiskUse)
    }

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size.
     * @return this.
     * @see [Batch Size](https://www.mongodb.com/docs/manual/reference/method/cursor.batchSize/#cursor.batchSize)
     */
    public fun batchSize(batchSize: Int): ListSearchIndexesFlow<T> = apply { wrapped.batchSize(batchSize) }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime the max time.
     * @param timeUnit the time unit, defaults to Milliseconds.
     * @return this.
     */
    public fun maxTime(maxTime: Long, timeUnit: TimeUnit = TimeUnit.MILLISECONDS): ListSearchIndexesFlow<T> = apply {
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
    public fun collation(collation: Collation?): ListSearchIndexesFlow<T> = apply { wrapped.collation(collation) }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment.
     * @return this.
     */
    public fun comment(comment: String?): ListSearchIndexesFlow<T> = apply { wrapped.comment(comment) }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment.
     * @return this.
     */
    public fun comment(comment: BsonValue?): ListSearchIndexesFlow<T> = apply { wrapped.comment(comment) }

    /**
     * Explain the execution plan for this operation with the given verbosity level.
     *
     * @param verbosity the verbosity of the explanation.
     * @return the execution plan.
     * @see [Explain command](https://www.mongodb.com/docs/manual/reference/command/explain/)
     */
    @JvmName("explainDocument")
    public suspend fun explain(verbosity: ExplainVerbosity? = null): Document = explain<Document>(verbosity)

    /**
     * Explain the execution plan for this operation with the given verbosity level.
     *
     * @param R the type of the document class.
     * @param resultClass the result document type.
     * @param verbosity the verbosity of the explanation.
     * @return the execution plan.
     * @see [Explain command](https://www.mongodb.com/docs/manual/reference/command/explain/)
     */
    public suspend fun <R : Any> explain(resultClass: Class<R>, verbosity: ExplainVerbosity? = null): R =
        if (verbosity == null) wrapped.explain(resultClass).awaitSingle()
        else wrapped.explain(resultClass, verbosity).awaitSingle()

    /**
     * Explain the execution plan for this operation with the given verbosity level.
     *
     * @param R the type of the document class.
     * @param verbosity the verbosity of the explanation.
     * @return the execution plan.
     * @see [Explain command](https://www.mongodb.com/docs/manual/reference/command/explain/)
     */
    public suspend inline fun <reified R : Any> explain(verbosity: ExplainVerbosity? = null): R =
        explain(R::class.java, verbosity)

    public override suspend fun collect(collector: FlowCollector<T>): Unit = wrapped.asFlow().collect(collector)
}
