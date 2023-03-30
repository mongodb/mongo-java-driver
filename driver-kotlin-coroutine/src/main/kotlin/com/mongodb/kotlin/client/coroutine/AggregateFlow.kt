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
import com.mongodb.reactivestreams.client.AggregatePublisher
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import org.bson.BsonValue
import org.bson.Document
import org.bson.conversions.Bson

/**
 * Flow implementation for aggregate operations.
 *
 * @param T The type of the result.
 * @see [Aggregation command](https://www.mongodb.com/docs/manual/reference/command/aggregate)
 */
public class AggregateFlow<T : Any>(private val wrapped: AggregatePublisher<T>) : Flow<T> {

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @see [Batch Size](https://www.mongodb.com/docs/manual/reference/method/cursor.batchSize/#cursor.batchSize)
     */
    public fun batchSize(batchSize: Int): AggregateFlow<T> = apply { wrapped.batchSize(batchSize) }

    /**
     * Aggregates documents according to the specified aggregation pipeline, which must end with a $out or $merge stage.
     *
     * @throws IllegalStateException if the pipeline does not end with a $out or $merge stage
     * @see [$out stage](https://www.mongodb.com/docs/manual/reference/operator/aggregation/out/)
     * @see [$merge stage](https://www.mongodb.com/docs/manual/reference/operator/aggregation/merge/)
     */
    public suspend fun toCollection() {
        wrapped.toCollection().awaitFirstOrNull()
    }

    /**
     * Enables writing to temporary files. A null value indicates that it's unspecified.
     *
     * @param allowDiskUse true if writing to temporary files is enabled
     * @return this
     * @see [Aggregation command](https://www.mongodb.com/docs/manual/reference/command/aggregate/)
     */
    public fun allowDiskUse(allowDiskUse: Boolean?): AggregateFlow<T> = apply { wrapped.allowDiskUse(allowDiskUse) }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime the max time
     * @param timeUnit the time unit, defaults to Milliseconds
     * @return this
     * @see [Max Time](https://www.mongodb.com/docs/manual/reference/method/cursor.maxTimeMS/#cursor.maxTimeMS)
     */
    public fun maxTime(maxTime: Long, timeUnit: TimeUnit = TimeUnit.MILLISECONDS): AggregateFlow<T> = apply {
        wrapped.maxTime(maxTime, timeUnit)
    }

    /**
     * The maximum amount of time for the server to wait on new documents to satisfy a `$changeStream` aggregation.
     *
     * A zero value will be ignored.
     *
     * @param maxAwaitTime the max await time
     * @param timeUnit the time unit to return the result in, defaults to Milliseconds
     * @return the maximum await execution time in the given time unit
     */
    public fun maxAwaitTime(maxAwaitTime: Long, timeUnit: TimeUnit = TimeUnit.MILLISECONDS): AggregateFlow<T> = apply {
        wrapped.maxAwaitTime(maxAwaitTime, timeUnit)
    }

    /**
     * Sets the bypass document level validation flag.
     *
     * Note: This only applies when an $out or $merge stage is specified.
     *
     * @param bypassDocumentValidation If true, allows the write to opt-out of document level validation.
     * @return this
     * @see [Aggregation command](https://www.mongodb.com/docs/manual/reference/command/aggregate/)
     */
    public fun bypassDocumentValidation(bypassDocumentValidation: Boolean?): AggregateFlow<T> = apply {
        wrapped.bypassDocumentValidation(bypassDocumentValidation)
    }

    /**
     * Sets the collation options
     *
     * A null value represents the server default.
     *
     * @param collation the collation options to use
     * @return this
     */
    public fun collation(collation: Collation?): AggregateFlow<T> = apply { wrapped.collation(collation) }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     */
    public fun comment(comment: String?): AggregateFlow<T> = apply { wrapped.comment(comment) }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * The comment can be any valid BSON type for server versions 4.4 and above. Server versions between 3.6 and 4.2
     * only support string as comment, and providing a non-string type will result in a server-side error.
     *
     * @param comment the comment
     * @return this
     */
    public fun comment(comment: BsonValue?): AggregateFlow<T> = apply { wrapped.comment(comment) }

    /**
     * Sets the hint for which index to use. A null value means no hint is set.
     *
     * @param hint the hint
     * @return this
     */
    public fun hint(hint: Bson?): AggregateFlow<T> = apply { wrapped.hint(hint) }

    /**
     * Sets the hint to apply.
     *
     * Note: If [AggregateFlow.hint] is set that will be used instead of any hint string.
     *
     * @param hint the name of the index which should be used for the operation
     * @return this
     */
    public fun hintString(hint: String?): AggregateFlow<T> = apply { wrapped.hintString(hint) }

    /**
     * Add top-level variables to the aggregation.
     *
     * For MongoDB 5.0+, the aggregate command accepts a `let` option. This option is a document consisting of zero or
     * more fields representing variables that are accessible to the aggregation pipeline. The key is the name of the
     * variable and the value is a constant in the aggregate expression language. Each parameter name is then usable to
     * access the value of the corresponding expression with the "$$" syntax within aggregate expression contexts which
     * may require the use of $expr or a pipeline.
     *
     * @param variables the variables
     * @return this
     */
    public fun let(variables: Bson?): AggregateFlow<T> = apply { wrapped.let(variables) }

    /**
     * Explain the execution plan for this operation with the given verbosity level
     *
     * @param R the type of the document class
     * @param verbosity the verbosity of the explanation
     * @return the execution plan
     * @see [Explain command](https://www.mongodb.com/docs/manual/reference/command/explain/)
     */
    @JvmName("explainDocument")
    public suspend fun explain(verbosity: ExplainVerbosity? = null): Document = explain<Document>(verbosity)

    /**
     * Explain the execution plan for this operation with the given verbosity level
     *
     * @param R the type of the document class
     * @param resultClass the result document type.
     * @param verbosity the verbosity of the explanation
     * @return the execution plan
     * @see [Explain command](https://www.mongodb.com/docs/manual/reference/command/explain/)
     */
    public suspend fun <R : Any> explain(resultClass: Class<R>, verbosity: ExplainVerbosity? = null): R =
        if (verbosity == null) wrapped.explain(resultClass).awaitSingle()
        else wrapped.explain(resultClass, verbosity).awaitSingle()

    /**
     * Explain the execution plan for this operation with the given verbosity level
     *
     * @param R the type of the document class
     * @param verbosity the verbosity of the explanation
     * @return the execution plan
     * @see [Explain command](https://www.mongodb.com/docs/manual/reference/command/explain/)
     */
    public suspend inline fun <reified R : Any> explain(verbosity: ExplainVerbosity? = null): R =
        explain(R::class.java, verbosity)

    public override suspend fun collect(collector: FlowCollector<T>): Unit = wrapped.asFlow().collect(collector)
}
