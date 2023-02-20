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

import com.mongodb.CursorType
import com.mongodb.ExplainVerbosity
import com.mongodb.client.FindIterable as JFindIterable
import com.mongodb.client.model.Collation
import java.util.concurrent.TimeUnit
import org.bson.BsonValue
import org.bson.Document
import org.bson.conversions.Bson

/**
 * Iterable like implementation for find operations.
 *
 * @param <T> The type of the result.
 * @see [Collection filter](https://www.mongodb.com/docs/manual/reference/method/db.collection.find/)
 */
public class FindIterable<T>(@PublishedApi internal val wrapped: JFindIterable<T>) : MongoIterable<T>(wrapped) {
    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @see [Batch Size](https://www.mongodb.com/docs/manual/reference/method/cursor.batchSize/#cursor.batchSize)
     */
    public override fun batchSize(batchSize: Int): FindIterable<T> = apply { wrapped.batchSize(batchSize) }

    /**
     * Sets the query filter to apply to the query.
     *
     * @param filter the filter.
     * @return this
     * @see [Collection filter](https://www.mongodb.com/docs/manual/reference/method/db.collection.find/)
     */
    public fun filter(filter: Bson?): FindIterable<T> = apply { wrapped.filter(filter) }

    /**
     * Sets the limit to apply.
     *
     * @param limit the limit, which may be 0
     * @return this
     * @see [Cursor limit](https://www.mongodb.com/docs/manual/reference/method/cursor.limit/#cursor.limit)
     */
    public fun limit(limit: Int): FindIterable<T> = apply { wrapped.limit(limit) }

    /**
     * Sets the number of documents to skip.
     *
     * @param skip the number of documents to skip
     * @return this
     * @see [Cursor skip](https://www.mongodb.com/docs/manual/reference/method/cursor.skip/#cursor.skip)
     */
    public fun skip(skip: Int): FindIterable<T> = apply { wrapped.skip(skip) }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime the max time
     * @param timeUnit the time unit, which defaults to Milliseconds
     * @return this
     */
    public fun maxTime(maxTime: Long, timeUnit: TimeUnit = TimeUnit.MILLISECONDS): FindIterable<T> = apply {
        wrapped.maxTime(maxTime, timeUnit)
    }

    /**
     * The maximum amount of time for the server to wait on new documents to satisfy a tailable cursor query. This only
     * applies to a TAILABLE_AWAIT cursor. When the cursor is not a TAILABLE_AWAIT cursor, this option is ignored.
     *
     * On servers >= 3.2, this option will be specified on the getMore command as "maxTimeMS". The default is no value:
     * no "maxTimeMS" is sent to the server with the getMore command.
     *
     * On servers < 3.2, this option is ignored, and indicates that the driver should respect the server's default value
     *
     * A zero value will be ignored.
     *
     * @param maxAwaitTime the max await time
     * @param timeUnit the time unit to return results in, which defaults to Milliseconds
     * @return the maximum await execution time in the given time unit
     * @see [Max Time](https://www.mongodb.com/docs/manual/reference/method/cursor.maxTimeMS/#cursor.maxTimeMS)
     */
    public fun maxAwaitTime(maxAwaitTime: Long, timeUnit: TimeUnit = TimeUnit.MILLISECONDS): FindIterable<T> = apply {
        wrapped.maxAwaitTime(maxAwaitTime, timeUnit)
    }

    /**
     * Sets a document describing the fields to return for all matching documents.
     *
     * @param projection the project document.
     * @return this
     */
    public fun projection(projection: Bson?): FindIterable<T> = apply { wrapped.projection(projection) }

    /**
     * Sets the sort criteria to apply to the query.
     *
     * @param sort the sort criteria.
     * @return this
     * @see [Cursor sort](https://www.mongodb.com/docs/manual/reference/method/cursor.sort/)
     */
    public fun sort(sort: Bson?): FindIterable<T> = apply { wrapped.sort(sort) }

    /**
     * The server normally times out idle cursors after an inactivity period (10 minutes) to prevent excess memory use.
     * Set this option to prevent that.
     *
     * @param noCursorTimeout true if cursor timeout is disabled
     * @return this
     */
    public fun noCursorTimeout(noCursorTimeout: Boolean): FindIterable<T> = apply {
        wrapped.noCursorTimeout(noCursorTimeout)
    }

    /**
     * Users should not set this under normal circumstances.
     *
     * @param oplogReplay if oplog replay is enabled
     * @return this
     * @deprecated oplogReplay has been deprecated in MongoDB 4.4.
     */
    @Suppress("DEPRECATION")
    @Deprecated("oplogReplay has been deprecated in MongoDB 4.4", replaceWith = ReplaceWith(""))
    public fun oplogReplay(oplogReplay: Boolean): FindIterable<T> = apply { wrapped.oplogReplay(oplogReplay) }

    /**
     * Get partial results from a sharded cluster if one or more shards are unreachable (instead of throwing an error).
     *
     * @param partial if partial results for sharded clusters is enabled
     * @return this
     */
    public fun partial(partial: Boolean): FindIterable<T> = apply { wrapped.partial(partial) }

    /**
     * Sets the cursor type.
     *
     * @param cursorType the cursor type
     * @return this
     */
    public fun cursorType(cursorType: CursorType): FindIterable<T> = apply { wrapped.cursorType(cursorType) }

    /**
     * Sets the collation options
     *
     * A null value represents the server default.
     *
     * @param collation the collation options to use
     * @return this
     */
    public fun collation(collation: Collation?): FindIterable<T> = apply { wrapped.collation(collation) }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     */
    public fun comment(comment: String?): FindIterable<T> = apply { wrapped.comment(comment) }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * The comment can be any valid BSON type for server versions 4.4 and above. Server versions between 3.6 and 4.2
     * only support string as comment, and providing a non-string type will result in a server-side error.
     *
     * @param comment the comment
     * @return this
     */
    public fun comment(comment: BsonValue?): FindIterable<T> = apply { wrapped.comment(comment) }

    /**
     * Sets the hint for which index to use. A null value means no hint is set.
     *
     * @param hint the hint
     * @return this
     */
    public fun hint(hint: Bson?): FindIterable<T> = apply { wrapped.hint(hint) }

    /**
     * Sets the hint to apply.
     *
     * Note: If [FindIterable.hint] is set that will be used instead of any hint string.
     *
     * @param hint the name of the index which should be used for the operation
     * @return this
     */
    public fun hintString(hint: String?): FindIterable<T> = apply { wrapped.hintString(hint) }

    /**
     * Add top-level variables to the operation. A null value means no variables are set.
     *
     * Allows for improved command readability by separating the variables from the query text.
     *
     * @param variables for find operation
     * @return this
     */
    public fun let(variables: Bson?): FindIterable<T> = apply { wrapped.let(variables) }

    /**
     * Sets the exclusive upper bound for a specific index. A null value means no max is set.
     *
     * @param max the max
     * @return this
     */
    public fun max(max: Bson?): FindIterable<T> = apply { wrapped.max(max) }

    /**
     * Sets the minimum inclusive lower bound for a specific index. A null value means no max is set.
     *
     * @param min the min
     * @return this
     */
    public fun min(min: Bson?): FindIterable<T> = apply { wrapped.min(min) }

    /**
     * Sets the returnKey. If true the find operation will return only the index keys in the resulting documents.
     *
     * @param returnKey the returnKey
     * @return this
     */
    public fun returnKey(returnKey: Boolean): FindIterable<T> = apply { wrapped.returnKey(returnKey) }

    /**
     * Sets the showRecordId. Set to true to add a field `$recordId` to the returned documents.
     *
     * @param showRecordId the showRecordId
     * @return this
     */
    public fun showRecordId(showRecordId: Boolean): FindIterable<T> = apply { wrapped.showRecordId(showRecordId) }

    /**
     * Enables writing to temporary files on the server. When set to true, the server can write temporary data to disk
     * while executing the find operation.
     *
     * This option is sent only if the caller explicitly sets it to true.
     *
     * @param allowDiskUse the allowDiskUse
     * @return this
     */
    public fun allowDiskUse(allowDiskUse: Boolean?): FindIterable<T> = apply { wrapped.allowDiskUse(allowDiskUse) }

    /**
     * Explain the execution plan for this operation with the given verbosity level
     *
     * @param <R> the type of the document class
     * @param verbosity the verbosity of the explanation
     * @return the execution plan
     * @see [Explain command](https://www.mongodb.com/docs/manual/reference/command/explain/)
     */
    public fun explain(verbosity: ExplainVerbosity? = null): Document = explain<Document>(verbosity)

    /**
     * Explain the execution plan for this operation with the given verbosity level
     *
     * @param R the type of the document class
     * @param resultClass the result document type.
     * @param verbosity the verbosity of the explanation
     * @return the execution plan
     * @see [Explain command](https://www.mongodb.com/docs/manual/reference/command/explain/)
     */
    public fun <R : Any> explain(resultClass: Class<R>, verbosity: ExplainVerbosity? = null): R =
        if (verbosity == null) wrapped.explain(resultClass) else wrapped.explain(resultClass, verbosity)

    /**
     * Explain the execution plan for this operation with the given verbosity level
     *
     * @param R the type of the document class
     * @param verbosity the verbosity of the explanation
     * @return the execution plan
     * @see [Explain command](https://www.mongodb.com/docs/manual/reference/command/explain/)
     */
    public inline fun <reified R : Any> explain(verbosity: ExplainVerbosity? = null): R =
        explain(R::class.java, verbosity)
}
