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
@file:Suppress("DEPRECATION")

package com.mongodb.kotlin.client.coroutine

import com.mongodb.client.cursor.TimeoutMode
import com.mongodb.client.model.Collation
import com.mongodb.client.model.MapReduceAction
import com.mongodb.reactivestreams.client.MapReducePublisher
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.bson.conversions.Bson

/**
 * Flow implementation for map reduce operations.
 *
 * Note: Starting in MongoDB 5.0, map-reduce is deprecated, prefer Aggregation instead
 *
 * @param T The type of the result.
 * @see [Map Reduce](https://www.mongodb.com/docs/manual/reference/command/mapReduce/)
 */
@Deprecated("Map Reduce has been deprecated. Use Aggregation instead", replaceWith = ReplaceWith(""))
public class MapReduceFlow<T : Any>(private val wrapped: MapReducePublisher<T>) : Flow<T> by wrapped.asFlow() {

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @see [Batch Size](https://www.mongodb.com/docs/manual/reference/method/cursor.batchSize/#cursor.batchSize)
     */
    public fun batchSize(batchSize: Int): MapReduceFlow<T> = apply { wrapped.batchSize(batchSize) }

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
    public fun timeoutMode(timeoutMode: TimeoutMode): MapReduceFlow<T> = apply { wrapped.timeoutMode(timeoutMode) }

    /**
     * Aggregates documents to a collection according to the specified map-reduce function with the given options, which
     * must specify a non-inline result.
     *
     * @throws IllegalStateException if a collection name to write the results to has not been specified
     */
    public suspend fun toCollection() {
        wrapped.toCollection().awaitFirstOrNull()
    }

    /**
     * Sets the collectionName for the output of the MapReduce
     *
     * The default action is replace the collection if it exists, to change this use [.action].
     *
     * @param collectionName the name of the collection that you want the map-reduce operation to write its output.
     * @return this
     */
    public fun collectionName(collectionName: String): MapReduceFlow<T> = apply {
        wrapped.collectionName(collectionName)
    }

    /**
     * Sets the JavaScript function that follows the reduce method and modifies the output.
     *
     * @param finalizeFunction the JavaScript function that follows the reduce method and modifies the output.
     * @return this
     * @see
     *   [Requirements for the finalize Function](https://www.mongodb.com/docs/manual/reference/command/mapReduce/#mapreduce-finalize-cmd)
     */
    public fun finalizeFunction(finalizeFunction: String?): MapReduceFlow<T> = apply {
        wrapped.finalizeFunction(finalizeFunction)
    }

    /**
     * Sets the global variables that are accessible in the map, reduce and finalize functions.
     *
     * @param scope the global variables that are accessible in the map, reduce and finalize functions.
     * @return this
     * @see [mapReduce command](https://www.mongodb.com/docs/manual/reference/command/mapReduce)
     */
    public fun scope(scope: Bson?): MapReduceFlow<T> = apply { wrapped.scope(scope) }

    /**
     * Sets the sort criteria to apply to the query.
     *
     * @param sort the sort criteria
     * @return this
     * @see [Sort results](https://www.mongodb.com/docs/manual/reference/method/cursor.sort/)
     */
    public fun sort(sort: Bson?): MapReduceFlow<T> = apply { wrapped.sort(sort) }

    /**
     * Sets the query filter to apply to the query.
     *
     * @param filter the filter to apply to the query.
     * @return this
     * @see [Filter results](https://www.mongodb.com/docs/manual/reference/method/db.collection.find/)
     */
    public fun filter(filter: Bson?): MapReduceFlow<T> = apply { wrapped.filter(filter) }

    /**
     * Sets the limit to apply.
     *
     * @param limit the limit
     * @return this
     * @see [Cursor limit](https://www.mongodb.com/docs/manual/reference/method/cursor.limit/#cursor.limit)
     */
    public fun limit(limit: Int): MapReduceFlow<T> = apply { wrapped.limit(limit) }

    /**
     * Sets the flag that specifies whether to convert intermediate data into BSON format between the execution of the
     * map and reduce functions. Defaults to false.
     *
     * @param jsMode the flag that specifies whether to convert intermediate data into BSON format between the execution
     *   of the map and reduce functions
     * @return jsMode
     * @see [mapReduce command](https://www.mongodb.com/docs/manual/reference/command/mapReduce)
     */
    public fun jsMode(jsMode: Boolean): MapReduceFlow<T> = apply { wrapped.jsMode(jsMode) }

    /**
     * Sets whether to include the timing information in the result information.
     *
     * @param verbose whether to include the timing information in the result information.
     * @return this
     */
    public fun verbose(verbose: Boolean): MapReduceFlow<T> = apply { wrapped.verbose(verbose) }

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
    public fun maxTime(maxTime: Long, timeUnit: TimeUnit = TimeUnit.MILLISECONDS): MapReduceFlow<T> = apply {
        wrapped.maxTime(maxTime, timeUnit)
    }

    /**
     * Specify the `MapReduceAction` to be used when writing to a collection.
     *
     * @param action an [com.mongodb.client.model.MapReduceAction] to perform on the collection
     * @return this
     */
    public fun action(action: MapReduceAction): MapReduceFlow<T> = apply { wrapped.action(action) }

    /**
     * Sets the name of the database to output into.
     *
     * @param databaseName the name of the database to output into.
     * @return this
     * @see
     *   [output with an action](https://www.mongodb.com/docs/manual/reference/command/mapReduce/#output-to-a-collection-with-an-action)
     */
    public fun databaseName(databaseName: String?): MapReduceFlow<T> = apply { wrapped.databaseName(databaseName) }

    /**
     * Sets the bypass document level validation flag.
     *
     * Note: This only applies when an $out or $merge stage is specified.
     *
     * @param bypassDocumentValidation If true, allows the write to opt-out of document level validation.
     * @return this
     * @see [Aggregation command](https://www.mongodb.com/docs/manual/reference/command/aggregate/)
     */
    public fun bypassDocumentValidation(bypassDocumentValidation: Boolean?): MapReduceFlow<T> = apply {
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
    public fun collation(collation: Collation?): MapReduceFlow<T> = apply { wrapped.collation(collation) }

    public override suspend fun collect(collector: FlowCollector<T>): Unit = wrapped.asFlow().collect(collector)
}
