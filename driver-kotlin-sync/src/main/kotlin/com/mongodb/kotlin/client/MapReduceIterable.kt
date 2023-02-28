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

package com.mongodb.kotlin.client

import com.mongodb.client.MapReduceIterable as JMapReduceIterable
import com.mongodb.client.model.Collation
import com.mongodb.client.model.MapReduceAction
import java.util.concurrent.TimeUnit
import org.bson.conversions.Bson

/**
 * Iterable like implementation for map reduce operations.
 *
 * Note: Starting in MongoDB 5.0, map-reduce is deprecated, prefer Aggregation instead
 *
 * @param T The type of the result.
 * @see [Map Reduce](https://www.mongodb.com/docs/manual/reference/command/mapReduce/)
 */
@Deprecated("Map Reduce has been deprecated. Use Aggregation instead", replaceWith = ReplaceWith(""))
public class MapReduceIterable<T : Any>(private val wrapped: JMapReduceIterable<T>) : MongoIterable<T>(wrapped) {
    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @see [Batch Size](https://www.mongodb.com/docs/manual/reference/method/cursor.batchSize/#cursor.batchSize)
     */
    public override fun batchSize(batchSize: Int): MapReduceIterable<T> = apply { wrapped.batchSize(batchSize) }

    /**
     * Aggregates documents to a collection according to the specified map-reduce function with the given options, which
     * must specify a non-inline result.
     *
     * @throws IllegalStateException if a collection name to write the results to has not been specified
     */
    public fun toCollection(): Unit = wrapped.toCollection()

    /**
     * Sets the collectionName for the output of the MapReduce
     *
     * The default action is replace the collection if it exists, to change this use [.action].
     *
     * @param collectionName the name of the collection that you want the map-reduce operation to write its output.
     * @return this
     */
    public fun collectionName(collectionName: String): MapReduceIterable<T> = apply {
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
    public fun finalizeFunction(finalizeFunction: String?): MapReduceIterable<T> = apply {
        wrapped.finalizeFunction(finalizeFunction)
    }

    /**
     * Sets the global variables that are accessible in the map, reduce and finalize functions.
     *
     * @param scope the global variables that are accessible in the map, reduce and finalize functions.
     * @return this
     * @see [mapReduce command](https://www.mongodb.com/docs/manual/reference/command/mapReduce)
     */
    public fun scope(scope: Bson?): MapReduceIterable<T> = apply { wrapped.scope(scope) }

    /**
     * Sets the sort criteria to apply to the query.
     *
     * @param sort the sort criteria
     * @return this
     * @see [Sort results](https://www.mongodb.com/docs/manual/reference/method/cursor.sort/)
     */
    public fun sort(sort: Bson?): MapReduceIterable<T> = apply { wrapped.sort(sort) }

    /**
     * Sets the query filter to apply to the query.
     *
     * @param filter the filter to apply to the query.
     * @return this
     * @see [Filter results](https://www.mongodb.com/docs/manual/reference/method/db.collection.find/)
     */
    public fun filter(filter: Bson?): MapReduceIterable<T> = apply { wrapped.filter(filter) }

    /**
     * Sets the limit to apply.
     *
     * @param limit the limit
     * @return this
     * @see [Limit results](https://www.mongodb.com/docs/manual/reference/method/cursor.limit/#cursor.limit)
     */
    public fun limit(limit: Int): MapReduceIterable<T> = apply { wrapped.limit(limit) }

    /**
     * Sets the flag that specifies whether to convert intermediate data into BSON format between the execution of the
     * map and reduce functions. Defaults to false.
     *
     * @param jsMode the flag that specifies whether to convert intermediate data into BSON format between the execution
     *   of the map and reduce functions
     * @return jsMode
     * @see [mapReduce command](https://www.mongodb.com/docs/manual/reference/command/mapReduce)
     */
    public fun jsMode(jsMode: Boolean): MapReduceIterable<T> = apply { wrapped.jsMode(jsMode) }

    /**
     * Sets whether to include the timing information in the result information.
     *
     * @param verbose whether to include the timing information in the result information.
     * @return this
     */
    public fun verbose(verbose: Boolean): MapReduceIterable<T> = apply { wrapped.verbose(verbose) }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime the max time
     * @param timeUnit the time unit, defaults to Milliseconds
     * @return this
     * @see [Max Time](https://www.mongodb.com/docs/manual/reference/method/cursor.maxTimeMS/#cursor.maxTimeMS)
     */
    public fun maxTime(maxTime: Long, timeUnit: TimeUnit = TimeUnit.MILLISECONDS): MapReduceIterable<T> = apply {
        wrapped.maxTime(maxTime, timeUnit)
    }

    /**
     * Specify the `MapReduceAction` to be used when writing to a collection.
     *
     * @param action an [com.mongodb.client.model.MapReduceAction] to perform on the collection
     * @return this
     */
    public fun action(action: MapReduceAction): MapReduceIterable<T> = apply { wrapped.action(action) }

    /**
     * Sets the name of the database to output into.
     *
     * @param databaseName the name of the database to output into.
     * @return this
     * @see
     *   [output with an action](https://www.mongodb.com/docs/manual/reference/command/mapReduce/#output-to-a-collection-with-an-action)
     */
    public fun databaseName(databaseName: String?): MapReduceIterable<T> = apply { wrapped.databaseName(databaseName) }

    /**
     * Sets if the output database is sharded
     *
     * @param sharded if the output database is sharded
     * @return this
     * @see
     *   [output with an action](https://www.mongodb.com/docs/manual/reference/command/mapReduce/#output-to-a-collection-with-an-action)
     */
    public fun sharded(sharded: Boolean): MapReduceIterable<T> = apply { wrapped.sharded(sharded) }

    /**
     * Sets if the post-processing step will prevent MongoDB from locking the database.
     *
     * Valid only with the `MapReduceAction.MERGE` or `MapReduceAction.REDUCE` actions.
     *
     * @param nonAtomic if the post-processing step will prevent MongoDB from locking the database.
     * @return this
     * @see
     *   [output with an action](https://www.mongodb.com/docs/manual/reference/command/mapReduce/#output-to-a-collection-with-an-action)
     */
    public fun nonAtomic(nonAtomic: Boolean): MapReduceIterable<T> = apply { wrapped.nonAtomic(nonAtomic) }

    /**
     * Sets the bypass document level validation flag.
     *
     * Note: This only applies when an $out or $merge stage is specified.
     *
     * @param bypassDocumentValidation If true, allows the write to opt-out of document level validation.
     * @return this
     * @see [Aggregation command](https://www.mongodb.com/docs/manual/reference/command/aggregate/)
     */
    public fun bypassDocumentValidation(bypassDocumentValidation: Boolean?): MapReduceIterable<T> = apply {
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
    public fun collation(collation: Collation?): MapReduceIterable<T> = apply { wrapped.collation(collation) }
}
