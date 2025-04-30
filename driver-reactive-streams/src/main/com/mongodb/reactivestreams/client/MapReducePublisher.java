/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client;

import com.mongodb.annotations.Alpha;
import com.mongodb.annotations.Reason;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.Collation;
import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.concurrent.TimeUnit;

/**
 * Publisher for map reduce.
 * <p>
 * By default, the {@code MapReducePublisher} produces the results inline. You can write map-reduce output to a collection by using the
 * {@link #collectionName(String)} and {@link #toCollection()} methods.</p>
 *
 * @param <TResult> The type of the result.
 * @since 1.0
 * @deprecated Superseded by aggregate
 */
@Deprecated
public interface MapReducePublisher<TResult> extends Publisher<TResult> {

    /**
     * Sets the collectionName for the output of the MapReduce
     *
     * <p>The default action is replace the collection if it exists, to change this use {@link #action}.</p>
     *
     * @param collectionName the name of the collection that you want the map-reduce operation to write its output.
     * @return this
     * @see #toCollection()
     */
    MapReducePublisher<TResult> collectionName(String collectionName);

    /**
     * Sets the JavaScript function that follows the reduce method and modifies the output.
     *
     * @param finalizeFunction the JavaScript function that follows the reduce method and modifies the output.
     * @return this
     * @mongodb.driver.manual reference/command/mapReduce/#mapreduce-finalize-cmd Requirements for the finalize Function
     */
    MapReducePublisher<TResult> finalizeFunction(@Nullable String finalizeFunction);

    /**
     * Sets the global variables that are accessible in the map, reduce and finalize functions.
     *
     * @param scope the global variables that are accessible in the map, reduce and finalize functions.
     * @return this
     * @mongodb.driver.manual reference/command/mapReduce mapReduce
     */
    MapReducePublisher<TResult> scope(@Nullable Bson scope);

    /**
     * Sets the sort criteria to apply to the query.
     *
     * @param sort the sort criteria, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/cursor.sort/ Sort
     */
    MapReducePublisher<TResult> sort(@Nullable Bson sort);

    /**
     * Sets the query filter to apply to the query.
     *
     * @param filter the filter to apply to the query.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    MapReducePublisher<TResult> filter(@Nullable Bson filter);

    /**
     * Sets the limit to apply.
     *
     * @param limit the limit, which may be null
     * @return this
     * @mongodb.driver.manual reference/method/cursor.limit/#cursor.limit Limit
     */
    MapReducePublisher<TResult> limit(int limit);

    /**
     * Sets the flag that specifies whether to convert intermediate data into BSON format between the execution of the map and reduce
     * functions. Defaults to false.
     *
     * @param jsMode the flag that specifies whether to convert intermediate data into BSON format between the execution of the map and
     *               reduce functions
     * @return jsMode
     * @mongodb.driver.manual reference/command/mapReduce mapReduce
     */
    MapReducePublisher<TResult> jsMode(boolean jsMode);

    /**
     * Sets whether to include the timing information in the result information.
     *
     * @param verbose whether to include the timing information in the result information.
     * @return this
     */
    MapReducePublisher<TResult> verbose(boolean verbose);

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    MapReducePublisher<TResult> maxTime(long maxTime, TimeUnit timeUnit);

    /**
     * Specify the {@code MapReduceAction} to be used when writing to a collection.
     *
     * @param action an {@link com.mongodb.client.model.MapReduceAction} to perform on the collection
     * @return this
     */
    MapReducePublisher<TResult> action(com.mongodb.client.model.MapReduceAction action);

    /**
     * Sets the name of the database to output into.
     *
     * @param databaseName the name of the database to output into.
     * @return this
     * @mongodb.driver.manual reference/command/mapReduce/#output-to-a-collection-with-an-action output with an action
     */
    MapReducePublisher<TResult> databaseName(@Nullable String databaseName);

    /**
     * Sets the bypass document level validation flag.
     *
     * <p>Note: This only applies when an $out stage is specified</p>.
     *
     * @param bypassDocumentValidation If true, allows the write to opt-out of document level validation.
     * @return this
     * @since 1.2
     * @mongodb.driver.manual reference/command/aggregate/ Aggregation
     * @mongodb.server.release 3.2
     */
    MapReducePublisher<TResult> bypassDocumentValidation(@Nullable Boolean bypassDocumentValidation);

    /**
     * Aggregates documents to a collection according to the specified map-reduce function with the given options, which must not produce
     * results inline. Calling this method and then {@linkplain Publisher#subscribe(Subscriber) subscribing} to the returned
     * {@link Publisher} is the preferred alternative to {@linkplain #subscribe(Subscriber) subscribing} to this {@link MapReducePublisher},
     * because this method does what is explicitly requested without executing implicit operations.
     *
     * @return an empty publisher that indicates when the operation has completed
     * @throws IllegalStateException if a {@linkplain #collectionName(String) collection name} to write the results to has not been specified
     * @see #collectionName(String)
     * @mongodb.driver.manual aggregation/ Aggregation
     */
    Publisher<Void> toCollection();

    /**
     * Requests {@link MapReducePublisher} to start streaming data according to the specified map-reduce function with the given options.
     * <ul>
     *     <li>
     *     If the aggregation produces results inline, then {@linkplain MongoCollection#find() finds all} documents in the
     *     affected namespace and produces them. You may want to use {@link #toCollection()} instead.</li>
     *     <li>
     *     Otherwise, produces no elements.</li>
     * </ul>
     */
    void subscribe(Subscriber<? super TResult> s);

    /**
     * Sets the collation options
     *
     * <p>A null value represents the server default.</p>
     * @param collation the collation options to use
     * @return this
     * @since 1.3
     * @mongodb.server.release 3.4
     */
    MapReducePublisher<TResult> collation(@Nullable Collation collation);

    /**
     * Sets the number of documents to return per batch.
     *
     * <p>Overrides the {@link org.reactivestreams.Subscription#request(long)} value for setting the batch size, allowing for fine-grained
     * control over the underlying cursor.</p>
     *
     * @param batchSize the batch size
     * @return this
     * @since 1.8
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    MapReducePublisher<TResult> batchSize(int batchSize);

    /**
     * Sets the timeoutMode for the cursor.
     *
     * <p>
     *     Requires the {@code timeout} to be set, either in the {@link com.mongodb.MongoClientSettings},
     *     via {@link MongoDatabase} or via {@link MongoCollection}
     * </p>
     * <p>
     *     If the {@code timeout} is set then:
     *     <ul>
     *      <li>For non-tailable cursors, the default value of timeoutMode is {@link TimeoutMode#CURSOR_LIFETIME}</li>
     *      <li>For tailable cursors, the default value of timeoutMode is {@link TimeoutMode#ITERATION} and its an error
     *      to configure it as: {@link TimeoutMode#CURSOR_LIFETIME}</li>
     *     </ul>
     * @param timeoutMode the timeout mode
     * @return this
     * @since 5.2
     */
    @Alpha(Reason.CLIENT)
    MapReducePublisher<TResult> timeoutMode(TimeoutMode timeoutMode);

    /**
     * Helper to return a publisher limited to the first result.
     *
     * @return a Publisher which will contain a single item.
     * @since 1.8
     */
    Publisher<TResult> first();
}
