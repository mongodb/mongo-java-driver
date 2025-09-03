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

package org.mongodb.scala

import com.mongodb.annotations.{ Alpha, Reason }

import java.util.concurrent.TimeUnit
import com.mongodb.client.model.MapReduceAction
import com.mongodb.reactivestreams.client.MapReducePublisher
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.Collation
import org.reactivestreams.Subscriber

import scala.concurrent.duration.Duration

/**
 * Observable for map reduce.
 *
 * By default, the [[MapReduceObservable]] produces the results inline. You can write map-reduce output to a collection by using the
 * [[collectionName]] and [[toCollection]] methods.
 *
 * @define docsRef https://www.mongodb.com/docs/manual/reference
 *
 * @tparam TResult The type of the result.
 * @since 1.0
 */
@deprecated("Superseded by aggregate", "4.4.0")
case class MapReduceObservable[TResult](wrapped: MapReducePublisher[TResult]) extends Observable[TResult] {

  /**
   * Sets the collectionName for the output of the MapReduce
   *
   * <p>The default action is replace the collection if it exists, to change this use [[action]].</p>
   *
   * @param collectionName the name of the collection that you want the map-reduce operation to write its output.
   * @return this
   * @see [[toCollection]]
   */
  def collectionName(collectionName: String): MapReduceObservable[TResult] = {
    wrapped.collectionName(collectionName)
    this
  }

  /**
   * Sets the JavaScript function that follows the reduce method and modifies the output.
   *
   * [[https://www.mongodb.com/docs/manual/reference/command/mapReduce#mapreduce-finalize-cmd Requirements for the finalize Function]]
   * @param finalizeFunction the JavaScript function that follows the reduce method and modifies the output.
   * @return this
   */
  def finalizeFunction(finalizeFunction: String): MapReduceObservable[TResult] = {
    wrapped.finalizeFunction(finalizeFunction)
    this
  }

  /**
   * Sets the global variables that are accessible in the map, reduce and finalize functions.
   *
   * [[https://www.mongodb.com/docs/manual/reference/command/mapReduce mapReduce]]
   * @param scope the global variables that are accessible in the map, reduce and finalize functions.
   * @return this
   */
  def scope(scope: Bson): MapReduceObservable[TResult] = {
    wrapped.scope(scope)
    this
  }

  /**
   * Sets the sort criteria to apply to the query.
   *
   * [[https://www.mongodb.com/docs/manual/reference/method/cursor.sort/ Sort]]
   * @param sort the sort criteria, which may be null.
   * @return this
   */
  def sort(sort: Bson): MapReduceObservable[TResult] = {
    wrapped.sort(sort)
    this
  }

  /**
   * Sets the query filter to apply to the query.
   *
   * [[https://www.mongodb.com/docs/manual/reference/method/db.collection.find/ Filter]]
   * @param filter the filter to apply to the query.
   * @return this
   */
  def filter(filter: Bson): MapReduceObservable[TResult] = {
    wrapped.filter(filter)
    this
  }

  /**
   * Sets the limit to apply.
   *
   * [[https://www.mongodb.com/docs/manual/reference/method/cursor.limit/#cursor.limit Limit]]
   * @param limit the limit, which may be null
   * @return this
   */
  def limit(limit: Int): MapReduceObservable[TResult] = {
    wrapped.limit(limit)
    this
  }

  /**
   * Sets the flag that specifies whether to convert intermediate data into BSON format between the execution of the map and reduce
   * functions. Defaults to false.
   *
   * [[https://www.mongodb.com/docs/manual/reference/command/mapReduce mapReduce]]
   * @param jsMode the flag that specifies whether to convert intermediate data into BSON format between the execution of the map and
   *               reduce functions
   * @return jsMode
   */
  def jsMode(jsMode: Boolean): MapReduceObservable[TResult] = {
    wrapped.jsMode(jsMode)
    this
  }

  /**
   * Sets whether to include the timing information in the result information.
   *
   * @param verbose whether to include the timing information in the result information.
   * @return this
   */
  def verbose(verbose: Boolean): MapReduceObservable[TResult] = {
    wrapped.verbose(verbose)
    this
  }

  /**
   * Sets the maximum execution time on the server for this operation.
   *
   * [[https://www.mongodb.com/docs/manual/reference/operator/meta/maxTimeMS/ Max Time]]
   * @param duration the duration
   * @return this
   */
  def maxTime(duration: Duration): MapReduceObservable[TResult] = {
    wrapped.maxTime(duration.toMillis, TimeUnit.MILLISECONDS)
    this
  }

  /**
   * Specify the `MapReduceAction` to be used when writing to a collection.
   *
   * @param action an [[model.MapReduceAction]] to perform on the collection
   * @return this
   */
  def action(action: MapReduceAction): MapReduceObservable[TResult] = {
    wrapped.action(action)
    this
  }

  /**
   * Sets the name of the database to output into.
   *
   * [[https://www.mongodb.com/docs/manual/reference/command/mapReduce#output-to-a-collection-with-an-action output with an action]]
   * @param databaseName the name of the database to output into.
   * @return this
   */
  def databaseName(databaseName: String): MapReduceObservable[TResult] = {
    wrapped.databaseName(databaseName)
    this
  }

  /**
   * Sets the bypass document level validation flag.
   *
   * '''Note:''': This only applies when an `\$out` stage is specified.
   *
   * [[https://www.mongodb.com/docs/manual/reference/command/mapReduce#output-to-a-collection-with-an-action output with an action]]
   *
   * @note Requires MongoDB 3.2 or greater
   * @param bypassDocumentValidation If true, allows the write to opt-out of document level validation.
   * @return this
   * @since 1.1
   */
  def bypassDocumentValidation(bypassDocumentValidation: Boolean): MapReduceObservable[TResult] = {
    wrapped.bypassDocumentValidation(bypassDocumentValidation)
    this
  }

  /**
   * Sets the collation options
   *
   * @param collation the collation options to use
   * @return this
   * @since 1.2
   * @note A null value represents the server default.
   * @note Requires MongoDB 3.4 or greater
   */
  def collation(collation: Collation): MapReduceObservable[TResult] = {
    wrapped.collation(collation)
    this
  }

  /**
   * Sets the number of documents to return per batch.
   *
   * @param batchSize the batch size
   * @return this
   * @since 2.7
   */
  def batchSize(batchSize: Int): MapReduceObservable[TResult] = {
    wrapped.batchSize(batchSize)
    this
  }

  /**
   * Aggregates documents to a collection according to the specified map-reduce function with the given options, which must not produce
   * results inline. Calling this method and then subscribing to the returned [[SingleObservable]] is the preferred alternative to
   * subscribing to this [[MapReduceObservable]],
   * because this method does what is explicitly requested without executing implicit operations.
   *
   * @return an Observable that indicates when the operation has completed
   * [[https://www.mongodb.com/docs/manual/aggregation/ Aggregation]]
   * @throws java.lang.IllegalStateException if a collection name to write the results to has not been specified
   * @see [[collectionName]]
   */
  def toCollection(): SingleObservable[Unit] = wrapped.toCollection()

  /**
   * Sets the timeoutMode for the cursor.
   *
   * Requires the `timeout` to be set, either in the [[MongoClientSettings]],
   * via [[MongoDatabase]] or via [[MongoCollection]]
   *
   * @param timeoutMode the timeout mode
   * @return this
   * @since 5.2
   */
  @Alpha(Array(Reason.CLIENT))
  def timeoutMode(timeoutMode: TimeoutMode): MapReduceObservable[TResult] = {
    wrapped.timeoutMode(timeoutMode)
    this
  }

  /**
   * Helper to return a single observable limited to the first result.
   *
   * @return a single observable which will the first result.
   * @since 4.0
   */
  def first(): SingleObservable[TResult] = wrapped.first()

  /**
   * Requests [[MapReduceObservable]] to start streaming data according to the specified map-reduce function with the given options.
   *
   *  - If the aggregation produces results inline, then finds all documents in the
   *    affected namespace and produces them. You may want to use [[toCollection]] instead.
   *  - Otherwise, produces no elements.
   */
  override def subscribe(observer: Observer[_ >: TResult]): Unit = wrapped.subscribe(observer)

  /**
   * Requests [[MapReduceObservable]] to start streaming data according to the specified map-reduce function with the given options.
   *
   *  - If the aggregation produces results inline, then finds all documents in the
   *    affected namespace and produces them. You may want to use [[toCollection]] instead.
   *  - Otherwise, produces no elements.
   */
  override def subscribe(observer: Subscriber[_ >: TResult]): Unit = wrapped.subscribe(observer)
}
