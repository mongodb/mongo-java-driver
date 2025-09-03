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
import com.mongodb.reactivestreams.client.ListIndexesPublisher
import org.mongodb.scala.bson.BsonValue

import scala.concurrent.duration.Duration

/**
 * Observable interface for ListIndexes.
 *
 * @param wrapped the underlying java ListIndexesObservable
 * @tparam TResult The type of the result.
 * @since 1.0
 */
case class ListIndexesObservable[TResult](wrapped: ListIndexesPublisher[TResult]) extends Observable[TResult] {

  /**
   * Sets the maximum execution time on the server for this operation.
   *
   * [[https://www.mongodb.com/docs/manual/reference/operator/meta/maxTimeMS/ Max Time]]
   * @param duration the duration
   * @return this
   */
  def maxTime(duration: Duration): ListIndexesObservable[TResult] = {
    wrapped.maxTime(duration.toMillis, TimeUnit.MILLISECONDS)
    this
  }

  /**
   * Sets the number of documents to return per batch.
   *
   * @param batchSize the batch size
   * @return this
   * @since 2.7
   */
  def batchSize(batchSize: Int): ListIndexesObservable[TResult] = {
    wrapped.batchSize(batchSize)
    this
  }

  /**
   * Sets the comment for this operation. A null value means no comment is set.
   *
   * @param comment the comment
   * @return this
   * @since 4.6
   * @note Requires MongoDB 4.4 or greater
   */
  def comment(comment: String): ListIndexesObservable[TResult] = {
    wrapped.comment(comment)
    this
  }

  /**
   * Sets the comment for this operation. A null value means no comment is set.
   *
   * @param comment the comment
   * @return this
   * @since 4.6
   * @note Requires MongoDB 4.4 or greater
   */
  def comment(comment: BsonValue): ListIndexesObservable[TResult] = {
    wrapped.comment(comment)
    this
  }

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
  def timeoutMode(timeoutMode: TimeoutMode): ListIndexesObservable[TResult] = {
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

  override def subscribe(observer: Observer[_ >: TResult]): Unit = wrapped.subscribe(observer)
}
