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

import java.util.concurrent.TimeUnit

import com.mongodb.reactivestreams.client.AggregatePublisher
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.Collation

import scala.concurrent.duration.Duration

/**
 * Observable for aggregate
 *
 * @param wrapped the underlying java AggregateObservable
 * @tparam TResult The type of the result.
 * @since 1.0
 */
case class AggregateObservable[TResult](private val wrapped: AggregatePublisher[TResult]) extends Observable[TResult] {

  /**
   * Enables writing to temporary files. A null value indicates that it's unspecified.
   *
   * [[http://docs.mongodb.org/manual/reference/command/aggregate/ Aggregation]]
   *
   * @param allowDiskUse true if writing to temporary files is enabled
   * @return this
   */
  def allowDiskUse(allowDiskUse: Boolean): AggregateObservable[TResult] = {
    wrapped.allowDiskUse(allowDiskUse)
    this
  }

  /**
   * Sets the maximum execution time on the server for this operation.
   *
   * [[http://docs.mongodb.org/manual/reference/operator/meta/maxTimeMS/ Max Time]]
   * @param duration the duration
   * @return this
   */
  def maxTime(duration: Duration): AggregateObservable[TResult] = {
    wrapped.maxTime(duration.toMillis, TimeUnit.MILLISECONDS)
    this
  }

  /**
   * Sets the maximum await execution time on the server for this operation.
   *
   * [[http://docs.mongodb.org/manual/reference/operator/meta/maxTimeMS/ Max Time]]
   * @param duration the duration
   * @return this
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def maxAwaitTime(duration: Duration): AggregateObservable[TResult] = {
    wrapped.maxAwaitTime(duration.toMillis, TimeUnit.MILLISECONDS)
    this
  }

  /**
   * Sets the bypass document level validation flag.
   *
   * '''Note:''': This only applies when an `\$out` stage is specified.
   *
   * [[http://docs.mongodb.org/manual/reference/command/aggregate/ Aggregation]]
   * @note Requires MongoDB 3.2 or greater
   * @param bypassDocumentValidation If true, allows the write to opt-out of document level validation.
   * @return this
   * @since 1.1
   */
  def bypassDocumentValidation(bypassDocumentValidation: Boolean): AggregateObservable[TResult] = {
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
  def collation(collation: Collation): AggregateObservable[TResult] = {
    wrapped.collation(collation)
    this
  }

  /**
   * Sets the comment to the aggregation. A null value means no comment is set.
   *
   * @param comment the comment
   * @return this
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def comment(comment: String): AggregateObservable[TResult] = {
    wrapped.comment(comment)
    this
  }

  /**
   * Sets the hint for which index to use. A null value means no hint is set.
   *
   * @param hint the hint
   * @return this
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def hint(hint: Bson): AggregateObservable[TResult] = {
    wrapped.hint(hint)
    this
  }

  /**
   * Sets the number of documents to return per batch.
   *
   * @param batchSize the batch size
   * @return this
   * @since 2.7
   */
  def batchSize(batchSize: Int): AggregateObservable[TResult] = {
    wrapped.batchSize(batchSize)
    this
  }

  /**
   * Aggregates documents according to the specified aggregation pipeline, which must end with a `\$out` stage.
   *
   * [[http://docs.mongodb.org/manual/aggregation/ Aggregation]]
   * @return a Observable with a single element indicating when the operation has completed
   */
  def toCollection(): SingleObservable[Completed] = wrapped.toCollection()

  /**
   * Helper to return a single observable limited to the first result.
   *
   * @return a single observable which will the first result.
   * @since 4.0
   */
  def first(): SingleObservable[TResult] = wrapped.first()

  override def subscribe(observer: Observer[_ >: TResult]): Unit = wrapped.subscribe(observer)
}
