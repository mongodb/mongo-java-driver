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
import com.mongodb.reactivestreams.client.ListCollectionsPublisher
import org.mongodb.scala.bson.BsonValue
import org.mongodb.scala.bson.conversions.Bson

import scala.concurrent.duration.Duration

/**
 * Observable interface for ListCollections
 *
 * @param wrapped the underlying java ListCollectionsObservable
 * @tparam TResult The type of the result.
 * @since 1.0
 */
case class ListCollectionsObservable[TResult](wrapped: ListCollectionsPublisher[TResult]) extends Observable[TResult] {

  /**
   * Sets the query filter to apply to the query.
   *
   * [[https://www.mongodb.com/docs/manual/reference/method/db.collection.find/ Filter]]
   * @param filter the filter, which may be null.
   * @return this
   */
  def filter(filter: Bson): ListCollectionsObservable[TResult] = {
    wrapped.filter(filter)
    this
  }

  /**
   * Sets the maximum execution time on the server for this operation.
   *
   * [[https://www.mongodb.com/docs/manual/reference/operator/meta/maxTimeMS/ Max Time]]
   * @param duration the duration
   * @return this
   */
  def maxTime(duration: Duration): ListCollectionsObservable[TResult] = {
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
  def batchSize(batchSize: Int): ListCollectionsObservable[TResult] = {
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
  def comment(comment: String): ListCollectionsObservable[TResult] = {
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
  def comment(comment: BsonValue): ListCollectionsObservable[TResult] = {
    wrapped.comment(comment)
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
