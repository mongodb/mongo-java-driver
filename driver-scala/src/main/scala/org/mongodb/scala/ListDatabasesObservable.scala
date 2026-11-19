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
import com.mongodb.reactivestreams.client.ListDatabasesPublisher
import org.mongodb.scala.bson.BsonValue
import org.mongodb.scala.bson.conversions.Bson

import scala.concurrent.duration.Duration

/**
 * Observable interface for ListDatabases.
 *
 * @param wrapped the underlying java ListDatabasesObservable
 * @tparam TResult The type of the result.
 * @since 1.0
 */
case class ListDatabasesObservable[TResult](wrapped: ListDatabasesPublisher[TResult]) extends Observable[TResult] {

  /**
   * Sets the maximum execution time on the server for this operation.
   *
   * [[https://www.mongodb.com/docs/manual/reference/operator/meta/maxTimeMS/ Max Time]]
   * @param duration the duration
   * @return this
   */
  def maxTime(duration: Duration): ListDatabasesObservable[TResult] = {
    wrapped.maxTime(duration.toMillis, TimeUnit.MILLISECONDS)
    this
  }

  /**
   * Sets the query filter to apply to the returned database names.
   *
   * @param filter the filter, which may be null.
   * @return this
   * @since 2.2
   * @note Requires MongoDB 3.4.2 or greater
   */
  def filter(filter: Bson): ListDatabasesObservable[TResult] = {
    wrapped.filter(filter)
    this
  }

  /**
   * Sets the nameOnly flag that indicates whether the command should return just the database names or return the database names and
   * size information.
   *
   * @param nameOnly the nameOnly flag, which may be null
   * @return this
   * @since 2.2
   * @note Requires MongoDB 3.4.3 or greater
   */
  def nameOnly(nameOnly: Boolean): ListDatabasesObservable[TResult] = {
    wrapped.nameOnly(nameOnly)
    this
  }

  /**
   * Sets the authorizedDatabasesOnly flag that indicates whether the command should return just the databases which the user
   * is authorized to see.
   *
   * @param authorizedDatabasesOnly the authorizedDatabasesOnly flag, which may be null
   * @return this
   * @since 4.1
   * @note Requires MongoDB 4.0.5 or greater
   */
  def authorizedDatabasesOnly(authorizedDatabasesOnly: Boolean): ListDatabasesObservable[TResult] = {
    wrapped.authorizedDatabasesOnly(authorizedDatabasesOnly)
    this
  }

  /**
   * Sets the number of documents to return per batch.
   *
   * @param batchSize the batch size
   * @return this
   * @since 2.7
   */
  def batchSize(batchSize: Int): ListDatabasesObservable[TResult] = {
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
  def comment(comment: String): ListDatabasesObservable[TResult] = {
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
  def comment(comment: BsonValue): ListDatabasesObservable[TResult] = {
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
