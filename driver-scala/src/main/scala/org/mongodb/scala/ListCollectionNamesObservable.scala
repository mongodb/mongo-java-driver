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

import com.mongodb.reactivestreams.client.ListCollectionNamesPublisher
import org.mongodb.scala.bson.BsonValue
import org.mongodb.scala.bson.conversions.Bson

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

/**
 * Observable for listing collection names.
 *
 * @param wrapped the underlying java ListCollectionNamesPublisher
 * @since 5.0
 */
case class ListCollectionNamesObservable(wrapped: ListCollectionNamesPublisher) extends Observable[String] {

  /**
   * Sets the query filter to apply to the query.
   *
   * [[https://www.mongodb.com/docs/manual/reference/method/db.collection.find/ Filter]]
   * @param filter the filter, which may be null.
   * @return this
   */
  def filter(filter: Bson): ListCollectionNamesObservable = {
    wrapped.filter(filter)
    this
  }

  /**
   * Sets the maximum execution time on the server for this operation.
   *
   * [[https://www.mongodb.com/docs/manual/reference/operator/meta/maxTimeMS/ Max Time]]
   *
   * @param duration the duration
   * @return this
   * @deprecated Prefer using the operation execution timeout configuration options available at the following levels:
   *
     *             - [[org.mongodb.scala.MongoClientSettings.Builder timeout(long, TimeUnit)]]
     *             - [[org.mongodb.scala.MongoDatabase.withTimeout withTimeout(long, TimeUnit)]]
     *             - [[org.mongodb.scala.MongoCollection.withTimeout withTimeout(long, TimeUnit)]]
     *             - [[org.mongodb.scala.ClientSessionOptions]]
     *             - [[org.mongodb.scala.TransactionOptions]]
   *
   * When executing an operation, any explicitly set timeout at these levels takes precedence, rendering this maximum
   *             execution time irrelevant. If no timeout is specified at these levels, the maximum execution time will be used.
   */
  @deprecated
  def maxTime(duration: Duration): ListCollectionNamesObservable = {
    wrapped.maxTime(duration.toMillis, TimeUnit.MILLISECONDS)
    this
  }

  /**
   * Sets the number of documents to return per batch.
   *
   * @param batchSize the batch size
   * @return this
   */
  def batchSize(batchSize: Int): ListCollectionNamesObservable = {
    wrapped.batchSize(batchSize)
    this
  }

  /**
   * Sets the comment for this operation. A null value means no comment is set.
   *
   * @param comment the comment
   * @return this
   * @note Requires MongoDB 4.4 or greater
   */
  def comment(comment: String): ListCollectionNamesObservable = {
    wrapped.comment(comment)
    this
  }

  /**
   * Sets the comment for this operation. A null value means no comment is set.
   *
   * @param comment the comment
   * @return this
   * @note Requires MongoDB 4.4 or greater
   */
  def comment(comment: BsonValue): ListCollectionNamesObservable = {
    wrapped.comment(comment)
    this
  }

  /**
   * Sets the `authorizedCollections` field of the `istCollections` command.
   *
   * @param authorizedCollections If `true`, allows executing the `listCollections` command,
   * which has the `nameOnly` field set to `true`, without having the
   * <a href="https://docs.mongodb.com/manual/reference/privilege-actions/#mongodb-authaction-listCollections">
   * `listCollections` privilege</a> on the database resource.
   * @return `this`.
   * @note Requires MongoDB 4.0 or greater
   */
  def authorizedCollections(authorizedCollections: Boolean): ListCollectionNamesObservable = {
    wrapped.authorizedCollections(authorizedCollections)
    this
  }

  /**
   * Helper to return a single observable limited to the first result.
   *
   * @return a single observable which will the first result.
   */
  def first(): SingleObservable[String] = wrapped.first()

  override def subscribe(observer: Observer[_ >: String]): Unit = wrapped.subscribe(observer)
}
