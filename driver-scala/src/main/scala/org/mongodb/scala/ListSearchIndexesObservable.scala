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

import com.mongodb.ExplainVerbosity
import com.mongodb.reactivestreams.client.ListSearchIndexesPublisher
import org.mongodb.scala.bson.BsonValue
import org.mongodb.scala.bson.DefaultHelper.DefaultsTo
import org.mongodb.scala.model.Collation

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

/**
 * Observable interface for ListSearchIndexes.
 *
 * @param wrapped the underlying java ListSearchIndexesPublisher
 * @tparam TResult The type of the result.
 * @since 4.11
 */
case class ListSearchIndexesObservable[TResult](wrapped: ListSearchIndexesPublisher[TResult])
    extends Observable[TResult] {

  /**
   * Sets an Atlas Search index name for this operation. A null value means no index name is set.
   *
   * @param indexName Atlas Search index name.
   * @note Requires MongoDB 7.0 or greater
   */
  def name(indexName: String): ListSearchIndexesObservable[TResult] = {
    wrapped.name(indexName)
    this
  }

  /**
   * Enables writing to temporary files. A null value indicates that it's unspecified.
   *
   * @param allowDiskUse true if writing to temporary files is enabled.
   * @return this.
   * @see [[https://www.mongodb.com/docs/manual/reference/command/aggregate/ Aggregation]]
   */
  def allowDiskUse(allowDiskUse: Boolean): ListSearchIndexesObservable[TResult] = {
    wrapped.allowDiskUse(allowDiskUse)
    this
  }

  /**
   * Sets the maximum execution time on the server for this operation.
   *
   * @param duration the duration.
   * @return this.
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/meta/maxTimeMS/ Max Time]]
   */
  def maxTime(duration: Duration): ListSearchIndexesObservable[TResult] = {
    wrapped.maxTime(duration.toMillis, TimeUnit.MILLISECONDS)
    this
  }

  /**
   * Sets the maximum await execution time on the server for this operation.
   *
   * @param duration the duration.
   * @return this.
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater.
   * @see [[https://www.mongodb.com/docs/manual/reference/method/cursor.maxAwaitTimeMS/ Max Await Time]]
   */
  def maxAwaitTime(duration: Duration): ListSearchIndexesObservable[TResult] = {
    wrapped.maxAwaitTime(duration.toMillis, TimeUnit.MILLISECONDS)
    this
  }

  /**
   * Sets the collation options
   *
   * @param collation the collation options to use.
   * @return this.
   * @since 1.2
   * @note A null value represents the server default.
   * @note Requires MongoDB 3.4 or greater.
   */
  def collation(collation: Collation): ListSearchIndexesObservable[TResult] = {
    wrapped.collation(collation)
    this
  }

  /**
   * Sets the comment for this operation. A null value means no comment is set.
   *
   * @param comment the comment.
   * @return this.
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater.
   */
  def comment(comment: String): ListSearchIndexesObservable[TResult] = {
    wrapped.comment(comment)
    this
  }

  /**
   * Sets the comment for this operation. A null value means no comment is set.
   *
   * @param comment the comment
   * @return this
   * @since 4.6
   * @note The comment can be any valid BSON type for server versions 4.4 and above.
   *       Server versions between 3.6 and 4.2 only support
   *       string as comment, and providing a non-string type will result in a server-side error.
   */
  def comment(comment: BsonValue): ListSearchIndexesObservable[TResult] = {
    wrapped.comment(comment)
    this
  }

  /**
   * Sets the number of documents to return per batch.
   *
   * @param batchSize the batch size.
   * @return this.
   * @since 2.7
   */
  def batchSize(batchSize: Int): ListSearchIndexesObservable[TResult] = {
    wrapped.batchSize(batchSize)
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
   * Explain the execution plan for this operation with the server's default verbosity level.
   *
   * @tparam ExplainResult The type of the result.
   * @return the execution plan.
   * @since 4.2
   * @note Requires MongoDB 3.6 or greater.
   */
  def explain[ExplainResult]()(
      implicit e: ExplainResult DefaultsTo Document,
      ct: ClassTag[ExplainResult]
  ): SingleObservable[ExplainResult] =
    wrapped.explain[ExplainResult](ct)

  /**
   * Explain the execution plan for this operation with the given verbosity level.
   *
   * @tparam ExplainResult The type of the result.
   * @param verbosity the verbosity of the explanation.
   * @return the execution plan.
   * @since 4.2
   * @note Requires MongoDB 3.6 or greater.
   */
  def explain[ExplainResult](
      verbosity: ExplainVerbosity
  )(implicit e: ExplainResult DefaultsTo Document, ct: ClassTag[ExplainResult]): SingleObservable[ExplainResult] =
    wrapped.explain[ExplainResult](ct, verbosity)

  override def subscribe(observer: Observer[_ >: TResult]): Unit = wrapped.subscribe(observer)
}
