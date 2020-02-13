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

import com.mongodb.CursorType
import com.mongodb.reactivestreams.client.FindPublisher
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.Collation

import scala.concurrent.duration.Duration

/**
 * Observable interface for Find.
 *
 * @param wrapped the underlying java FindObservable
 * @tparam TResult The type of the result.
 * @since 1.0
 */
case class FindObservable[TResult](private val wrapped: FindPublisher[TResult]) extends Observable[TResult] {

  /**
   * Helper to return a Observable limited to just the first result the query.
   *
   * **Note:** Sets limit in the background so only returns 1.
   *
   * @return a Observable which will return the first item
   */
  def first(): SingleObservable[TResult] = wrapped.first()

  /**
   * Sets the query filter to apply to the query.
   *
   * [[http://docs.mongodb.org/manual/reference/method/db.collection.find/ Filter]]
   * @param filter the filter, which may be null.
   * @return this
   */
  def filter(filter: Bson): FindObservable[TResult] = {
    wrapped.filter(filter)
    this
  }

  /**
   * Sets the limit to apply.
   *
   * [[http://docs.mongodb.org/manual/reference/method/cursor.limit/#cursor.limit Limit]]
   * @param limit the limit, which may be null
   * @return this
   */
  def limit(limit: Int): FindObservable[TResult] = {
    wrapped.limit(limit)
    this
  }

  /**
   * Sets the number of documents to skip.
   *
   * [[http://docs.mongodb.org/manual/reference/method/cursor.skip/#cursor.skip Skip]]
   * @param skip the number of documents to skip
   * @return this
   */
  def skip(skip: Int): FindObservable[TResult] = {
    wrapped.skip(skip)
    this
  }

  /**
   * Sets the maximum execution time on the server for this operation.
   *
   * [[http://docs.mongodb.org/manual/reference/operator/meta/maxTimeMS/ Max Time]]
   * @param duration the duration
   * @return this
   */
  def maxTime(duration: Duration): FindObservable[TResult] = {
    wrapped.maxTime(duration.toMillis, TimeUnit.MILLISECONDS)
    this
  }

  /**
   * The maximum amount of time for the server to wait on new documents to satisfy a tailable cursor
   * query. This only applies to a TAILABLE_AWAIT cursor. When the cursor is not a TAILABLE_AWAIT cursor,
   * this option is ignored.
   *
   * On servers &gt;= 3.2, this option will be specified on the getMore command as "maxTimeMS". The default
   * is no value: no "maxTimeMS" is sent to the server with the getMore command.
   *
   * On servers &lt; 3.2, this option is ignored, and indicates that the driver should respect the server's default value
   *
   * A zero value will be ignored.
   *
   * [[http://docs.mongodb.org/manual/reference/operator/meta/maxTimeMS/ Max Time]]
   * @param duration the duration
   * @return the maximum await execution time in the given time unit
   * @since 1.1
   */
  def maxAwaitTime(duration: Duration): FindObservable[TResult] = {
    wrapped.maxAwaitTime(duration.toMillis, TimeUnit.MILLISECONDS)
    this
  }

  /**
   * Sets a document describing the fields to return for all matching documents.
   *
   * [[http://docs.mongodb.org/manual/reference/method/db.collection.find/ Projection]]
   * @param projection the project document, which may be null.
   * @return this
   */
  def projection(projection: Bson): FindObservable[TResult] = {
    wrapped.projection(projection)
    this
  }

  /**
   * Sets the sort criteria to apply to the query.
   *
   * [[http://docs.mongodb.org/manual/reference/method/cursor.sort/ Sort]]
   * @param sort the sort criteria, which may be null.
   * @return this
   */
  def sort(sort: Bson): FindObservable[TResult] = {
    wrapped.sort(sort)
    this
  }

  /**
   * The server normally times out idle cursors after an inactivity period (10 minutes)
   * to prevent excess memory use. Set this option to prevent that.
   *
   * @param noCursorTimeout true if cursor timeout is disabled
   * @return this
   */
  def noCursorTimeout(noCursorTimeout: Boolean): FindObservable[TResult] = {
    wrapped.noCursorTimeout(noCursorTimeout)
    this
  }

  /**
   * Users should not set this under normal circumstances.
   *
   * @param oplogReplay if oplog replay is enabled
   * @return this
   */
  def oplogReplay(oplogReplay: Boolean): FindObservable[TResult] = {
    wrapped.oplogReplay(oplogReplay)
    this
  }

  /**
   * Get partial results from a sharded cluster if one or more shards are unreachable (instead of throwing an error).
   *
   * @param partial if partial results for sharded clusters is enabled
   * @return this
   */
  def partial(partial: Boolean): FindObservable[TResult] = {
    wrapped.partial(partial)
    this
  }

  /**
   * Sets the cursor type.
   *
   * @param cursorType the cursor type
   * @return this
   */
  def cursorType(cursorType: CursorType): FindObservable[TResult] = {
    wrapped.cursorType(cursorType)
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
  def collation(collation: Collation): FindObservable[TResult] = {
    wrapped.collation(collation)
    this
  }

  /**
   * Sets the comment to the query. A null value means no comment is set.
   *
   * @param comment the comment
   * @return this
   * @since 2.2
   */
  def comment(comment: String): FindObservable[TResult] = {
    wrapped.comment(comment)
    this
  }

  /**
   * Sets the hint for which index to use. A null value means no hint is set.
   *
   * @param hint the hint
   * @return this
   * @since 2.2
   */
  def hint(hint: Bson): FindObservable[TResult] = {
    wrapped.hint(hint)
    this
  }

  /**
   * Sets the hint for which index to use. A null value means no hint is set.
   *
   * @param hint the name of the index which should be used for the operation
   * @return this
   * @note if [[hint]] is set that will be used instead of any hint string.
   * @since 2.8
   */
  def hintString(hint: String): FindObservable[TResult] = {
    wrapped.hintString(hint)
    this
  }

  /**
   * Sets the exclusive upper bound for a specific index. A null value means no max is set.
   *
   * @param max the max
   * @return this
   * @since 2.2
   */
  def max(max: Bson): FindObservable[TResult] = {
    wrapped.max(max)
    this
  }

  /**
   * Sets the minimum inclusive lower bound for a specific index. A null value means no max is set.
   *
   * @param min the min
   * @return this
   * @since 2.2
   */
  def min(min: Bson): FindObservable[TResult] = {
    wrapped.min(min)
    this
  }

  /**
   * Sets the returnKey. If true the find operation will return only the index keys in the resulting documents.
   *
   * @param returnKey the returnKey
   * @return this
   * @since 2.2
   */
  def returnKey(returnKey: Boolean): FindObservable[TResult] = {
    wrapped.returnKey(returnKey)
    this
  }

  /**
   * Sets the showRecordId. Set to true to add a field `\$recordId` to the returned documents.
   *
   * @param showRecordId the showRecordId
   * @return this
   * @since 2.2
   */
  def showRecordId(showRecordId: Boolean): FindObservable[TResult] = {
    wrapped.showRecordId(showRecordId)
    this
  }

  /**
   * Sets the number of documents to return per batch.
   *
   * @param batchSize the batch size
   * @return this
   * @since 2.7
   */
  def batchSize(batchSize: Int): FindObservable[TResult] = {
    wrapped.batchSize(batchSize)
    this
  }

  /**
   * Enables writing to temporary files on the server. When set to true, the server
   * can write temporary data to disk while executing the find operation.
   *
   * <p>This option is sent only if the caller explicitly provides a value. The default
   * is to not send a value. For servers &lt; 3.2, this option is ignored and not sent
   * as allowDiskUse does not exist in the OP_QUERY wire protocol.</p>
   *
   * @param allowDiskUse the allowDiskUse
   * @since 4.1
   * @note Requires MongoDB 4.4 or greater
   */
  def allowDiskUse(allowDiskUse: Boolean): FindObservable[TResult] = {
    wrapped.allowDiskUse(allowDiskUse)
    this
  }

  override def subscribe(observer: Observer[_ >: TResult]): Unit = wrapped.subscribe(observer)
}
