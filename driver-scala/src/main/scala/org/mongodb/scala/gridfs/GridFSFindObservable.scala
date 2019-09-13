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

package org.mongodb.scala.gridfs

import java.util.concurrent.TimeUnit

import com.mongodb.reactivestreams.client.gridfs.GridFSFindPublisher
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.{ Observable, Observer, SingleObservable }

import scala.concurrent.duration.Duration

/**
 * Observable representing the GridFS Files Collection.
 *
 * @since 1.2
 */
case class GridFSFindObservable(private val wrapped: GridFSFindPublisher) extends Observable[GridFSFile] {

  /**
   * Sets the query filter to apply to the query.
   *
   * Below is an example of filtering against the filename and some nested metadata that can also be stored along with the file data:
   *
   * {{{
   * Filters.and(Filters.eq("filename", "mongodb.png"), Filters.eq("metadata.contentType", "image/png"));
   * }}}
   *
   * @param filter the filter, which may be null.
   * @return this
   * @see [[http://docs.mongodb.org/manual/reference/method/db.collection.find/ Filter]]
   * @see [[org.mongodb.scala.model.Filters]]
   */
  def filter(filter: Bson): GridFSFindObservable = {
    wrapped.filter(filter)
    this
  }

  /**
   * Sets the limit to apply.
   *
   * @param limit the limit, which may be null
   * @return this
   * @see [[http://docs.mongodb.org/manual/reference/method/cursor.limit/#cursor.limit Limit]]
   */
  def limit(limit: Int): GridFSFindObservable = {
    wrapped.limit(limit)
    this
  }

  /**
   * Sets the number of documents to skip.
   *
   * @param skip the number of documents to skip
   * @return this
   * @see [[http://docs.mongodb.org/manual/reference/method/cursor.skip/#cursor.skip Skip]]
   */
  def skip(skip: Int): GridFSFindObservable = {
    wrapped.skip(skip)
    this
  }

  /**
   * Sets the sort criteria to apply to the query.
   *
   * @param sort the sort criteria, which may be null.
   * @return this
   * @see [[http://docs.mongodb.org/manual/reference/method/cursor.sort/ Sort]]
   */
  def sort(sort: Bson): GridFSFindObservable = {
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
  def noCursorTimeout(noCursorTimeout: Boolean): GridFSFindObservable = {
    wrapped.noCursorTimeout(noCursorTimeout)
    this
  }

  /**
   * Sets the maximum execution time on the server for this operation.
   *
   * @see [[http://docs.mongodb.org/manual/reference/operator/meta/maxTimeMS/ Max Time]]
   * @param duration the duration
   * @return this
   */
  def maxTime(duration: Duration): GridFSFindObservable = {
    wrapped.maxTime(duration.toMillis, TimeUnit.MILLISECONDS)
    this
  }

  /**
   * Sets the number of documents to return per batch.
   *
   * @param batchSize the batch size
   * @return this
   * @see [[http://docs.mongodb.org/manual/reference/method/cursor.batchSize/#cursor.batchSize Batch Size]]
   */
  def batchSize(batchSize: Int): GridFSFindObservable = {
    wrapped.batchSize(batchSize)
    this
  }

  /**
   * Helper to return a single observable limited to the first result.
   *
   * @return a single observable which will the first result.
   * @since 4.0
   */
  def first(): SingleObservable[GridFSFile] = wrapped.first()

  /**
   * Request `Observable` to start streaming data.
   *
   * This is a "factory method" and can be called multiple times, each time starting a new [[org.mongodb.scala.Subscription]].
   * Each `Subscription` will work for only a single [[Observer]].
   *
   * If the `Observable` rejects the subscription attempt or otherwise fails it will signal the error via [[Observer.onError]].
   *
   * @param observer the `Observer` that will consume signals from this `Observable`
   */
  override def subscribe(observer: Observer[_ >: GridFSFile]): Unit = wrapped.subscribe(observer)
}
