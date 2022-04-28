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
import com.mongodb.reactivestreams.client.ChangeStreamPublisher
import org.mongodb.scala.bson.{ BsonTimestamp, BsonValue }
import org.mongodb.scala.model.Collation
import org.mongodb.scala.model.changestream.{ ChangeStreamDocument, FullDocument, FullDocumentBeforeChange }

import scala.concurrent.duration.Duration

/**
 * Observable for change streams.
 *
 * '''Note:''' The `ChangeStreamDocument` class will not be applicable for all change stream outputs.
 * If using custom pipelines that radically change the result, the [[ChangeStreamObservable#withDocumentClass]] method should be used
 * to provide an alternative document format.
 *
 * @param wrapped the underlying java ChangeStreamIterable
 * @tparam TResult The type of the result.
 * @since 2.2
 * @note Requires MongoDB 3.6 or greater
 */
case class ChangeStreamObservable[TResult](private val wrapped: ChangeStreamPublisher[TResult])
    extends Observable[ChangeStreamDocument[TResult]] {

  /**
   * Sets the fullDocument value.
   *
   * @param fullDocument the fullDocument
   * @return this
   */
  def fullDocument(fullDocument: FullDocument): ChangeStreamObservable[TResult] = {
    wrapped.fullDocument(fullDocument)
    this
  }

  /**
   * Sets the fullDocumentBeforeChange value.
   *
   * @param fullDocumentBeforeChange the fullDocumentBeforeChange
   * @return this
   * @since 4.7
   * @note Requires MongoDB 6.0 or greater
   */
  def fullDocumentBeforeChange(fullDocumentBeforeChange: FullDocumentBeforeChange): ChangeStreamObservable[TResult] = {
    wrapped.fullDocumentBeforeChange(fullDocumentBeforeChange)
    this
  }

  /**
   * Sets the logical starting point for the new change stream.
   *
   * @param resumeToken the resume token
   * @return this
   */
  def resumeAfter(resumeToken: Document): ChangeStreamObservable[TResult] = {
    wrapped.resumeAfter(resumeToken.underlying)
    this
  }

  /**
   * The change stream will only provide changes that occurred at or after the specified timestamp.
   *
   * Any command run against the server will return an operation time that can be used here.
   * The default value is an operation time obtained from the server before the change stream was created.
   *
   * @param startAtOperationTime the start at operation time
   * @return this
   * @since 2.4
   * @note Requires MongoDB 4.0 or greater
   */
  def startAtOperationTime(startAtOperationTime: BsonTimestamp): ChangeStreamObservable[TResult] = {
    wrapped.startAtOperationTime(startAtOperationTime)
    this
  }

  /**
   * Sets the logical starting point for the new change stream.
   *
   *
   * This will allow users to watch collections that have been dropped and recreated or newly renamed collections without missing
   * any notifications.
   *
   * @param startAfter the resume token
   * @return this
   * @since 2.7
   * @note Requires MongoDB 4.2 or greater
   * @note The server will report an error if both `startAfter` and `resumeAfter` are specified.
   * @see [[https://www.mongodb.com/docs/manual/changeStreams/#change-stream-start-after Change stream start after]]
   */
  def startAfter(startAfter: Document): ChangeStreamObservable[TResult] = {
    wrapped.startAfter(startAfter.underlying)
    this
  }

  /**
   * Sets the number of documents to return per batch.
   *
   * @param batchSize the batch size
   * @return this
   */
  def batchSize(batchSize: Int): ChangeStreamObservable[TResult] = {
    wrapped.batchSize(batchSize)
    this
  }

  /**
   * Sets the maximum await execution time on the server for this operation.
   *
   * [[https://www.mongodb.com/docs/manual/reference/operator/meta/maxTimeMS/ Max Time]]
   * @param duration the duration
   * @return this
   */
  def maxAwaitTime(duration: Duration): ChangeStreamObservable[TResult] = {
    wrapped.maxAwaitTime(duration.toMillis, TimeUnit.MILLISECONDS)
    this
  }

  /**
   * Sets the collation options
   *
   * A null value represents the server default.
   *
   * @param collation the collation options to use
   * @return this
   */
  def collation(collation: Collation): ChangeStreamObservable[TResult] = {
    wrapped.collation(collation)
    this
  }

  /**
   * Sets the comment for this operation. A null value means no comment is set.
   *
   * @param comment the comment
   * @return this
   * @since 4.6
   * @note Requires MongoDB 3.6 or greater
   */
  def comment(comment: String): ChangeStreamObservable[TResult] = {
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
  def comment(comment: BsonValue): ChangeStreamObservable[TResult] = {
    wrapped.comment(comment)
    this
  }

  /**
   * Returns an `Observable` containing the results of the change stream based on the document class provided.
   *
   * @param clazz the class to use for the raw result.
   * @tparam T the result type
   * @return an Observable
   */
  def withDocumentClass[T](clazz: Class[T]): Observable[T] = wrapped.withDocumentClass(clazz).toObservable()

  /**
   * Helper to return a single observable limited to the first result.
   *
   * @return a single observable which will the first result.
   * @since 4.0
   */
  def first(): SingleObservable[ChangeStreamDocument[TResult]] = wrapped.first()

  override def subscribe(observer: Observer[_ >: ChangeStreamDocument[TResult]]): Unit = wrapped.subscribe(observer)
}
