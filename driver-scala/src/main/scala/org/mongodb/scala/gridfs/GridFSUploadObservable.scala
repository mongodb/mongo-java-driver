/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb.scala.gridfs

import com.mongodb.reactivestreams.client.gridfs.GridFSUploadPublisher
import org.bson.BsonValue
import org.bson.types.ObjectId
import org.mongodb.scala.{ Observer, SingleObservable }

/**
 * A GridFS `Observable` for uploading data into GridFS
 *
 * Provides the `id` for the file to be uploaded. Cancelling the subscription to this publisher will cause any uploaded data
 * to be cleaned up and removed.
 *
 * @tparam T the result type of the publisher
 * @since 2.8
 */
case class GridFSUploadObservable[T](private val wrapped: GridFSUploadPublisher[T]) extends SingleObservable[T] {

  /**
   * Gets the ObjectId for the file to be uploaded
   *
   * @throws MongoGridFSException if the file id is not an ObjectId.
   *
   * @return the ObjectId for the file to be uploaded
   */
  lazy val objectId: ObjectId = wrapped.getObjectId

  /**
   * The BsonValue id for this file.
   *
   * @return the id for this file
   */
  lazy val id: BsonValue = wrapped.getId

  /**
   * Request `Observable` to start streaming data.
   *
   * This is a "factory method" and can be called multiple times, each time starting a new `Subscription`.
   * Each `Subscription` will work for only a single [[Observer]].
   *
   * If the `Observable` rejects the subscription attempt or otherwise fails it will signal the error via [[Observer.onError]].
   *
   * @param observer the `Observer` that will consume signals from this `Observable`
   */
  override def subscribe(observer: Observer[_ >: T]): Unit = wrapped.subscribe(observer)

}
