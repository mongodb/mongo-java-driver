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

import com.mongodb.reactivestreams.client.gridfs.GridFSUploadPublisher
import org.bson.BsonValue
import org.mongodb.scala.bson.ObjectId
import org.reactivestreams.Subscriber
import reactor.core.publisher.Flux

package object gridfs extends ObservableImplicits {

  /**
   * An exception indicating that a failure occurred in GridFS.
   */
  type MongoGridFSException = com.mongodb.MongoGridFSException

  /**
   * GridFS upload options
   *
   * Customizable options used when uploading files into GridFS
   */
  type GridFSUploadOptions = com.mongodb.client.gridfs.model.GridFSUploadOptions

  /**
   * The GridFSFile
   */
  type GridFSFile = com.mongodb.client.gridfs.model.GridFSFile

  /**
   * The GridFS download by name options
   *
   * Controls the selection of the revision to download
   */
  type GridFSDownloadOptions = com.mongodb.client.gridfs.model.GridFSDownloadOptions

  /**
   * A `GridFSUploadPublisher`` that emits
   *
   *   - exactly one item, if the wrapped `Publisher` does not signal an error, even if the represented stream is empty;
   *   - no items if the wrapped `Publisher` signals an error.
   *
   * @param pub A `Publisher` representing a finite stream.
   */
  implicit class ToGridFSUploadPublisherUnit(pub: => GridFSUploadPublisher[Void]) extends GridFSUploadPublisher[Unit] {
    val publisher = pub

    override def subscribe(observer: Subscriber[_ >: Unit]): Unit =
      Flux.from(publisher).reduce((), (_: Unit, _: Void) => ()).subscribe(observer)

    override def getObjectId: ObjectId = publisher.getObjectId

    override def getId: BsonValue = publisher.getId
  }
}
