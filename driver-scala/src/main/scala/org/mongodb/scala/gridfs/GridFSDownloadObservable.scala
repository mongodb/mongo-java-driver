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

import java.nio.ByteBuffer

import com.mongodb.reactivestreams.client.gridfs.GridFSDownloadPublisher
import org.mongodb.scala.{ Observable, Observer }

/**
 * A GridFS Observable for downloading data from GridFS
 *
 * Provides the `GridFSFile` for the file to being downloaded as well as a way to control the batchsize.
 *
 * @since 2.8
 */
case class GridFSDownloadObservable(private val wrapped: GridFSDownloadPublisher) extends Observable[ByteBuffer] {

  /**
   * Gets the corresponding [[GridFSFile]] for the file being downloaded
   *
   * @return a Publisher with a single element, the corresponding GridFSFile for the file being downloaded
   */
  def gridFSFile(): Observable[GridFSFile] = wrapped.getGridFSFile

  /**
   * The preferred number of bytes per `ByteBuffer` returned by the `Observable`.
   *
   * Allows for larger than chunk size ByteBuffers. The actual chunk size of the data stored in MongoDB is the smallest allowable
   * `ByteBuffer` size.
   *
   * Can be used to control the memory consumption of this `Observable`. The smaller the bufferSizeBytes the lower the memory
   * consumption and higher latency.
   *
   * '''Note:''' Must be set before the Observable is subscribed to
   *
   * @param bufferSizeBytes the preferred buffer size in bytes to use per `ByteBuffer` in the `Observable`, defaults to chunk size.
   * @return this
   */
  def bufferSizeBytes(bufferSizeBytes: Int): GridFSDownloadObservable =
    GridFSDownloadObservable(wrapped.bufferSizeBytes(bufferSizeBytes))

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
  override def subscribe(observer: Observer[_ >: ByteBuffer]): Unit = wrapped.subscribe(observer)
}
