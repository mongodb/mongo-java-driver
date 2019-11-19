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

import java.nio.ByteBuffer

import com.mongodb.reactivestreams.client.gridfs.{ GridFSDownloadStream => JGridFSDownloadStream }
import org.mongodb.scala.SingleObservable

/**
 * A GridFS InputStream for downloading data from GridFS
 *
 * Provides the `GridFSFile` for the file to being downloaded as well as the `read` methods of a `AsyncInputStream`
 *
 * @since 1.2
 */
case class GridFSDownloadStream(private val wrapped: JGridFSDownloadStream) extends AsyncInputStream {

  /**
   * Gets the corresponding GridFSFile for the file being downloaded
   *
   * @return a Observable with a single element containing the corresponding GridFSFile for the file being downloaded
   */
  def gridFSFile(): SingleObservable[GridFSFile] = wrapped.getGridFSFile()

  /**
   * Sets the number of chunks to return per batch.
   *
   * Can be used to control the memory consumption of this InputStream. The smaller the batchSize the lower the memory consumption
   * and higher latency.
   *
   * @param batchSize the batch size
   * @return this
   * @see [[http://http://docs.mongodb.org/manual/reference/method/cursor.batchSize/#cursor.batchSize Batch Size]]
   */
  def batchSize(batchSize: Int): GridFSDownloadStream = {
    wrapped.batchSize(batchSize)
    this
  }

  /**
   * Reads a sequence of bytes from this stream into the given buffer.
   *
   * @param dst the destination buffer
   * @return an Observable with a single element indicating total number of bytes read into the buffer, or
   *         `-1` if there is no more data because the end of the stream has been reached.
   */
  override def read(dst: ByteBuffer): SingleObservable[Int] = wrapped.read(dst)

  /**
   * Skips over and discards n bytes of data from this input stream.
   *
   * @param bytesToSkip the number of bytes to skip
   * @return an Observable with a single element indicating the total number of bytes skipped
   * @since 2.6
   */
  override def skip(bytesToSkip: Long): SingleObservable[Long] = wrapped.skip(bytesToSkip)

  /**
   * Closes the input stream
   *
   * @return an empty Observable that indicates when the operation has completed
   */
  override def close(): SingleObservable[Void] = wrapped.close()
}
