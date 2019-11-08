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

import org.mongodb.scala.SingleObservable

/**
 * The Async Input Stream interface represents some asynchronous input stream of bytes.
 *
 * See the [[org.mongodb.scala.gridfs.helpers]] package for adapters that create an `AsyncInputStream`
 *
 * @since 1.2
 */
trait AsyncInputStream {

  /**
   * Reads a sequence of bytes from this stream into the given buffer.
   *
   * @param dst      the destination buffer
   * @return an Observable with a single element indicating total number of bytes read into the buffer, or
   *         `-1` if there is no more data because the end of the stream has been reached.
   */
  def read(dst: ByteBuffer): SingleObservable[Int]

  /**
   * Skips over and discards n bytes of data from this input stream.
   *
   * @param bytesToSkip the number of bytes to skip
   * @return an Observable with a single element indicating the total number of bytes skipped
   * @since 2.6
   */
  def skip(bytesToSkip: Long): SingleObservable[Long]

  /**
   * Closes the input stream
   *
   * @return a Observable with a single element indicating when the operation has completed
   */
  def close(): SingleObservable[Void]
}
