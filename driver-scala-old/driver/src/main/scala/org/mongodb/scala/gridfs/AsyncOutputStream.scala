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

import org.mongodb.scala.{Completed, Observable, SingleObservable}

/**
 * The Async Output Stream interface represents some asynchronous output stream of bytes.
 *
 * See the [[org.mongodb.scala.gridfs.helpers]] package for adapters that create an `AsyncOutputStream`
 *
 * @since 1.2
 */
trait AsyncOutputStream {
  /**
   * Writes a sequence of bytes from the given buffer into this stream.
   *
   * @param src the source buffer containing the data to be written.
   * @return a Observable returning a single element containing the number of bytes written.
   */
  def write(src: ByteBuffer): Observable[Int]

  /**
   * Closes the output stream
   *
   * @return a Observable with a single element indicating when the AsyncOutputStream has been closed
   */
  def close(): SingleObservable[Completed]
}
