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

package org.mongodb.scala.gridfs.helpers

import java.io.{ InputStream, OutputStream }
import java.nio.ByteBuffer

import com.mongodb.reactivestreams.client.gridfs.helpers.{ AsyncStreamHelper => JAsyncStreamHelper }

import org.mongodb.scala.gridfs.{ AsyncInputStream, AsyncOutputStream }

/**
 * A general helper class that creates [[org.mongodb.scala.gridfs.AsyncInputStream]] or
 * [[org.mongodb.scala.gridfs.AsyncOutputStream]] instances.
 *
 * Provides support for:
 *
 * - `Array[Byte]` - Converts byte arrays into Async Streams
 * - `ByteBuffer` - Converts ByteBuffers into Async Streams
 * - `InputStream` - Converts InputStreams into Async Streams (Note: InputStream implementations are blocking)
 * - `OutputStream` - Converts OutputStreams into Async Streams (Note: OutputStream implementations are blocking)
 *
 *
 * @since 1.2
 */
object AsyncStreamHelper {

  /**
   * Converts a `Array[Byte]` into a [[AsyncInputStream]]
   *
   * @param srcBytes the data source
   * @return the AsyncInputStream
   */
  def toAsyncInputStream(srcBytes: Array[Byte]): AsyncInputStream = JAsyncStreamHelper.toAsyncInputStream(srcBytes)

  /**
   * Converts a `Array[Byte]` into a [[AsyncOutputStream]]
   *
   * @param dstBytes the data destination
   * @return the AsyncOutputStream
   */
  def toAsyncOutputStream(dstBytes: Array[Byte]): AsyncOutputStream = JAsyncStreamHelper.toAsyncOutputStream(dstBytes)

  /**
   * Converts a `ByteBuffer` into a [[AsyncInputStream]]
   *
   * @param srcByteBuffer the data source
   * @return the AsyncInputStream
   */
  def toAsyncInputStream(srcByteBuffer: ByteBuffer): AsyncInputStream =
    JAsyncStreamHelper.toAsyncInputStream(srcByteBuffer)

  /**
   * Converts a `ByteBuffer` into a [[AsyncOutputStream]]
   *
   * @param dstByteBuffer the data destination
   * @return the AsyncOutputStream
   */
  def toAsyncOutputStream(dstByteBuffer: ByteBuffer): AsyncOutputStream =
    JAsyncStreamHelper.toAsyncOutputStream(dstByteBuffer)

  /**
   * Converts a `InputStream` into a [[AsyncInputStream]]
   *
   * @param inputStream the InputStream
   * @return the AsyncInputStream
   */
  def toAsyncInputStream(inputStream: InputStream): AsyncInputStream =
    JAsyncStreamHelper.toAsyncInputStream(inputStream)

  /**
   * Converts a `OutputStream` into a [[AsyncOutputStream]]
   *
   * @param outputStream the OutputStream
   * @return the AsyncOutputStream
   */
  def toAsyncOutputStream(outputStream: OutputStream): AsyncOutputStream =
    JAsyncStreamHelper.toAsyncOutputStream(outputStream)

}
