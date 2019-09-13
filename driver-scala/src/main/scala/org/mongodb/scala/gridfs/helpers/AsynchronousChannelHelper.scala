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

import java.nio.channels.{ AsynchronousByteChannel, AsynchronousFileChannel }

import com.mongodb.internal.async.client.gridfs.helpers.{ AsynchronousChannelHelper => JAsynchronousChannelHelper }
import com.mongodb.reactivestreams.client.internal.GridFSAsyncStreamHelper
import org.mongodb.scala.gridfs.{ AsyncInputStream, AsyncOutputStream }

/**
 * A helper class to convert to AsynchronousByteChannel or AsynchronousFileChannel instances into
 * [[org.mongodb.scala.gridfs.AsyncInputStream]] or [[org.mongodb.scala.gridfs.AsyncOutputStream]] instances.
 *
 * @note Requires Java 7 or greater.
 * @since 1.2
 */
object AsynchronousChannelHelper {

  /**
   * Converts a AsynchronousByteChannel into a AsyncInputStream
   *
   * @param asynchronousByteChannel the AsynchronousByteChannel
   * @return the AsyncInputStream
   */
  def channelToInputStream(asynchronousByteChannel: AsynchronousByteChannel): AsyncInputStream =
    GridFSAsyncStreamHelper.toAsyncInputStream(
      JAsynchronousChannelHelper.channelToInputStream(asynchronousByteChannel)
    );

  /**
   * Converts a AsynchronousFileChannel into a AsyncInputStream
   *
   * @param asynchronousFileChannel the AsynchronousFileChannel
   * @return the AsyncInputStream
   */
  def channelToInputStream(asynchronousFileChannel: AsynchronousFileChannel): AsyncInputStream =
    GridFSAsyncStreamHelper.toAsyncInputStream(
      JAsynchronousChannelHelper.channelToInputStream(asynchronousFileChannel)
    );

  /**
   * Converts a AsynchronousByteChannel into a AsyncOutputStream
   *
   * @param asynchronousByteChannel the AsynchronousByteChannel
   * @return the AsyncOutputStream
   */
  def channelToOutputStream(asynchronousByteChannel: AsynchronousByteChannel): AsyncOutputStream =
    GridFSAsyncStreamHelper.toAsyncOutputStream(
      JAsynchronousChannelHelper.channelToOutputStream(asynchronousByteChannel)
    )

  /**
   * Converts a AsynchronousFileChannel into a AsyncOutputStream
   *
   * @param asynchronousFileChannel the AsynchronousFileChannel
   * @return the AsyncOutputStream
   */
  def channelToOutputStream(asynchronousFileChannel: AsynchronousFileChannel): AsyncOutputStream =
    GridFSAsyncStreamHelper.toAsyncOutputStream(
      JAsynchronousChannelHelper.channelToOutputStream(asynchronousFileChannel)
    );

}
