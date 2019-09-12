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

import java.nio.ByteBuffer

import com.mongodb.reactivestreams.client.Success
import com.mongodb.reactivestreams.client.gridfs.{AsyncInputStream => JAsyncInputStream, AsyncOutputStream => JAsyncOutputStream}
import org.reactivestreams.Publisher

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

  implicit class JavaAsyncInputStreamToScala(wrapped: JAsyncInputStream) extends AsyncInputStream {

    override def close(): SingleObservable[Completed] = wrapped.close()

    override def read(dst: ByteBuffer): SingleObservable[Int] = wrapped.read(dst)

    override def skip(bytesToSkip: Long): SingleObservable[Long] = wrapped.skip(bytesToSkip)
  }

  implicit class JavaAsyncOutputStreamToScala(wrapped: JAsyncOutputStream) extends AsyncOutputStream {

    override def close(): SingleObservable[Completed] = wrapped.close()

    override def write(src: ByteBuffer): SingleObservable[Int] = wrapped.write(src)
  }

  implicit class ScalaAsyncInputStreamToJava(wrapped: AsyncInputStream) extends JAsyncInputStream {
    override def read(dst: ByteBuffer): Publisher[java.lang.Integer] = wrapped.read(dst).map(i => java.lang.Integer.valueOf(i))

    override def skip(bytesToSkip: Long): Publisher[java.lang.Long] = wrapped.skip(bytesToSkip).map(l => java.lang.Long.valueOf(l))

    override def close(): Publisher[Success] = wrapped.close().map(_ => Success.SUCCESS)
  }

  implicit class ScalaAsyncOutputStreamToJava(wrapped: AsyncOutputStream) extends JAsyncOutputStream {
    override def write(src: ByteBuffer): Publisher[java.lang.Integer] = wrapped.write(src).map(i => java.lang.Integer.valueOf(i))

    override def close(): Publisher[Success] = wrapped.close().map(_ => Success.SUCCESS)
  }
}
