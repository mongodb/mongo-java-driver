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

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream }
import java.nio.ByteBuffer

import org.mongodb.scala.{ FuturesSpec, RequiresMongoDBISpec }
import org.mongodb.scala.gridfs.helpers.AsyncStreamHelper._
import org.mongodb.scala.gridfs.{ AsyncInputStream, AsyncOutputStream, GridFSBucket }
import org.scalatest.Inspectors.forEvery

class AsyncStreamHelperSpec extends RequiresMongoDBISpec with FuturesSpec {

  val content = "Hello GridFS Round Trip".getBytes()

  behavior of "AsyncStreamHelper"

  trait SourceAndDestination[T, R] {
    val description: String
    val sourceData: T
    val destinationData: R
    def inputStream: AsyncInputStream
    def outputStream: AsyncOutputStream
    def roundTripped: Boolean
  }

  val arrayOfBytesData = new SourceAndDestination[Array[Byte], Array[Byte]] {
    override val description: String = "Array[Byte]"
    override val sourceData: Array[Byte] = content
    override val destinationData: Array[Byte] = new Array[Byte](content.length)
    override def inputStream: AsyncInputStream = toAsyncInputStream(sourceData)
    override def outputStream: AsyncOutputStream = toAsyncOutputStream(destinationData)
    override def roundTripped: Boolean = sourceData sameElements destinationData

  }

  val byteBufferData = new SourceAndDestination[ByteBuffer, ByteBuffer] {
    override val description: String = "ByteBuffer"
    override val sourceData: ByteBuffer = ByteBuffer.wrap(content)
    override val destinationData: ByteBuffer = ByteBuffer.allocate(content.length)
    override def inputStream: AsyncInputStream = toAsyncInputStream(sourceData)
    override def outputStream: AsyncOutputStream = toAsyncOutputStream(destinationData)
    override def roundTripped: Boolean = sourceData == destinationData
  }

  val inputOutputStreams = new SourceAndDestination[ByteArrayInputStream, ByteArrayOutputStream] {
    override val description: String = "InputStream and OutputStream"
    override val sourceData: ByteArrayInputStream = new ByteArrayInputStream(content)
    override val destinationData: ByteArrayOutputStream = new ByteArrayOutputStream(content.length)
    override def inputStream: AsyncInputStream = toAsyncInputStream(sourceData)
    override def outputStream: AsyncOutputStream = toAsyncOutputStream(destinationData)
    override def roundTripped: Boolean = destinationData.toByteArray sameElements content
  }
  val sourceAndDestination = Seq(arrayOfBytesData, byteBufferData, inputOutputStreams)

  forEvery(sourceAndDestination) { (data: SourceAndDestination[_, _]) =>
    it should s"be able to roundtrip ${data.description}" in withDatabase(databaseName) { database =>
      val gridFSBucket = GridFSBucket(database, "fs")
      val filesCollection = database.getCollection("fs.files")
      val chunksCollection = database.getCollection("fs.chunks")

      gridFSBucket.drop().futureValue

      info("Testing uploading data")
      val objectId = gridFSBucket.uploadFromStream("myfile", data.inputStream).head().futureValue

      filesCollection.countDocuments().head().futureValue should equal(1)
      chunksCollection.countDocuments().head().futureValue should equal(1)

      info("Testing downloading data")
      gridFSBucket.downloadToStream(objectId, data.outputStream).head().futureValue
      data.outputStream.close().head().futureValue

      data.roundTripped should be(true)
    }
  }
}
