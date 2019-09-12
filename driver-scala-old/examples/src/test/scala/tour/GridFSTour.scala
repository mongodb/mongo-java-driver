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

package tour

import java.nio.ByteBuffer
import java.nio.channels.AsynchronousFileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.{ Path, Paths, StandardOpenOption }

import scala.util.Success

import org.bson.types.ObjectId

import org.mongodb.scala.model.Filters
import org.mongodb.scala.gridfs._
import org.mongodb.scala.gridfs.helpers.AsynchronousChannelHelper.channelToOutputStream
import org.mongodb.scala.gridfs.helpers.AsyncStreamHelper.toAsyncInputStream
import org.mongodb.scala._
import tour.Helpers._

/**
 * The GridFSTour code example
 */
object GridFSTour {

  //scalastyle:off
  /**
   * Run this main method to see the output of this quick example.
   *
   * @param args takes an optional single argument for the connection string
   * @throws Throwable if an operation fails
   */
  def main(args: Array[String]): Unit = {
    val mongoClient: MongoClient = if (args.isEmpty) MongoClient() else MongoClient(args.head)

    // get handle to "mydb" database
    val database: MongoDatabase = mongoClient.getDatabase("mydb")

    database.drop().results()

    val gridFSBucket = GridFSBucket(database)

    /*
    * UploadFromStream Example
    */
    // Get the input stream
    val streamToUploadFrom: AsyncInputStream = toAsyncInputStream("MongoDB Tutorial..".getBytes(StandardCharsets.UTF_8))

    // Create some custom options
    val options: GridFSUploadOptions = new GridFSUploadOptions().chunkSizeBytes(1024 * 1204).metadata(Document("type" -> "presentation"))

    val fileId: ObjectId = gridFSBucket.uploadFromStream("mongodb-tutorial", streamToUploadFrom, options).headResult()
    streamToUploadFrom.close().headResult()

    /*
     * OpenUploadStream Example
     */
    // Get some data to write
    val data: ByteBuffer = ByteBuffer.wrap("Data to upload into GridFS".getBytes(StandardCharsets.UTF_8))

    val uploadStream: GridFSUploadStream = gridFSBucket.openUploadStream("sampleData")
    uploadStream.write(data).headResult()
    uploadStream.close().headResult()

    /*
     * Find documents
     */
    println("File names:")
    gridFSBucket.find().results().foreach(file => println(s" - ${file.getFilename}"))

    /*
     * Find documents with a filter
     */
    gridFSBucket.find(Filters.equal("metadata.contentType", "image/png")).results().foreach(file => println(s" > ${file.getFilename}"))

    /*
     * DownloadToStream
     */
    val outputPath: Path = Paths.get("/tmp/mongodb-tutorial.txt")
    var streamToDownloadTo: AsynchronousFileChannel = AsynchronousFileChannel.open(outputPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.DELETE_ON_CLOSE)
    gridFSBucket.downloadToStream(fileId, channelToOutputStream(streamToDownloadTo)).headResult()
    streamToDownloadTo.close()

    /*
     * DownloadToStream by name
     */
    streamToDownloadTo = AsynchronousFileChannel.open(outputPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.DELETE_ON_CLOSE)
    val downloadOptions: GridFSDownloadOptions = new GridFSDownloadOptions().revision(0)
    gridFSBucket.downloadToStream("mongodb-tutorial", channelToOutputStream(streamToDownloadTo), downloadOptions).headResult()
    streamToDownloadTo.close()

    /*
     * OpenDownloadStream
     */
    val dstByteBuffer: ByteBuffer = ByteBuffer.allocate(1024 * 1024)
    val downloadStream: GridFSDownloadStream = gridFSBucket.openDownloadStream(fileId)
    downloadStream.read(dstByteBuffer).map(result => {
      dstByteBuffer.flip
      val bytes: Array[Byte] = new Array[Byte](result)
      dstByteBuffer.get(bytes)
      println(new String(bytes, StandardCharsets.UTF_8))
    }).headResult()

    /*
     * OpenDownloadStream by name
     */
    println("By name")
    dstByteBuffer.clear
    val downloadStreamByName: GridFSDownloadStream = gridFSBucket.openDownloadStream("sampleData")
    downloadStreamByName.read(dstByteBuffer).map(result => {
      dstByteBuffer.flip
      val bytes: Array[Byte] = new Array[Byte](result)
      dstByteBuffer.get(bytes)
      println(new String(bytes, StandardCharsets.UTF_8))
    }).headResult()

    /*
     * Rename
     */
    gridFSBucket.rename(fileId, "mongodbTutorial").andThen({ case Success(r) => println("renamed") }).results()
    println("renamed")

    /*
     * Delete
     */
    gridFSBucket.delete(fileId).results()
    println("deleted")

    // Final cleanup
    database.drop().results()
    println("Finished")
  }

  //scalastyle:on

}
