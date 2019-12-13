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
import java.nio.charset.StandardCharsets

import org.mongodb.scala._
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.gridfs._
import org.mongodb.scala.model.Filters
import tour.Helpers._

import scala.util.Success

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
    val observableToUploadFrom: Observable[ByteBuffer] = Observable(
      Seq(ByteBuffer.wrap("MongoDB Tutorial..".getBytes(StandardCharsets.UTF_8)))
    )

    // Create some custom options
    val options: GridFSUploadOptions =
      new GridFSUploadOptions().chunkSizeBytes(1024 * 1204).metadata(Document("type" -> "presentation"))

    val fileId: ObjectId =
      gridFSBucket.uploadFromObservable("mongodb-tutorial", observableToUploadFrom, options).headResult()

    /*
     * Find documents
     */
    println("File names:")
    gridFSBucket.find().results().foreach(file => println(s" - ${file.getFilename}"))

    /*
     * Find documents with a filter
     */
    gridFSBucket
      .find(Filters.equal("metadata.contentType", "image/png"))
      .results()
      .foreach(file => println(s" > ${file.getFilename}"))

    /*
     * Download to Observable
     */
    val downloadById = gridFSBucket.downloadToObservable(fileId).results()
    val downloadByIdSize = downloadById.map(_.limit()).sum
    System.out.println("downloaded file sized: " + downloadByIdSize)

    /*
     * Download to Observable by name
     */
    val downloadOptions: GridFSDownloadOptions = new GridFSDownloadOptions().revision(0)
    val downloadByName = gridFSBucket.downloadToObservable("mongodb-tutorial", downloadOptions).results()
    val downloadByNameSize = downloadByName.map(_.limit()).sum
    System.out.println("downloaded file sized: " + downloadByNameSize)

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
