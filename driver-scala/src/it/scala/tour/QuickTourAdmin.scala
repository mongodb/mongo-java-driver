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

import org.mongodb.scala.bson.conversions.Bson

import org.mongodb.scala._
import org.mongodb.scala.model.{ TextSearchOptions, CreateCollectionOptions }
import org.mongodb.scala.model.Filters.text
import org.mongodb.scala.model.Projections.metaTextScore
import org.mongodb.scala.model.Sorts.ascending

import tour.Helpers._

/**
 * The QuickTourAdmin code example
 */
object QuickTourAdmin {
  //scalastyle:off method.length

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

    // get a handle to the "test" collection
    val collection: MongoCollection[Document] = database.getCollection("test")

    // getting a list of databases
    mongoClient.listDatabaseNames().printResults()

    // drop a database
    mongoClient.getDatabase("databaseToBeDropped").drop().headResult()

    // create a collection
    database.createCollection(
      "cappedCollection",
      CreateCollectionOptions().capped(true).sizeInBytes(0x100000)
    ).printHeadResult("Collection Created! ")

    database.listCollectionNames().printResults("Collection Names: ")

    // drop a collection:
    collection.drop().headResult()

    // create an ascending index on the "i" field
    collection.createIndex(ascending("i")).printResults("Created an index named: ")

    // list the indexes on the collection
    collection.listIndexes().printResults()

    // create a text index on the "content" field and insert sample documents
    val indexAndInsert = for {
      indexResults <- collection.createIndex(Document("content" -> "text"))
      insertResults <- collection.insertMany(List(
        Document("_id" -> 0, "content" -> "textual content"),
        Document("_id" -> 1, "content" -> "additional content"),
        Document("_id" -> 2, "content" -> "irrelevant content")
      ))
    } yield insertResults

    indexAndInsert.results()

    // Find using the text index
    collection.countDocuments(text("textual content -irrelevant")).printResults("Text search matches: ")

    // Find using the $language operator
    val textSearch: Bson = text("textual content -irrelevant", TextSearchOptions().language("english"))
    collection.countDocuments(textSearch).printResults("Text search matches (english): ")

    // Find the highest scoring match
    collection.find(textSearch)
      .projection(metaTextScore("score"))
      .first()
      .printHeadResult("Highest scoring document: ")

    // Run a command
    database.runCommand(Document("buildInfo" -> 1)).printHeadResult()

    // Clean up
    database.drop().results()

    // release resources
    mongoClient.close()
  }

}
