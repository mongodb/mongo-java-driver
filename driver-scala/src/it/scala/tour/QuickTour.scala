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

import java.util.concurrent.CountDownLatch

import com.mongodb.Block
import com.mongodb.connection.{ ClusterSettings, SslSettings }
import org.bson.UuidRepresentation
import org.bson.codecs.UuidCodec
import org.bson.codecs.configuration.CodecRegistries
import org.mongodb.scala._
import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.model.{
  Accumulators,
  Aggregates,
  BulkWriteOptions,
  CreateCollectionOptions,
  DeleteOneModel,
  Filters,
  InsertOneModel,
  Projections,
  ReplaceOneModel,
  UpdateOneModel
}
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Projections._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Updates.{ inc, set }
import org.mongodb.scala.model.changestream.ChangeStreamDocument
import tour.Helpers._

import scala.collection.immutable.IndexedSeq

/**
 * The QuickTour code example
 */
object QuickTour {
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

    // get a handle to the "test" collection
    val collection: MongoCollection[Document] = database.getCollection("test")

    collection.drop().results()

    // make a document and insert it
    val doc: Document = Document(
      "_id" -> 0,
      "name" -> "MongoDB",
      "type" -> "database",
      "count" -> 1,
      "info" -> Document("x" -> 203, "y" -> 102)
    )

    collection.insertOne(doc).results()

    // get it (since it's the only one in there since we dropped the rest earlier on)
    collection.find.first().printResults()

    // now, lets add lots of little documents to the collection so we can explore queries and cursors
    val documents: IndexedSeq[Document] = (1 to 100) map { i: Int =>
      Document("i" -> i)
    }
    val insertObservable = collection.insertMany(documents)

    val insertAndCount = for {
      insertResult <- insertObservable
      countResult <- collection.countDocuments()
    } yield countResult

    println(s"total # of documents after inserting 100 small ones (should be 101):  ${insertAndCount.headResult()}")

    collection.find().first().printHeadResult()

    // Query Filters
    // now use a query to get 1 document out
    collection.find(equal("i", 71)).first().printHeadResult()

    // now use a range query to get a larger subset
    collection.find(gt("i", 50)).printResults()

    // range query with multiple constraints
    collection.find(and(gt("i", 50), lte("i", 100))).printResults()

    // Sorting
    collection.find(exists("i")).sort(descending("i")).first().printHeadResult()

    // Projection
    collection.find().projection(excludeId()).first().printHeadResult()

    //Aggregation
    collection
      .aggregate(
        Seq(
          filter(gt("i", 0)),
          project(Document("""{ITimes10: {$multiply: ["$i", 10]}}"""))
        )
      )
      .printResults()

    // Update One
    collection.updateOne(equal("i", 10), set("i", 110)).printHeadResult("Update Result: ")

    // Update Many
    collection.updateMany(lt("i", 100), inc("i", 100)).printHeadResult("Update Result: ")

    // Delete One
    collection.deleteOne(equal("i", 110)).printHeadResult("Delete Result: ")

    // Delete Many
    collection.deleteMany(gte("i", 100)).printHeadResult("Delete Result: ")

    // Create Index
    collection.createIndex(Document("i" -> 1)).printHeadResult("Create Index Result: %s")

    // Clean up
    collection.drop().results()

    // release resources
    mongoClient.close()

    import scala.collection.JavaConverters._
    import org.mongodb.scala.bson._

    val codecRegistry =
      CodecRegistries.fromRegistries(
        CodecRegistries.fromCodecs(new UuidCodec(UuidRepresentation.STANDARD)),
        MongoClient.DEFAULT_CODEC_REGISTRY
      )
  }

}
