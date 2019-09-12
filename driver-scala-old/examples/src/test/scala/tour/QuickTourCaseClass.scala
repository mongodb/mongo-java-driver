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

import org.mongodb.scala._
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Updates._
import tour.Helpers._

/**
 * The QuickTour code example
 */
object QuickTourCaseClass {
  //scalastyle:off method.length

  /**
   * Run this main method to see the output of this quick example.
   *
   * @param args takes an optional single argument for the connection string
   * @throws Throwable if an operation fails
   */
  def main(args: Array[String]): Unit = {

    // Create the case class
    object Person {
      def apply(firstName: String, lastName: String): Person = Person(new ObjectId(), firstName, lastName);
    }
    case class Person(_id: ObjectId, firstName: String, lastName: String)

    // Create a codec for the Person case class
    import org.mongodb.scala.bson.codecs.Macros._
    import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
    import org.bson.codecs.configuration.CodecRegistries.{ fromRegistries, fromProviders }
    val codecRegistry = fromRegistries(fromProviders(classOf[Person]), DEFAULT_CODEC_REGISTRY)

    // Create the client
    val mongoClient: MongoClient = if (args.isEmpty) MongoClient() else MongoClient(args.head)

    // get handle to "mydb" database
    val database: MongoDatabase = mongoClient.getDatabase("mydb").withCodecRegistry(codecRegistry)

    // get a handle to the "test" collection
    val collection: MongoCollection[Person] = database.getCollection("test")

    collection.drop().results()

    // make a document and insert it
    val person: Person = Person("Ada", "Lovelace")

    collection.insertOne(person).results()

    // get it (since it's the only one in there since we dropped the rest earlier on)
    collection.find.first().printResults()

    // now, lets add lots of little documents to the collection so we can explore queries and cursors
    val people: Seq[Person] = Seq(
      Person("Charles", "Babbage"),
      Person("George", "Boole"),
      Person("Gertrude", "Blanch"),
      Person("Grace", "Hopper"),
      Person("Ida", "Rhodes"),
      Person("Jean", "Bartik"),
      Person("John", "Backus"),
      Person("Lucy", "Sanders"),
      Person("Tim", "Berners Lee"),
      Person("Zaphod", "Beeblebrox")
    )
    collection.insertMany(people).printResults()

    // Querying
    collection.find().first().printHeadResult()

    // Query Filters
    collection.find(equal("firstName", "Ida")).first().printHeadResult()

    // now use a range query to get a larger subset
    collection.find(regex("firstName", "^G")).sort(ascending("lastName")).printResults()

    // Update One
    collection.updateOne(equal("lastName", "Berners Lee"), set("lastName", "Berners-Lee")).printHeadResult("Update Result: ")

    // Delete One
    collection.deleteOne(equal("firstName", "Zaphod")).printHeadResult("Delete Result: ")

    // Clean up
    collection.drop().results()

    // release resources
    mongoClient.close()
  }

}
