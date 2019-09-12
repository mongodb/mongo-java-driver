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

package reactivestreams

import scala.collection.immutable.IndexedSeq
import scala.concurrent.duration.Duration

import org.mongodb.scala.{ Document, _ }

object ReactiveStreamsExample {
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

    // Now an reactive streams subscriber!
    println("Dropping the test collection")
    val dropSubscriber = TestSubscriber[Completed]()
    collection.drop().subscribe(dropSubscriber)
    dropSubscriber.assertNoTerminalEvent()
    dropSubscriber.requestMore(1)
    dropSubscriber.awaitTerminalEvent(Duration(10, "s"))
    dropSubscriber.assertNoErrors()
    dropSubscriber.assertReceivedOnNext(Seq())

    // Insert some documents
    println("Inserting documents")
    val insertSubscriber = TestSubscriber[Completed]()
    val documents: IndexedSeq[Document] = (1 to 100) map { i: Int => Document("_id" -> i) }
    collection.insertMany(documents).subscribe(insertSubscriber)
    insertSubscriber.requestMore(1)
    insertSubscriber.awaitTerminalEvent(Duration(10, "s"))
    insertSubscriber.assertNoErrors()
    insertSubscriber.assertReceivedOnNext(Seq())

    println("Finding documents")
    val findSubscriber = TestSubscriber[Document]()
    collection.find().subscribe(findSubscriber)
    findSubscriber.assertNoTerminalEvent()
    findSubscriber.requestMore(101)
    findSubscriber.awaitTerminalEvent(Duration(10, "s"))
    findSubscriber.assertNoErrors()
    findSubscriber.assertReceivedOnNext(documents)
  }
}
