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

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit.SECONDS

import com.mongodb.client.model.changestream.FullDocument
import org.mongodb.scala.model.{Aggregates, Filters, Updates}
import org.mongodb.scala.model.changestream.ChangeStreamDocument

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration

//scalastyle:off magic.number regex
class DocumentationChangeStreamExampleSpec extends RequiresMongoDBISpec with FuturesSpec {

  "The Scala driver" should "be able to use $changeStreams" in withDatabase { database: MongoDatabase =>
    assume(serverVersionAtLeast(List(3, 6, 0)) && !hasSingleHost())

    database.drop().execute()
    database.createCollection(collectionName).execute()
    val collection = database.getCollection(collectionName)

    /*
     * Example 1
     * Create a simple change stream against an existing collection.
     */
    println("1. Initial document from the Change Stream:")

    // Create the change stream observable.
    var observable: ChangeStreamObservable[Document] = collection.watch()

    // Create a observer
    var observer = new LatchedObserver[ChangeStreamDocument[Document]]()
    observable.subscribe(observer)

    // Insert a test document into the collection and request a result
    collection.insertOne(Document("{username: 'alice123', name: 'Alice'}")).execute()
    observer.waitForThenCancel()

    /*
     * Example 2
     * Create a change stream with 'lookup' option enabled.
     * The test document will be returned with a full version of the updated document.
     */
    println("2. Document from the Change Stream, with lookup enabled:")

    observable = collection.watch.fullDocument(FullDocument.UPDATE_LOOKUP)
    observer = new LatchedObserver[ChangeStreamDocument[Document]]()
    observable.subscribe(observer)

    // Update the test document.
    collection.updateOne(Document("{username: 'alice123'}"), Document("{$set : { email: 'alice@example.com'}}")).subscribeAndAwait()
    observer.waitForThenCancel()

    /*
     * Example 3
     * Create a change stream with 'lookup' option using a $match and ($redact or $project) stage.
     */
    println("3. Document from the Change Stream, with lookup enabled, matching `update` operations only: ")

    // Insert some dummy data.
    collection.insertMany(List(Document("{updateMe: 1}"), Document("{replaceMe: 1}"))).subscribeAndAwait()

    // Create $match pipeline stage.
    val pipeline = List(Aggregates.filter(Filters.or(Document("{'fullDocument.username': 'alice123'}"),
      Filters.in("operationType", "update", "replace", "delete"))))

    // Create the change stream cursor with $match.
    observable = collection.watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP)
    observer = new LatchedObserver[ChangeStreamDocument[Document]](false, 3)
    observable.subscribe(observer)

    // Update the test document.
    collection.updateOne(Filters.eq("updateMe", 1), Updates.set("updated", true)).subscribeAndAwait()
    // Replace the test document.
    collection.replaceOne(Filters.eq("replaceMe", 1), Document("{replaced: true}")).subscribeAndAwait()
    // Delete the test document.
    collection.deleteOne(Filters.eq("username", "alice123")).subscribeAndAwait()

    observer.waitForThenCancel()

    val results = observer.results()
    println(
      s"""
         |Update operationType: ${results.head.getUpdateDescription}
         |                      ${results.head}
   """.stripMargin.trim)
    println(s"Replace operationType: ${results(1)}")
    println(s"Delete operationType: ${results(2)}")


    /*
     * Example 4
     * Resume a change stream using a resume token.
     */
    println("4. Document from the Change Stream including a resume token:")

    // Get the resume token from the last document we saw in the previous change stream cursor.
    val resumeToken = results(2).getResumeToken
    println(resumeToken)

    // Pass the resume token to the resume after function to continue the change stream cursor.
    observable = collection.watch.resumeAfter(resumeToken)
    observer = new LatchedObserver[ChangeStreamDocument[Document]]
    observable.subscribe(observer)

    // Insert a test document.
    collection.insertOne(Document("{test: 'd'}")).subscribeAndAwait()

    // Block until the next result is printed
    observer.waitForThenCancel()

  }

  // Implicit functions that execute the Observable and return the results
  val waitDuration = Duration(120, "seconds")

  implicit class ObservableExecutor[T](observable: Observable[T]) {
    def execute(): Seq[T] = Await.result(observable, waitDuration)

    def subscribeAndAwait(): Unit = {
      val observer: LatchedObserver[T] = new LatchedObserver[T](false)
      observable.subscribe(observer)
      observer.await()
    }
  }

  implicit class SingleObservableExecutor[T](observable: SingleObservable[T]) {
    def execute(): T = Await.result(observable, waitDuration)
  }

  // end implicit functions

  private class LatchedObserver[T](val printResults: Boolean = true, val minimumNumberOfResults: Int = 1) extends Observer[T] {
    private val latch: CountDownLatch = new CountDownLatch(1)
    private val resultsBuffer: mutable.ListBuffer[T] = new mutable.ListBuffer[T]
    private var subscription: Option[Subscription] = None
    private var error: Option[Throwable] = None

    override def onSubscribe(s: Subscription): Unit = {
      subscription = Some(s)
      s.request(Integer.MAX_VALUE)
    }

    override def onNext(t: T): Unit = {
      resultsBuffer.append(t)
      if (printResults) println(t)
      if (resultsBuffer.size >= minimumNumberOfResults) latch.countDown()
    }

    override def onError(t: Throwable): Unit = {
      error = Some(t)
      println(t.getMessage)
      onComplete()
    }

    override def onComplete(): Unit = {
      latch.countDown()
    }

    def results(): List[T] = resultsBuffer.toList

    def await(): Unit = {
      if (!latch.await(120, SECONDS)) throw new MongoTimeoutException("observable timed out")
      if (error.isDefined) throw error.get
    }

    def waitForThenCancel(): Unit = {
      if (minimumNumberOfResults > resultsBuffer.size) await()
      subscription.foreach(_.unsubscribe())
    }
  }


}
