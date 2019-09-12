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

import org.mongodb.scala.model.{Filters, Updates}
import org.mongodb.scala.result.UpdateResult

import scala.concurrent.Await
import scala.concurrent.duration.Duration


//scalastyle:off magic.number regex
class DocumentationTransactionsExampleSpec extends RequiresMongoDBISpec {

  // Implicit functions that execute the Observable and return the results
  val waitDuration = Duration(5, "seconds")
  implicit class ObservableExecutor[T](observable: Observable[T]) {
    def execute(): Seq[T] = Await.result(observable.toFuture(), waitDuration)
  }

  implicit class SingleObservableExecutor[T](observable: SingleObservable[T]) {
    def execute(): T = Await.result(observable.toFuture(), waitDuration)
  }
  // end implicit functions

  "The Scala driver" should "be able to commit a transaction" in withClient { client =>
    assume(serverVersionAtLeast(List(4, 0, 0)) && !hasSingleHost())
    client.getDatabase("hr").drop().execute()
    client.getDatabase("hr").createCollection("employees").execute()
    client.getDatabase("hr").createCollection("events").execute()

    updateEmployeeInfoWithRetry(client).execute()
    client.getDatabase("hr").drop().execute()
  }

  def updateEmployeeInfo(database: MongoDatabase, observable: SingleObservable[ClientSession]): SingleObservable[ClientSession] = {
    observable.map(clientSession => {
      val employeesCollection = database.getCollection("employees")
      val eventsCollection = database.getCollection("events")

      val transactionOptions = TransactionOptions.builder()
        .readPreference(ReadPreference.primary())
        .readConcern(ReadConcern.SNAPSHOT)
        .writeConcern(WriteConcern.MAJORITY)
        .build()
      clientSession.startTransaction(transactionOptions)
      employeesCollection.updateOne(clientSession, Filters.eq("employee", 3), Updates.set("status", "Inactive"))
        .subscribe((res: UpdateResult) => println(res))
      eventsCollection.insertOne(clientSession, Document("employee" -> 3, "status" -> Document("new" -> "Inactive", "old" -> "Active")))
        .subscribe((res: Completed) => ())

      clientSession
    })
  }

  def commitAndRetry(observable: SingleObservable[Completed]): SingleObservable[Completed] = {
    observable.recoverWith({
      case e: MongoException if e.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL) => {
        println("UnknownTransactionCommitResult, retrying commit operation ...")
        commitAndRetry(observable)
      }
      case e: Exception => {
        println(s"Exception during commit ...: $e")
        throw e
      }
    })
  }

  def runTransactionAndRetry(observable: SingleObservable[Completed]): SingleObservable[Completed] = {
    observable.recoverWith({
      case e: MongoException if e.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL) => {
        println("TransientTransactionError, aborting transaction and retrying ...")
        runTransactionAndRetry(observable)
      }
    })
  }

  def updateEmployeeInfoWithRetry(client: MongoClient): SingleObservable[Completed] = {

    val database = client.getDatabase("hr")
    val updateEmployeeInfoObservable: SingleObservable[ClientSession] = updateEmployeeInfo(database, client.startSession())
    val commitTransactionObservable: SingleObservable[Completed] =
      updateEmployeeInfoObservable.flatMap(clientSession => clientSession.commitTransaction())
    val commitAndRetryObservable: SingleObservable[Completed] = commitAndRetry(commitTransactionObservable)

    runTransactionAndRetry(commitAndRetryObservable)
  }
}
