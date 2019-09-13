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

import java.util.concurrent.TimeUnit

import _root_.scala.concurrent.duration.Duration

import com.mongodb.{ MongoCredential => JMongoCredential }

import org.mongodb.scala
import org.mongodb.scala.bson.BsonString
import org.mongodb.scala.model._
import org.scalatest.{ FlatSpec, Matchers }

class ScalaPackageSpec extends BaseSpec {

  it should "be able to create Observable, Observers and Subscriptions" in {
    var success = false
    val observerable = new Observable[Int] {
      override def subscribe(observer: Observer[_ >: Int]): Unit = {
        val subscription = new Subscription {
          override def isUnsubscribed: Boolean = false

          override def request(l: Long): Unit = observer.onComplete()

          override def unsubscribe(): Unit = {}
        }

        observer.onSubscribe(subscription)
      }
    }
    val observer = new Observer[Int] {
      override def onError(throwable: Throwable): Unit = {}

      override def onSubscribe(subscription: Subscription): Unit = subscription.request(1)

      override def onComplete(): Unit = success = true

      override def onNext(tResult: Int): Unit = {}
    }

    observerable.subscribe(observer)

    success shouldBe true
  }

  it should "be able to create MongoClientSettings" in {
    val settings = scala.MongoClientSettings.builder().readPreference(ReadPreference.primary()).build()
    settings shouldBe a[com.mongodb.MongoClientSettings]
  }

  it should "be able to create Documents" in {
    val doc = Document("a" -> BsonString("1"))
    val doc2 = org.mongodb.scala.bson.collection.Document("a" -> BsonString("1"))

    doc shouldBe a[org.mongodb.scala.bson.collection.immutable.Document]
    doc should equal(doc2)
  }

  it should "be able to create BulkWriteOptions" in {
    val options = BulkWriteOptions()
    options shouldBe a[com.mongodb.client.model.BulkWriteOptions]
  }

  it should "be able to create MongoNamespace" in {
    val namespace = MongoNamespace("db.coll")
    namespace shouldBe a[com.mongodb.MongoNamespace]

    val namespace2 = MongoNamespace("db", "coll")
    namespace2 shouldBe a[com.mongodb.MongoNamespace]
  }

  it should "be able to create WriteConcern" in {
    WriteConcern.ACKNOWLEDGED should equal(com.mongodb.WriteConcern.ACKNOWLEDGED)

    WriteConcern.W1 should equal(new com.mongodb.WriteConcern(1))

    WriteConcern.W2 should equal(new com.mongodb.WriteConcern(2))

    WriteConcern.W3 should equal(new com.mongodb.WriteConcern(3))

    WriteConcern.UNACKNOWLEDGED should equal(com.mongodb.WriteConcern.UNACKNOWLEDGED)

    WriteConcern.JOURNALED should equal(com.mongodb.WriteConcern.JOURNALED)

    WriteConcern.MAJORITY should equal(com.mongodb.WriteConcern.MAJORITY)

    WriteConcern(1) should equal(new com.mongodb.WriteConcern(1))

    WriteConcern("Majority") should equal(new com.mongodb.WriteConcern("Majority"))

    WriteConcern(1).withJournal(true) should equal(new com.mongodb.WriteConcern(1).withJournal(true))

    WriteConcern("Majority").withWTimeout(Duration(10, TimeUnit.MILLISECONDS)) should equal(
      new com.mongodb.WriteConcern("Majority").withWTimeout(10, TimeUnit.MILLISECONDS)
    )

    WriteConcern(1).withWTimeout(Duration(10, TimeUnit.MILLISECONDS)) should equal(
      new com.mongodb.WriteConcern(1).withWTimeout(10, TimeUnit.MILLISECONDS)
    )
  }

  it should "create MongoCredential" in {

    val scalaCredential = MongoCredential.createCredential("userName", "database", "password".toCharArray)
    val javaCredential = JMongoCredential.createCredential("userName", "database", "password".toCharArray)
    scalaCredential should equal(javaCredential)

    val scalaCredential1 = MongoCredential.createScramSha1Credential("userName", "database", "password".toCharArray)
    val javaCredential1 = JMongoCredential.createScramSha1Credential("userName", "database", "password".toCharArray)
    scalaCredential1 should equal(javaCredential1)

    val scalaCredential2 = MongoCredential.createMongoX509Credential("userName")
    val javaCredential2 = JMongoCredential.createMongoX509Credential("userName")
    scalaCredential2 should equal(javaCredential2)

    val scalaCredential3 = MongoCredential.createMongoX509Credential()
    val javaCredential3 = JMongoCredential.createMongoX509Credential()
    scalaCredential3 should equal(javaCredential3)

    val scalaCredential4 = MongoCredential.createPlainCredential("userName", "database", "password".toCharArray)
    val javaCredential4 = JMongoCredential.createPlainCredential("userName", "database", "password".toCharArray)
    scalaCredential4 should equal(javaCredential4)

    val scalaCredential5 = MongoCredential.createGSSAPICredential("userName")
    val javaCredential5 = JMongoCredential.createGSSAPICredential("userName")
    scalaCredential5 should equal(javaCredential5)
  }
}
