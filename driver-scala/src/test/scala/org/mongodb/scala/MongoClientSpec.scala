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

import com.mongodb.reactivestreams.client.{MongoClient => JMongoClient}
import org.bson.BsonDocument
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class MongoClientSpec extends FlatSpec with Matchers with MockFactory {

  val wrapped = mock[JMongoClient]
  val clientSession = mock[ClientSession]
  val mongoClient = new MongoClient(wrapped)

  "MongoClient" should "have the same methods as the wrapped MongoClient" in {
    val wrapped = classOf[JMongoClient].getMethods.map(_.getName).toSet -- Seq("getSettings")
    val local = classOf[MongoClient].getMethods.map(_.getName)

    wrapped.foreach((name: String) => {
      val cleanedName = name.stripPrefix("get")
      assert(local.contains(name) | local.contains(cleanedName.head.toLower + cleanedName.tail), s"Missing: $name")
    })
  }

  it should "accept MongoDriverInformation" in {
    val driverInformation = MongoDriverInformation.builder().driverName("test").driverVersion("1.2.0").build()
    MongoClient("mongodb://localhost", Some(driverInformation))
  }

  it should "call the underlying getDatabase" in {
    wrapped.expects(Symbol("getDatabase"))("dbName").once()

    mongoClient.getDatabase("dbName")
  }

  it should "call the underlying close" in {
    wrapped.expects(Symbol("close"))().once()

    mongoClient.close()
  }

  it should "call the underlying startSession" in {
    val clientSessionOptions = ClientSessionOptions.builder.build()
    wrapped.expects(Symbol("startSession"))(clientSessionOptions).once()

    mongoClient.startSession(clientSessionOptions)
  }

  it should "call the underlying listDatabases[T]" in {
    wrapped.expects(Symbol("listDatabases"))(classOf[Document]).once()
    wrapped.expects(Symbol("listDatabases"))(clientSession, classOf[Document]).once()
    wrapped.expects(Symbol("listDatabases"))(classOf[BsonDocument]).once()
    wrapped.expects(Symbol("listDatabases"))(clientSession, classOf[BsonDocument]).once()

    mongoClient.listDatabases()
    mongoClient.listDatabases(clientSession)
    mongoClient.listDatabases[BsonDocument]()
    mongoClient.listDatabases[BsonDocument](clientSession)
  }

  it should "call the underlying listDatabaseNames" in {
    wrapped.expects(Symbol("listDatabaseNames"))().once()
    wrapped.expects(Symbol("listDatabaseNames"))(clientSession).once()

    mongoClient.listDatabaseNames()
    mongoClient.listDatabaseNames(clientSession)
  }

  it should "call the underlying watch" in {
    val pipeline = List(Document("$match" -> 1))

    wrapped.expects(Symbol("watch"))(classOf[Document]).once()
    wrapped.expects(Symbol("watch"))(pipeline.asJava, classOf[Document]).once()
    wrapped.expects(Symbol("watch"))(pipeline.asJava, classOf[BsonDocument]).once()
    wrapped.expects(Symbol("watch"))(clientSession, pipeline.asJava, classOf[Document]).once()
    wrapped.expects(Symbol("watch"))(clientSession, pipeline.asJava, classOf[BsonDocument]).once()

    mongoClient.watch() shouldBe a[ChangeStreamObservable[_]]
    mongoClient.watch(pipeline) shouldBe a[ChangeStreamObservable[_]]
    mongoClient.watch[BsonDocument](pipeline) shouldBe a[ChangeStreamObservable[_]]
    mongoClient.watch(clientSession, pipeline) shouldBe a[ChangeStreamObservable[_]]
    mongoClient.watch[BsonDocument](clientSession, pipeline) shouldBe a[ChangeStreamObservable[_]]
  }

}
