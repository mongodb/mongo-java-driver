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

import scala.collection.JavaConverters._

import org.bson.BsonDocument
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.configuration.CodecRegistries.fromProviders
import com.mongodb.reactivestreams.client.{ ListCollectionsPublisher, MongoDatabase => JMongoDatabase }
import org.mockito.Mockito.{ verify, when }
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model._
import org.scalatestplus.mockito.MockitoSugar

class MongoDatabaseSpec extends BaseSpec with MockitoSugar {

  val wrapped = mock[JMongoDatabase]
  val clientSession = mock[ClientSession]
  val mongoDatabase = MongoDatabase(wrapped)
  val command = Document()
  val readPreference: ReadPreference = ReadPreference.secondary()

  "MongoDatabase" should "have the same methods as the wrapped MongoDatabase" in {
    val wrapped = classOf[JMongoDatabase].getMethods.map(_.getName)

    val local = classOf[MongoDatabase].getMethods.map(_.getName)

    wrapped.foreach((name: String) => {
      val cleanedName = name.stripPrefix("get")
      assert(local.contains(name) | local.contains(cleanedName.head.toLower + cleanedName.tail), s"Missing: $name")
    })
  }

  it should "return the underlying getCollection[T]" in {
    mongoDatabase.getCollection("collectionName")
    mongoDatabase.getCollection[BsonDocument]("collectionName")

    verify(wrapped).getCollection("collectionName", classOf[Document])
    verify(wrapped).getCollection("collectionName", classOf[BsonDocument])
  }

  it should "return the underlying getName" in {
    mongoDatabase.name

    verify(wrapped).getName
  }

  it should "return the underlying getCodecRegistry" in {
    mongoDatabase.codecRegistry

    verify(wrapped).getCodecRegistry
  }

  it should "return the underlying getReadPreference" in {
    mongoDatabase.readPreference

    verify(wrapped).getReadPreference
  }

  it should "return the underlying getWriteConcern" in {
    mongoDatabase.writeConcern

    verify(wrapped).getWriteConcern
  }

  it should "return the underlying getReadConcern" in {
    mongoDatabase.readConcern

    verify(wrapped).getReadConcern
  }

  it should "return the underlying withCodecRegistry" in {
    val codecRegistry = fromProviders(new BsonValueCodecProvider())

    mongoDatabase.withCodecRegistry(codecRegistry)

    verify(wrapped).withCodecRegistry(codecRegistry)
  }

  it should "return the underlying withReadPreference" in {
    mongoDatabase.withReadPreference(readPreference)

    verify(wrapped).withReadPreference(readPreference)
  }

  it should "return the underlying withWriteConcern" in {
    val writeConcern = WriteConcern.MAJORITY
    mongoDatabase.withWriteConcern(writeConcern)

    verify(wrapped).withWriteConcern(writeConcern)
  }

  it should "return the underlying withReadConcern" in {
    val readConcern = ReadConcern.MAJORITY
    mongoDatabase.withReadConcern(readConcern)

    verify(wrapped).withReadConcern(readConcern)
  }

  it should "call the underlying runCommand[T] when writing" in {
    mongoDatabase.runCommand(command)
    mongoDatabase.runCommand[BsonDocument](command)
    mongoDatabase.runCommand(clientSession, command)
    mongoDatabase.runCommand[BsonDocument](clientSession, command)

    verify(wrapped).runCommand(command, classOf[Document])
    verify(wrapped).runCommand(command, classOf[BsonDocument])
    verify(wrapped).runCommand(clientSession, command, classOf[Document])
    verify(wrapped).runCommand(clientSession, command, classOf[BsonDocument])
  }

  it should "call the underlying runCommand[T] when reading" in {
    mongoDatabase.runCommand(command, readPreference)
    mongoDatabase.runCommand[BsonDocument](command, readPreference)
    mongoDatabase.runCommand(clientSession, command, readPreference)
    mongoDatabase.runCommand[BsonDocument](clientSession, command, readPreference)

    verify(wrapped).runCommand(command, readPreference, classOf[Document])
    verify(wrapped).runCommand(command, readPreference, classOf[BsonDocument])
    verify(wrapped).runCommand(clientSession, command, readPreference, classOf[Document])
    verify(wrapped).runCommand(clientSession, command, readPreference, classOf[BsonDocument])
  }

  it should "call the underlying drop()" in {
    mongoDatabase.drop()
    mongoDatabase.drop(clientSession)

    verify(wrapped).drop()
    verify(wrapped).drop(clientSession)
  }

  it should "call the underlying listCollectionNames()" in {
    mongoDatabase.listCollectionNames()
    mongoDatabase.listCollectionNames(clientSession)

    verify(wrapped).listCollectionNames()
    verify(wrapped).listCollectionNames(clientSession)
  }

  it should "call the underlying listCollections()" in {
    when(wrapped.listCollections()).thenReturn(mock[ListCollectionsPublisher[org.bson.Document]])
    when(wrapped.listCollections(classOf[BsonDocument])).thenReturn(mock[ListCollectionsPublisher[BsonDocument]])
    when(wrapped.listCollections(clientSession)).thenReturn(mock[ListCollectionsPublisher[org.bson.Document]])
    when(wrapped.listCollections(clientSession, classOf[BsonDocument]))
      .thenReturn(mock[ListCollectionsPublisher[BsonDocument]])

    mongoDatabase.listCollections()
    mongoDatabase.listCollections[BsonDocument]()
    mongoDatabase.listCollections(clientSession)
    mongoDatabase.listCollections[BsonDocument](clientSession)
  }

  it should "call the underlying createCollection()" in {
    val options = CreateCollectionOptions()
      .capped(true)
      .validationOptions(
        ValidationOptions()
          .validator(Document("""{level: {$gte: 10}}"""))
          .validationLevel(ValidationLevel.MODERATE)
          .validationAction(ValidationAction.WARN)
      )
      .indexOptionDefaults(IndexOptionDefaults().storageEngine(Document("""{storageEngine: { mmapv1: {}}}""")))
      .storageEngineOptions(Document("""{ wiredTiger: {}}"""))

    mongoDatabase.createCollection("collectionName")
    mongoDatabase.createCollection("collectionName", options)
    mongoDatabase.createCollection(clientSession, "collectionName")
    mongoDatabase.createCollection(clientSession, "collectionName", options)

    verify(wrapped).createCollection("collectionName")
    verify(wrapped).createCollection("collectionName", options)
    verify(wrapped).createCollection(clientSession, "collectionName")
    verify(wrapped).createCollection(clientSession, "collectionName", options)
  }

  it should "call the underlying createView()" in {
    val options = CreateViewOptions().collation(Collation.builder().locale("en").build())
    val pipeline = List.empty[Bson]

    mongoDatabase.createView("viewName", "collectionName", pipeline)
    mongoDatabase.createView("viewName", "collectionName", pipeline, options)
    mongoDatabase.createView(clientSession, "viewName", "collectionName", pipeline)
    mongoDatabase.createView(clientSession, "viewName", "collectionName", pipeline, options)

    verify(wrapped).createView("viewName", "collectionName", pipeline.asJava)
    verify(wrapped).createView("viewName", "collectionName", pipeline.asJava, options)
    verify(wrapped).createView(clientSession, "viewName", "collectionName", pipeline.asJava)
    verify(wrapped).createView(clientSession, "viewName", "collectionName", pipeline.asJava, options)
  }

  it should "call the underlying watch" in {
    val pipeline = List(Document("$match" -> 1))

    mongoDatabase.watch() shouldBe a[ChangeStreamObservable[_]]
    mongoDatabase.watch(pipeline) shouldBe a[ChangeStreamObservable[_]]
    mongoDatabase.watch[BsonDocument](pipeline) shouldBe a[ChangeStreamObservable[_]]
    mongoDatabase.watch(clientSession, pipeline) shouldBe a[ChangeStreamObservable[_]]
    mongoDatabase.watch[BsonDocument](clientSession, pipeline) shouldBe a[ChangeStreamObservable[_]]

    verify(wrapped).watch(classOf[Document])
    verify(wrapped).watch(pipeline.asJava, classOf[Document])
    verify(wrapped).watch(pipeline.asJava, classOf[BsonDocument])
    verify(wrapped).watch(clientSession, pipeline.asJava, classOf[Document])
    verify(wrapped).watch(clientSession, pipeline.asJava, classOf[BsonDocument])
  }

  it should "call the underlying aggregate" in {
    val pipeline = List(Document("$match" -> 1))

    mongoDatabase.aggregate(pipeline) shouldBe a[AggregateObservable[_]]
    mongoDatabase.aggregate[BsonDocument](pipeline) shouldBe a[AggregateObservable[_]]
    mongoDatabase.aggregate(clientSession, pipeline) shouldBe a[AggregateObservable[_]]
    mongoDatabase.aggregate[BsonDocument](clientSession, pipeline) shouldBe a[AggregateObservable[_]]

    verify(wrapped).aggregate(pipeline.asJava, classOf[Document])
    verify(wrapped).aggregate(pipeline.asJava, classOf[BsonDocument])
    verify(wrapped).aggregate(clientSession, pipeline.asJava, classOf[Document])
    verify(wrapped).aggregate(clientSession, pipeline.asJava, classOf[BsonDocument])
  }
}
