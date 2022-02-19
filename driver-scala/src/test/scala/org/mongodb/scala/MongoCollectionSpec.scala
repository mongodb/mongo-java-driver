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

import com.mongodb.reactivestreams.client.{ MongoCollection => JMongoCollection }
import org.bson.BsonDocument
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.configuration.CodecRegistries.fromProviders
import org.mockito.Mockito.{ times, verify }
import org.mongodb.scala.model._
import org.scalatestplus.mockito.MockitoSugar

import scala.collection.JavaConverters._

class MongoCollectionSpec extends BaseSpec with MockitoSugar {

  val wrapped = mock[JMongoCollection[Document]]
  val clientSession = mock[ClientSession]
  val mongoCollection = MongoCollection[Document](wrapped)
  val readPreference = ReadPreference.secondary()
  val collation = Collation.builder().locale("en").build()

  val filter: Document = Document("filter" -> 1)

  "MongoCollection" should "have the same methods as the wrapped MongoCollection" in {
    val wrapped = classOf[JMongoCollection[Document]].getMethods.map(_.getName).toSet
    val local = classOf[MongoCollection[Document]].getMethods.map(_.getName).toSet

    wrapped.foreach((name: String) => {
      val cleanedName = name.stripPrefix("get")
      assert(local.contains(name) | local.contains(cleanedName.head.toLower + cleanedName.tail), s"Missing: $name")
    })
  }

  it should "return the underlying getNamespace" in {
    mongoCollection.namespace

    verify(wrapped).getNamespace
  }

  it should "return the underlying getCodecRegistry" in {
    mongoCollection.codecRegistry

    verify(wrapped).getCodecRegistry
  }

  it should "return the underlying getReadPreference" in {
    mongoCollection.readPreference

    verify(wrapped).getReadPreference
  }

  it should "return the underlying getWriteConcern" in {
    mongoCollection.writeConcern

    verify(wrapped).getWriteConcern
  }

  it should "return the underlying getReadConcern" in {
    mongoCollection.readConcern

    verify(wrapped).getReadConcern
  }

  it should "return the underlying getDocumentClass" in {
    mongoCollection.documentClass

    verify(wrapped).getDocumentClass
  }

  it should "return the underlying withCodecRegistry" in {
    val codecRegistry = fromProviders(new BsonValueCodecProvider())

    mongoCollection.withCodecRegistry(codecRegistry)

    verify(wrapped).withCodecRegistry(codecRegistry)
  }

  it should "return the underlying withReadPreference" in {
    mongoCollection.withReadPreference(readPreference)

    verify(wrapped).withReadPreference(readPreference)
  }

  it should "return the underlying withWriteConcern" in {
    val writeConcern = WriteConcern.MAJORITY
    mongoCollection.withWriteConcern(writeConcern)

    verify(wrapped).withWriteConcern(writeConcern)
  }

  it should "return the underlying withReadConcern" in {
    val readConcern = ReadConcern.MAJORITY
    mongoCollection.withReadConcern(readConcern)

    verify(wrapped).withReadConcern(readConcern)
  }

  it should "return the underlying withDocumentClass" in {
    mongoCollection.withDocumentClass()
    mongoCollection.withDocumentClass[Document]()
    mongoCollection.withDocumentClass[BsonDocument]()

    verify(wrapped, times(2)).withDocumentClass(classOf[Document])
    verify(wrapped).withDocumentClass(classOf[BsonDocument])

  }

  it should "return the underlying countDocuments" in {
    val countOptions = CountOptions()

    mongoCollection.countDocuments()
    mongoCollection.countDocuments(filter)
    mongoCollection.countDocuments(filter, countOptions)
    mongoCollection.countDocuments(clientSession)
    mongoCollection.countDocuments(clientSession, filter)
    mongoCollection.countDocuments(clientSession, filter, countOptions)

    verify(wrapped).countDocuments()
    verify(wrapped).countDocuments(filter)
    verify(wrapped).countDocuments(filter, countOptions)
    verify(wrapped).countDocuments(clientSession)
    verify(wrapped).countDocuments(clientSession, filter)
    verify(wrapped).countDocuments(clientSession, filter, countOptions)
  }

  it should "return the underlying estimatedDocumentCount" in {
    val options = EstimatedDocumentCountOptions().maxTime(1, TimeUnit.SECONDS)

    mongoCollection.estimatedDocumentCount()
    mongoCollection.estimatedDocumentCount(options)

    verify(wrapped).estimatedDocumentCount()
    verify(wrapped).estimatedDocumentCount(options)
  }

  it should "wrap the underlying DistinctObservable correctly" in {
    mongoCollection.distinct[String]("fieldName")
    mongoCollection.distinct[String]("fieldName", filter)
    mongoCollection.distinct[String](clientSession, "fieldName")
    mongoCollection.distinct[String](clientSession, "fieldName", filter)

    verify(wrapped).distinct("fieldName", classOf[String])
    verify(wrapped).distinct("fieldName", filter, classOf[String])
    verify(wrapped).distinct(clientSession, "fieldName", classOf[String])
    verify(wrapped).distinct(clientSession, "fieldName", filter, classOf[String])
  }

  it should "wrap the underlying FindObservable correctly" in {
    mongoCollection.find() shouldBe a[FindObservable[_]]
    mongoCollection.find[BsonDocument]() shouldBe a[FindObservable[_]]
    mongoCollection.find(filter) shouldBe a[FindObservable[_]]
    mongoCollection.find[BsonDocument](filter) shouldBe a[FindObservable[_]]
    mongoCollection.find(clientSession) shouldBe a[FindObservable[_]]
    mongoCollection.find[BsonDocument](clientSession) shouldBe a[FindObservable[_]]
    mongoCollection.find(clientSession, filter) shouldBe a[FindObservable[_]]
    mongoCollection.find[BsonDocument](clientSession, filter) shouldBe a[FindObservable[_]]

    verify(wrapped).find(classOf[Document])
    verify(wrapped).find(classOf[BsonDocument])
    verify(wrapped).find(filter, classOf[Document])
    verify(wrapped).find(filter, classOf[BsonDocument])
    verify(wrapped).find(clientSession, classOf[Document])
    verify(wrapped).find(clientSession, classOf[BsonDocument])
    verify(wrapped).find(clientSession, filter, classOf[Document])
    verify(wrapped).find(clientSession, filter, classOf[BsonDocument])
  }

  it should "wrap the underlying AggregateObservable correctly" in {
    val pipeline = List(Document("$match" -> 1))

    mongoCollection.aggregate(pipeline) shouldBe a[AggregateObservable[_]]
    mongoCollection.aggregate[BsonDocument](pipeline) shouldBe a[AggregateObservable[_]]
    mongoCollection.aggregate(clientSession, pipeline) shouldBe a[AggregateObservable[_]]
    mongoCollection.aggregate[BsonDocument](clientSession, pipeline) shouldBe a[AggregateObservable[_]]

    verify(wrapped).aggregate(pipeline.asJava, classOf[Document])
    verify(wrapped).aggregate(pipeline.asJava, classOf[BsonDocument])
    verify(wrapped).aggregate(clientSession, pipeline.asJava, classOf[Document])
    verify(wrapped).aggregate(clientSession, pipeline.asJava, classOf[BsonDocument])
  }

  it should "wrap the underlying MapReduceObservable correctly" in {
    mongoCollection.mapReduce("map", "reduce") shouldBe a[MapReduceObservable[_]]
    mongoCollection.mapReduce[BsonDocument]("map", "reduce") shouldBe a[MapReduceObservable[_]]
    mongoCollection.mapReduce(clientSession, "map", "reduce") shouldBe a[MapReduceObservable[_]]
    mongoCollection.mapReduce[BsonDocument](clientSession, "map", "reduce") shouldBe a[MapReduceObservable[_]]

    verify(wrapped).mapReduce("map", "reduce", classOf[Document])
    verify(wrapped).mapReduce("map", "reduce", classOf[BsonDocument])
    verify(wrapped).mapReduce(clientSession, "map", "reduce", classOf[Document])
    verify(wrapped).mapReduce(clientSession, "map", "reduce", classOf[BsonDocument])
  }

  it should "wrap the underlying bulkWrite correctly" in {
    val bulkRequests = List(
      InsertOneModel(Document("a" -> 1)),
      DeleteOneModel(filter),
      UpdateOneModel(filter, Document("$set" -> Document("b" -> 1)))
    )
    val bulkWriteOptions = new BulkWriteOptions().ordered(true)

    mongoCollection.bulkWrite(bulkRequests)
    mongoCollection.bulkWrite(bulkRequests, bulkWriteOptions)
    mongoCollection.bulkWrite(clientSession, bulkRequests)
    mongoCollection.bulkWrite(clientSession, bulkRequests, bulkWriteOptions)

    verify(wrapped).bulkWrite(bulkRequests.asJava)
    verify(wrapped).bulkWrite(bulkRequests.asJava, bulkWriteOptions)
    verify(wrapped).bulkWrite(clientSession, bulkRequests.asJava)
    verify(wrapped).bulkWrite(clientSession, bulkRequests.asJava, bulkWriteOptions)
  }

  it should "wrap the underlying insertOne correctly" in {
    val insertDoc = Document("a" -> 1)
    val insertOptions = InsertOneOptions().bypassDocumentValidation(true)

    mongoCollection.insertOne(insertDoc)
    mongoCollection.insertOne(insertDoc, insertOptions)
    mongoCollection.insertOne(clientSession, insertDoc)
    mongoCollection.insertOne(clientSession, insertDoc, insertOptions)

    verify(wrapped).insertOne(insertDoc)
    verify(wrapped).insertOne(insertDoc, insertOptions)
    verify(wrapped).insertOne(clientSession, insertDoc)
    verify(wrapped).insertOne(clientSession, insertDoc, insertOptions)
  }

  it should "wrap the underlying insertMany correctly" in {
    val insertDocs = List(Document("a" -> 1))
    val insertOptions = new InsertManyOptions().ordered(false)

    mongoCollection.insertMany(insertDocs)
    mongoCollection.insertMany(insertDocs, insertOptions)
    mongoCollection.insertMany(clientSession, insertDocs)
    mongoCollection.insertMany(clientSession, insertDocs, insertOptions)

    verify(wrapped).insertMany(insertDocs.asJava)
    verify(wrapped).insertMany(insertDocs.asJava, insertOptions)
    verify(wrapped).insertMany(clientSession, insertDocs.asJava)
    verify(wrapped).insertMany(clientSession, insertDocs.asJava, insertOptions)
  }

  it should "wrap the underlying deleteOne correctly" in {
    val options = new DeleteOptions().collation(collation)

    mongoCollection.deleteOne(filter)
    mongoCollection.deleteOne(filter, options)
    mongoCollection.deleteOne(clientSession, filter)
    mongoCollection.deleteOne(clientSession, filter, options)

    verify(wrapped).deleteOne(filter)
    verify(wrapped).deleteOne(filter, options)
    verify(wrapped).deleteOne(clientSession, filter)
    verify(wrapped).deleteOne(clientSession, filter, options)
  }

  it should "wrap the underlying deleteMany correctly" in {
    val options = new DeleteOptions().collation(collation)
    mongoCollection.deleteMany(filter)
    mongoCollection.deleteMany(filter, options)
    mongoCollection.deleteMany(clientSession, filter)
    mongoCollection.deleteMany(clientSession, filter, options)

    verify(wrapped).deleteMany(filter)
    verify(wrapped).deleteMany(filter, options)
    verify(wrapped).deleteMany(clientSession, filter)
    verify(wrapped).deleteMany(clientSession, filter, options)
  }

  it should "wrap the underlying replaceOne correctly" in {
    val replacement = Document("a" -> 1)
    val replaceOptions = new ReplaceOptions().upsert(true)

    mongoCollection.replaceOne(filter, replacement)
    mongoCollection.replaceOne(filter, replacement, replaceOptions)
    mongoCollection.replaceOne(clientSession, filter, replacement)
    mongoCollection.replaceOne(clientSession, filter, replacement, replaceOptions)

    verify(wrapped).replaceOne(filter, replacement)
    verify(wrapped).replaceOne(filter, replacement, replaceOptions)
    verify(wrapped).replaceOne(clientSession, filter, replacement)
    verify(wrapped).replaceOne(clientSession, filter, replacement, replaceOptions)
  }

  it should "wrap the underlying updateOne correctly" in {
    val update = Document("$set" -> Document("a" -> 2))
    val pipeline = Seq(update)
    val updateOptions = new UpdateOptions().upsert(true)

    mongoCollection.updateOne(filter, update)
    mongoCollection.updateOne(filter, update, updateOptions)
    mongoCollection.updateOne(clientSession, filter, update)
    mongoCollection.updateOne(clientSession, filter, update, updateOptions)

    mongoCollection.updateOne(filter, pipeline)
    mongoCollection.updateOne(filter, pipeline, updateOptions)
    mongoCollection.updateOne(clientSession, filter, pipeline)
    mongoCollection.updateOne(clientSession, filter, pipeline, updateOptions)

    verify(wrapped).updateOne(filter, update)
    verify(wrapped).updateOne(filter, update, updateOptions)
    verify(wrapped).updateOne(clientSession, filter, update)
    verify(wrapped).updateOne(clientSession, filter, update, updateOptions)

    verify(wrapped).updateOne(filter, pipeline.asJava)
    verify(wrapped).updateOne(filter, pipeline.asJava, updateOptions)
    verify(wrapped).updateOne(clientSession, filter, pipeline.asJava)
    verify(wrapped).updateOne(clientSession, filter, pipeline.asJava, updateOptions)
  }

  it should "wrap the underlying updateMany correctly" in {
    val update = Document("$set" -> Document("a" -> 2))
    val pipeline = Seq(update)
    val updateOptions = new UpdateOptions().upsert(true)

    mongoCollection.updateMany(filter, update)
    mongoCollection.updateMany(filter, update, updateOptions)
    mongoCollection.updateMany(clientSession, filter, update)
    mongoCollection.updateMany(clientSession, filter, update, updateOptions)

    mongoCollection.updateMany(filter, pipeline)
    mongoCollection.updateMany(filter, pipeline, updateOptions)
    mongoCollection.updateMany(clientSession, filter, pipeline)
    mongoCollection.updateMany(clientSession, filter, pipeline, updateOptions)

    verify(wrapped).updateMany(filter, update)
    verify(wrapped).updateMany(filter, update, updateOptions)
    verify(wrapped).updateMany(clientSession, filter, update)
    verify(wrapped).updateMany(clientSession, filter, update, updateOptions)

    verify(wrapped).updateMany(filter, pipeline.asJava)
    verify(wrapped).updateMany(filter, pipeline.asJava, updateOptions)
    verify(wrapped).updateMany(clientSession, filter, pipeline.asJava)
    verify(wrapped).updateMany(clientSession, filter, pipeline.asJava, updateOptions)
  }

  it should "wrap the underlying findOneAndDelete correctly" in {
    val options = new FindOneAndDeleteOptions().sort(Document("sort" -> 1))

    mongoCollection.findOneAndDelete(filter)
    mongoCollection.findOneAndDelete(filter, options)
    mongoCollection.findOneAndDelete(clientSession, filter)
    mongoCollection.findOneAndDelete(clientSession, filter, options)

    verify(wrapped).findOneAndDelete(filter)
    verify(wrapped).findOneAndDelete(filter, options)
    verify(wrapped).findOneAndDelete(clientSession, filter)
    verify(wrapped).findOneAndDelete(clientSession, filter, options)
  }

  it should "wrap the underlying findOneAndReplace correctly" in {
    val replacement = Document("a" -> 2)
    val options = new FindOneAndReplaceOptions().sort(Document("sort" -> 1))

    mongoCollection.findOneAndReplace(filter, replacement)
    mongoCollection.findOneAndReplace(filter, replacement, options)
    mongoCollection.findOneAndReplace(clientSession, filter, replacement)
    mongoCollection.findOneAndReplace(clientSession, filter, replacement, options)

    verify(wrapped).findOneAndReplace(filter, replacement)
    verify(wrapped).findOneAndReplace(filter, replacement, options)
    verify(wrapped).findOneAndReplace(clientSession, filter, replacement)
    verify(wrapped).findOneAndReplace(clientSession, filter, replacement, options)
  }

  it should "wrap the underlying findOneAndUpdate correctly" in {
    val update = Document("a" -> 2)
    val pipeline = Seq(update)
    val options = new FindOneAndUpdateOptions().sort(Document("sort" -> 1))

    mongoCollection.findOneAndUpdate(filter, update)
    mongoCollection.findOneAndUpdate(filter, update, options)
    mongoCollection.findOneAndUpdate(clientSession, filter, update)
    mongoCollection.findOneAndUpdate(clientSession, filter, update, options)

    mongoCollection.findOneAndUpdate(filter, pipeline)
    mongoCollection.findOneAndUpdate(filter, pipeline, options)
    mongoCollection.findOneAndUpdate(clientSession, filter, pipeline)
    mongoCollection.findOneAndUpdate(clientSession, filter, pipeline, options)

    verify(wrapped).findOneAndUpdate(filter, update)
    verify(wrapped).findOneAndUpdate(filter, update, options)
    verify(wrapped).findOneAndUpdate(clientSession, filter, update)
    verify(wrapped).findOneAndUpdate(clientSession, filter, update, options)

    verify(wrapped).findOneAndUpdate(filter, pipeline.asJava)
    verify(wrapped).findOneAndUpdate(filter, pipeline.asJava, options)
    verify(wrapped).findOneAndUpdate(clientSession, filter, pipeline.asJava)
    verify(wrapped).findOneAndUpdate(clientSession, filter, pipeline.asJava, options)
  }

  it should "wrap the underlying drop correctly" in {
    mongoCollection.drop()
    mongoCollection.drop(clientSession)

    verify(wrapped).drop()
    verify(wrapped).drop(clientSession)
  }

  it should "wrap the underlying createIndex correctly" in {
    val index = Document("a" -> 1)
    val options = new IndexOptions().background(true)

    mongoCollection.createIndex(index)
    mongoCollection.createIndex(index, options)
    mongoCollection.createIndex(clientSession, index)
    mongoCollection.createIndex(clientSession, index, options)

    verify(wrapped).createIndex(index)
    verify(wrapped).createIndex(index, options)
    verify(wrapped).createIndex(clientSession, index)
    verify(wrapped).createIndex(clientSession, index, options)
  }

  it should "wrap the underlying createIndexes correctly" in {
    val indexes = new IndexModel(Document("a" -> 1))
    val options = new CreateIndexOptions()

    mongoCollection.createIndexes(List(indexes))
    mongoCollection.createIndexes(List(indexes), options)
    mongoCollection.createIndexes(clientSession, List(indexes))
    mongoCollection.createIndexes(clientSession, List(indexes), options)

    verify(wrapped).createIndexes(List(indexes).asJava)
    verify(wrapped).createIndexes(List(indexes).asJava, options)
    verify(wrapped).createIndexes(clientSession, List(indexes).asJava)
    verify(wrapped).createIndexes(clientSession, List(indexes).asJava, options)
  }

  it should "wrap the underlying listIndexes correctly" in {
    mongoCollection.listIndexes()
    mongoCollection.listIndexes[BsonDocument]()
    mongoCollection.listIndexes(clientSession)
    mongoCollection.listIndexes[BsonDocument](clientSession)

    verify(wrapped).listIndexes(classOf[Document])
    verify(wrapped).listIndexes(classOf[BsonDocument])
    verify(wrapped).listIndexes(clientSession, classOf[Document])
    verify(wrapped).listIndexes(clientSession, classOf[BsonDocument])
  }

  it should "wrap the underlying dropIndex correctly" in {
    val indexDocument = Document("""{a: 1}""")
    val options = new DropIndexOptions()

    mongoCollection.dropIndex("indexName")
    mongoCollection.dropIndex(indexDocument)
    mongoCollection.dropIndex("indexName", options)
    mongoCollection.dropIndex(indexDocument, options)
    mongoCollection.dropIndex(clientSession, "indexName")
    mongoCollection.dropIndex(clientSession, indexDocument)
    mongoCollection.dropIndex(clientSession, "indexName", options)
    mongoCollection.dropIndex(clientSession, indexDocument, options)

    verify(wrapped).dropIndex("indexName")
    verify(wrapped).dropIndex(indexDocument)
    verify(wrapped).dropIndex("indexName", options)
    verify(wrapped).dropIndex(indexDocument, options)
    verify(wrapped).dropIndex(clientSession, "indexName")
    verify(wrapped).dropIndex(clientSession, indexDocument)
    verify(wrapped).dropIndex(clientSession, "indexName", options)
    verify(wrapped).dropIndex(clientSession, indexDocument, options)
  }

  it should "wrap the underlying dropIndexes correctly" in {
    val options = new DropIndexOptions()

    mongoCollection.dropIndexes()
    mongoCollection.dropIndexes(options)
    mongoCollection.dropIndexes(clientSession)
    mongoCollection.dropIndexes(clientSession, options)

    verify(wrapped).dropIndexes()
    verify(wrapped).dropIndexes(options)
    verify(wrapped).dropIndexes(clientSession)
    verify(wrapped).dropIndexes(clientSession, options)
  }

  it should "wrap the underlying renameCollection correctly" in {
    val newNamespace = new MongoNamespace("db", "coll")
    val options = new RenameCollectionOptions()

    mongoCollection.renameCollection(newNamespace)
    mongoCollection.renameCollection(newNamespace, options)
    mongoCollection.renameCollection(clientSession, newNamespace)
    mongoCollection.renameCollection(clientSession, newNamespace, options)

    verify(wrapped).renameCollection(newNamespace)
    verify(wrapped).renameCollection(newNamespace, options)
    verify(wrapped).renameCollection(clientSession, newNamespace)
    verify(wrapped).renameCollection(clientSession, newNamespace, options)
  }

  it should "wrap the underlying ChangeStreamPublisher correctly" in {
    val pipeline = List(Document("$match" -> 1))

    mongoCollection.watch() shouldBe a[ChangeStreamObservable[_]]
    mongoCollection.watch[BsonDocument]() shouldBe a[ChangeStreamObservable[_]]
    mongoCollection.watch(pipeline) shouldBe a[ChangeStreamObservable[_]]
    mongoCollection.watch[BsonDocument](pipeline) shouldBe a[ChangeStreamObservable[_]]
    mongoCollection.watch(clientSession) shouldBe a[ChangeStreamObservable[_]]
    mongoCollection.watch[BsonDocument](clientSession) shouldBe a[ChangeStreamObservable[_]]
    mongoCollection.watch(clientSession, pipeline) shouldBe a[ChangeStreamObservable[_]]
    mongoCollection.watch[BsonDocument](clientSession, pipeline) shouldBe a[ChangeStreamObservable[_]]

    verify(wrapped).watch(classOf[Document])
    verify(wrapped).watch(classOf[BsonDocument])
    verify(wrapped).watch(pipeline.asJava, classOf[Document])
    verify(wrapped).watch(pipeline.asJava, classOf[BsonDocument])
    verify(wrapped).watch(clientSession, classOf[Document])
    verify(wrapped).watch(clientSession, classOf[BsonDocument])
    verify(wrapped).watch(clientSession, pipeline.asJava, classOf[Document])
    verify(wrapped).watch(clientSession, pipeline.asJava, classOf[BsonDocument])
  }

}
