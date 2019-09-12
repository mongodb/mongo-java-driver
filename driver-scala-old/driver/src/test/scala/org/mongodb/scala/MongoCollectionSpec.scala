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

import com.mongodb.reactivestreams.client.{MongoCollection => JMongoCollection}
import org.bson.BsonDocument
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.configuration.CodecRegistries.fromProviders
import org.mongodb.scala.model._
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class MongoCollectionSpec extends FlatSpec with Matchers with MockFactory {

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
    wrapped.expects(Symbol("getNamespace"))().once()

    mongoCollection.namespace
  }

  it should "return the underlying getCodecRegistry" in {
    wrapped.expects(Symbol("getCodecRegistry"))().once()

    mongoCollection.codecRegistry
  }

  it should "return the underlying getReadPreference" in {
    wrapped.expects(Symbol("getReadPreference"))().once()

    mongoCollection.readPreference
  }

  it should "return the underlying getWriteConcern" in {
    wrapped.expects(Symbol("getWriteConcern"))().once()

    mongoCollection.writeConcern
  }

  it should "return the underlying getReadConcern" in {
    wrapped.expects(Symbol("getReadConcern"))().once()

    mongoCollection.readConcern
  }

  it should "return the underlying getDocumentClass" in {
    wrapped.expects(Symbol("getDocumentClass"))().once()

    mongoCollection.documentClass
  }

  it should "return the underlying withCodecRegistry" in {
    val codecRegistry = fromProviders(new BsonValueCodecProvider())

    wrapped.expects(Symbol("withCodecRegistry"))(codecRegistry).once()

    mongoCollection.withCodecRegistry(codecRegistry)
  }

  it should "return the underlying withReadPreference" in {
    wrapped.expects(Symbol("withReadPreference"))(readPreference).once()

    mongoCollection.withReadPreference(readPreference)
  }

  it should "return the underlying withWriteConcern" in {
    val writeConcern = WriteConcern.MAJORITY
    wrapped.expects(Symbol("withWriteConcern"))(writeConcern).once()

    mongoCollection.withWriteConcern(writeConcern)
  }

  it should "return the underlying withReadConcern" in {
    val readConcern = ReadConcern.MAJORITY
    wrapped.expects(Symbol("withReadConcern"))(readConcern).once()

    mongoCollection.withReadConcern(readConcern)
  }

  it should "return the underlying withDocumentClass" in {
    wrapped.expects(Symbol("withDocumentClass"))(classOf[Document]).once()
    wrapped.expects(Symbol("withDocumentClass"))(classOf[Document]).once()
    wrapped.expects(Symbol("withDocumentClass"))(classOf[BsonDocument]).once()

    mongoCollection.withDocumentClass()
    mongoCollection.withDocumentClass[Document]()
    mongoCollection.withDocumentClass[BsonDocument]()
  }

  it should "return the underlying count" in {
    val countOptions = CountOptions().hintString("Hint")

    wrapped.expects(Symbol("count"))().once()
    wrapped.expects(Symbol("count"))(filter).once()
    wrapped.expects(Symbol("count"))(filter, countOptions).once()
    wrapped.expects(Symbol("count"))(clientSession).once()
    wrapped.expects(Symbol("count"))(clientSession, filter).once()
    wrapped.expects(Symbol("count"))(clientSession, filter, countOptions).once()

    mongoCollection.count()
    mongoCollection.count(filter)
    mongoCollection.count(filter, countOptions)
    mongoCollection.count(clientSession)
    mongoCollection.count(clientSession, filter)
    mongoCollection.count(clientSession, filter, countOptions)
  }

  it should "return the underlying countDocuments" in {
    val countOptions = CountOptions().hintString("Hint")

    wrapped.expects(Symbol("countDocuments"))().once()
    wrapped.expects(Symbol("countDocuments"))(filter).once()
    wrapped.expects(Symbol("countDocuments"))(filter, countOptions).once()
    wrapped.expects(Symbol("countDocuments"))(clientSession).once()
    wrapped.expects(Symbol("countDocuments"))(clientSession, filter).once()
    wrapped.expects(Symbol("countDocuments"))(clientSession, filter, countOptions).once()

    mongoCollection.countDocuments()
    mongoCollection.countDocuments(filter)
    mongoCollection.countDocuments(filter, countOptions)
    mongoCollection.countDocuments(clientSession)
    mongoCollection.countDocuments(clientSession, filter)
    mongoCollection.countDocuments(clientSession, filter, countOptions)
  }

  it should "return the underlying estimatedDocumentCount" in {
    val options = EstimatedDocumentCountOptions().maxTime(1, TimeUnit.SECONDS)

    wrapped.expects(Symbol("estimatedDocumentCount"))().once()
    wrapped.expects(Symbol("estimatedDocumentCount"))(options).once()

    mongoCollection.estimatedDocumentCount()
    mongoCollection.estimatedDocumentCount(options)
  }

  it should "wrap the underlying DistinctObservable correctly" in {
    wrapped.expects(Symbol("distinct"))("fieldName", classOf[String]).once()
    wrapped.expects(Symbol("distinct"))("fieldName", filter, classOf[String]).once()
    wrapped.expects(Symbol("distinct"))(clientSession, "fieldName", classOf[String]).once()
    wrapped.expects(Symbol("distinct"))(clientSession, "fieldName", filter, classOf[String]).once()

    mongoCollection.distinct[String]("fieldName")
    mongoCollection.distinct[String]("fieldName", filter)
    mongoCollection.distinct[String](clientSession, "fieldName")
    mongoCollection.distinct[String](clientSession, "fieldName", filter)
  }

  it should "wrap the underlying FindObservable correctly" in {
    wrapped.expects(Symbol("find"))(classOf[Document]).once()
    wrapped.expects(Symbol("find"))(classOf[BsonDocument]).once()
    wrapped.expects(Symbol("find"))(filter, classOf[Document]).once()
    wrapped.expects(Symbol("find"))(filter, classOf[BsonDocument]).once()
    wrapped.expects(Symbol("find"))(clientSession, classOf[Document]).once()
    wrapped.expects(Symbol("find"))(clientSession, classOf[BsonDocument]).once()
    wrapped.expects(Symbol("find"))(clientSession, filter, classOf[Document]).once()
    wrapped.expects(Symbol("find"))(clientSession, filter, classOf[BsonDocument]).once()

    mongoCollection.find() shouldBe a[FindObservable[_]]
    mongoCollection.find[BsonDocument]() shouldBe a[FindObservable[_]]
    mongoCollection.find(filter) shouldBe a[FindObservable[_]]
    mongoCollection.find[BsonDocument](filter) shouldBe a[FindObservable[_]]
    mongoCollection.find(clientSession) shouldBe a[FindObservable[_]]
    mongoCollection.find[BsonDocument](clientSession) shouldBe a[FindObservable[_]]
    mongoCollection.find(clientSession, filter) shouldBe a[FindObservable[_]]
    mongoCollection.find[BsonDocument](clientSession, filter) shouldBe a[FindObservable[_]]
  }

  it should "wrap the underlying AggregateObservable correctly" in {
    val pipeline = List(Document("$match" -> 1))

    wrapped.expects(Symbol("aggregate"))(pipeline.asJava, classOf[Document]).once()
    wrapped.expects(Symbol("aggregate"))(pipeline.asJava, classOf[BsonDocument]).once()
    wrapped.expects(Symbol("aggregate"))(clientSession, pipeline.asJava, classOf[Document]).once()
    wrapped.expects(Symbol("aggregate"))(clientSession, pipeline.asJava, classOf[BsonDocument]).once()

    mongoCollection.aggregate(pipeline) shouldBe a[AggregateObservable[_]]
    mongoCollection.aggregate[BsonDocument](pipeline) shouldBe a[AggregateObservable[_]]
    mongoCollection.aggregate(clientSession, pipeline) shouldBe a[AggregateObservable[_]]
    mongoCollection.aggregate[BsonDocument](clientSession, pipeline) shouldBe a[AggregateObservable[_]]
  }

  it should "wrap the underlying MapReduceObservable correctly" in {
    wrapped.expects(Symbol("mapReduce"))("map", "reduce", classOf[Document]).once()
    wrapped.expects(Symbol("mapReduce"))("map", "reduce", classOf[BsonDocument]).once()
    wrapped.expects(Symbol("mapReduce"))(clientSession, "map", "reduce", classOf[Document]).once()
    wrapped.expects(Symbol("mapReduce"))(clientSession, "map", "reduce", classOf[BsonDocument]).once()

    mongoCollection.mapReduce("map", "reduce") shouldBe a[MapReduceObservable[_]]
    mongoCollection.mapReduce[BsonDocument]("map", "reduce") shouldBe a[MapReduceObservable[_]]
    mongoCollection.mapReduce(clientSession, "map", "reduce") shouldBe a[MapReduceObservable[_]]
    mongoCollection.mapReduce[BsonDocument](clientSession, "map", "reduce") shouldBe a[MapReduceObservable[_]]
  }

  it should "wrap the underlying bulkWrite correctly" in {
    val bulkRequests = List(
      InsertOneModel(Document("a" -> 1)),
      DeleteOneModel(filter),
      UpdateOneModel(filter, Document("$set" -> Document("b" -> 1)))
    )
    val bulkWriteOptions = new BulkWriteOptions().ordered(true)

    wrapped.expects(Symbol("bulkWrite"))(bulkRequests.asJava).once()
    wrapped.expects(Symbol("bulkWrite"))(bulkRequests.asJava, bulkWriteOptions).once()
    wrapped.expects(Symbol("bulkWrite"))(clientSession, bulkRequests.asJava).once()
    wrapped.expects(Symbol("bulkWrite"))(clientSession, bulkRequests.asJava, bulkWriteOptions).once()

    mongoCollection.bulkWrite(bulkRequests)
    mongoCollection.bulkWrite(bulkRequests, bulkWriteOptions)
    mongoCollection.bulkWrite(clientSession, bulkRequests)
    mongoCollection.bulkWrite(clientSession, bulkRequests, bulkWriteOptions)
  }

  it should "wrap the underlying insertOne correctly" in {
    val insertDoc = Document("a" -> 1)
    val insertOptions = InsertOneOptions().bypassDocumentValidation(true)
    wrapped.expects(Symbol("insertOne"))(insertDoc).once()
    wrapped.expects(Symbol("insertOne"))(insertDoc, insertOptions).once()
    wrapped.expects(Symbol("insertOne"))(clientSession, insertDoc).once()
    wrapped.expects(Symbol("insertOne"))(clientSession, insertDoc, insertOptions).once()

    mongoCollection.insertOne(insertDoc)
    mongoCollection.insertOne(insertDoc, insertOptions)
    mongoCollection.insertOne(clientSession, insertDoc)
    mongoCollection.insertOne(clientSession, insertDoc, insertOptions)
  }

  it should "wrap the underlying insertMany correctly" in {
    val insertDocs = List(Document("a" -> 1))
    val insertOptions = new InsertManyOptions().ordered(false)

    wrapped.expects(Symbol("insertMany"))(insertDocs.asJava).once()
    wrapped.expects(Symbol("insertMany"))(insertDocs.asJava, insertOptions).once()
    wrapped.expects(Symbol("insertMany"))(clientSession, insertDocs.asJava).once()
    wrapped.expects(Symbol("insertMany"))(clientSession, insertDocs.asJava, insertOptions).once()

    mongoCollection.insertMany(insertDocs)
    mongoCollection.insertMany(insertDocs, insertOptions)
    mongoCollection.insertMany(clientSession, insertDocs)
    mongoCollection.insertMany(clientSession, insertDocs, insertOptions)
  }

  it should "wrap the underlying deleteOne correctly" in {
    val options = new DeleteOptions().collation(collation)
    wrapped.expects(Symbol("deleteOne"))(filter).once()
    wrapped.expects(Symbol("deleteOne"))(filter, options).once()
    wrapped.expects(Symbol("deleteOne"))(clientSession, filter).once()
    wrapped.expects(Symbol("deleteOne"))(clientSession, filter, options).once()

    mongoCollection.deleteOne(filter)
    mongoCollection.deleteOne(filter, options)
    mongoCollection.deleteOne(clientSession, filter)
    mongoCollection.deleteOne(clientSession, filter, options)
  }

  it should "wrap the underlying deleteMany correctly" in {
    val options = new DeleteOptions().collation(collation)
    wrapped.expects(Symbol("deleteMany"))(filter).once()
    wrapped.expects(Symbol("deleteMany"))(filter, options).once()
    wrapped.expects(Symbol("deleteMany"))(clientSession, filter).once()
    wrapped.expects(Symbol("deleteMany"))(clientSession, filter, options).once()

    mongoCollection.deleteMany(filter)
    mongoCollection.deleteMany(filter, options)
    mongoCollection.deleteMany(clientSession, filter)
    mongoCollection.deleteMany(clientSession, filter, options)
  }

  it should "wrap the underlying replaceOne correctly" in {
    val replacement = Document("a" -> 1)
    val replaceOptions = new ReplaceOptions().upsert(true)
    val updateOptions = new UpdateOptions().upsert(true)

    wrapped.expects(Symbol("replaceOne"))(filter, replacement).once()
    wrapped.expects(Symbol("replaceOne"))(filter, replacement, replaceOptions).once()
    wrapped.expects(Symbol("replaceOne"))(clientSession, filter, replacement).once()
    wrapped.expects(Symbol("replaceOne"))(clientSession, filter, replacement, replaceOptions).once()

    wrapped.expects(Symbol("replaceOne"))(filter, replacement, updateOptions).once()
    wrapped.expects(Symbol("replaceOne"))(clientSession, filter, replacement, updateOptions).once()

    mongoCollection.replaceOne(filter, replacement)
    mongoCollection.replaceOne(filter, replacement, replaceOptions)
    mongoCollection.replaceOne(clientSession, filter, replacement)
    mongoCollection.replaceOne(clientSession, filter, replacement, replaceOptions)

    mongoCollection.replaceOne(filter, replacement, updateOptions)
    mongoCollection.replaceOne(clientSession, filter, replacement, updateOptions)
  }

  it should "wrap the underlying updateOne correctly" in {
    val update = Document("$set" -> Document("a" -> 2))
    val pipeline = Seq(update)
    val updateOptions = new UpdateOptions().upsert(true)

    wrapped.expects(Symbol("updateOne"))(filter, update).once()
    wrapped.expects(Symbol("updateOne"))(filter, update, updateOptions).once()
    wrapped.expects(Symbol("updateOne"))(clientSession, filter, update).once()
    wrapped.expects(Symbol("updateOne"))(clientSession, filter, update, updateOptions).once()

    wrapped.expects(Symbol("updateOne"))(filter, pipeline.asJava).once()
    wrapped.expects(Symbol("updateOne"))(filter, pipeline.asJava, updateOptions).once()
    wrapped.expects(Symbol("updateOne"))(clientSession, filter, pipeline.asJava).once()
    wrapped.expects(Symbol("updateOne"))(clientSession, filter, pipeline.asJava, updateOptions).once()

    mongoCollection.updateOne(filter, update)
    mongoCollection.updateOne(filter, update, updateOptions)
    mongoCollection.updateOne(clientSession, filter, update)
    mongoCollection.updateOne(clientSession, filter, update, updateOptions)

    mongoCollection.updateOne(filter, pipeline)
    mongoCollection.updateOne(filter, pipeline, updateOptions)
    mongoCollection.updateOne(clientSession, filter, pipeline)
    mongoCollection.updateOne(clientSession, filter, pipeline, updateOptions)
  }

  it should "wrap the underlying updateMany correctly" in {
    val update = Document("$set" -> Document("a" -> 2))
    val pipeline = Seq(update)
    val updateOptions = new UpdateOptions().upsert(true)

    wrapped.expects(Symbol("updateMany"))(filter, update).once()
    wrapped.expects(Symbol("updateMany"))(filter, update, updateOptions).once()
    wrapped.expects(Symbol("updateMany"))(clientSession, filter, update).once()
    wrapped.expects(Symbol("updateMany"))(clientSession, filter, update, updateOptions).once()

    wrapped.expects(Symbol("updateMany"))(filter, pipeline.asJava).once()
    wrapped.expects(Symbol("updateMany"))(filter, pipeline.asJava, updateOptions).once()
    wrapped.expects(Symbol("updateMany"))(clientSession, filter, pipeline.asJava).once()
    wrapped.expects(Symbol("updateMany"))(clientSession, filter, pipeline.asJava, updateOptions).once()

    mongoCollection.updateMany(filter, update)
    mongoCollection.updateMany(filter, update, updateOptions)
    mongoCollection.updateMany(clientSession, filter, update)
    mongoCollection.updateMany(clientSession, filter, update, updateOptions)

    mongoCollection.updateMany(filter, pipeline)
    mongoCollection.updateMany(filter, pipeline, updateOptions)
    mongoCollection.updateMany(clientSession, filter, pipeline)
    mongoCollection.updateMany(clientSession, filter, pipeline, updateOptions)
  }

  it should "wrap the underlying findOneAndDelete correctly" in {
    val options = new FindOneAndDeleteOptions().sort(Document("sort" -> 1))

    wrapped.expects(Symbol("findOneAndDelete"))(filter).once()
    wrapped.expects(Symbol("findOneAndDelete"))(filter, options).once()
    wrapped.expects(Symbol("findOneAndDelete"))(clientSession, filter).once()
    wrapped.expects(Symbol("findOneAndDelete"))(clientSession, filter, options).once()

    mongoCollection.findOneAndDelete(filter)
    mongoCollection.findOneAndDelete(filter, options)
    mongoCollection.findOneAndDelete(clientSession, filter)
    mongoCollection.findOneAndDelete(clientSession, filter, options)
  }

  it should "wrap the underlying findOneAndReplace correctly" in {
    val replacement = Document("a" -> 2)
    val options = new FindOneAndReplaceOptions().sort(Document("sort" -> 1))

    wrapped.expects(Symbol("findOneAndReplace"))(filter, replacement).once()
    wrapped.expects(Symbol("findOneAndReplace"))(filter, replacement, options).once()
    wrapped.expects(Symbol("findOneAndReplace"))(clientSession, filter, replacement).once()
    wrapped.expects(Symbol("findOneAndReplace"))(clientSession, filter, replacement, options).once()

    mongoCollection.findOneAndReplace(filter, replacement)
    mongoCollection.findOneAndReplace(filter, replacement, options)
    mongoCollection.findOneAndReplace(clientSession, filter, replacement)
    mongoCollection.findOneAndReplace(clientSession, filter, replacement, options)
  }

  it should "wrap the underlying findOneAndUpdate correctly" in {
    val update = Document("a" -> 2)
    val pipeline = Seq(update)
    val options = new FindOneAndUpdateOptions().sort(Document("sort" -> 1))

    wrapped.expects(Symbol("findOneAndUpdate"))(filter, update).once()
    wrapped.expects(Symbol("findOneAndUpdate"))(filter, update, options).once()
    wrapped.expects(Symbol("findOneAndUpdate"))(clientSession, filter, update).once()
    wrapped.expects(Symbol("findOneAndUpdate"))(clientSession, filter, update, options).once()

    wrapped.expects(Symbol("findOneAndUpdate"))(filter, pipeline.asJava).once()
    wrapped.expects(Symbol("findOneAndUpdate"))(filter, pipeline.asJava, options).once()
    wrapped.expects(Symbol("findOneAndUpdate"))(clientSession, filter, pipeline.asJava).once()
    wrapped.expects(Symbol("findOneAndUpdate"))(clientSession, filter, pipeline.asJava, options).once()

    mongoCollection.findOneAndUpdate(filter, update)
    mongoCollection.findOneAndUpdate(filter, update, options)
    mongoCollection.findOneAndUpdate(clientSession, filter, update)
    mongoCollection.findOneAndUpdate(clientSession, filter, update, options)

    mongoCollection.findOneAndUpdate(filter, pipeline)
    mongoCollection.findOneAndUpdate(filter, pipeline, options)
    mongoCollection.findOneAndUpdate(clientSession, filter, pipeline)
    mongoCollection.findOneAndUpdate(clientSession, filter, pipeline, options)
  }

  it should "wrap the underlying drop correctly" in {
    wrapped.expects(Symbol("drop"))().once()
    wrapped.expects(Symbol("drop"))(clientSession).once()

    mongoCollection.drop()
    mongoCollection.drop(clientSession)
  }

  it should "wrap the underlying createIndex correctly" in {
    val index = Document("a" -> 1)
    val options = new IndexOptions().background(true)

    wrapped.expects(Symbol("createIndex"))(index).once()
    wrapped.expects(Symbol("createIndex"))(index, options).once()
    wrapped.expects(Symbol("createIndex"))(clientSession, index).once()
    wrapped.expects(Symbol("createIndex"))(clientSession, index, options).once()

    mongoCollection.createIndex(index)
    mongoCollection.createIndex(index, options)
    mongoCollection.createIndex(clientSession, index)
    mongoCollection.createIndex(clientSession, index, options)
  }

  it should "wrap the underlying createIndexes correctly" in {
    val indexes = new IndexModel(Document("a" -> 1))
    val options = new CreateIndexOptions()

    // https://github.com/paulbutcher/ScalaMock/issues/93
    wrapped.expects(Symbol("createIndexes"))(List(indexes).asJava).once()
    wrapped.expects(Symbol("createIndexes"))(List(indexes).asJava, options).once()
    wrapped.expects(Symbol("createIndexes"))(clientSession, List(indexes).asJava).once()
    wrapped.expects(Symbol("createIndexes"))(clientSession, List(indexes).asJava, options).once()

    mongoCollection.createIndexes(List(indexes))
    mongoCollection.createIndexes(List(indexes), options)
    mongoCollection.createIndexes(clientSession, List(indexes))
    mongoCollection.createIndexes(clientSession, List(indexes), options)
  }

  it should "wrap the underlying listIndexes correctly" in {
    wrapped.expects(Symbol("listIndexes"))(classOf[Document]).once()
    wrapped.expects(Symbol("listIndexes"))(classOf[BsonDocument]).once()
    wrapped.expects(Symbol("listIndexes"))(clientSession, classOf[Document]).once()
    wrapped.expects(Symbol("listIndexes"))(clientSession, classOf[BsonDocument]).once()

    mongoCollection.listIndexes()
    mongoCollection.listIndexes[BsonDocument]()
    mongoCollection.listIndexes(clientSession)
    mongoCollection.listIndexes[BsonDocument](clientSession)
  }

  it should "wrap the underlying dropIndex correctly" in {
    val indexDocument = Document("""{a: 1}""")
    val options = new DropIndexOptions()
    wrapped.expects(Symbol("dropIndex"))("indexName").once()
    wrapped.expects(Symbol("dropIndex"))(indexDocument).once()
    wrapped.expects(Symbol("dropIndex"))("indexName", options).once()
    wrapped.expects(Symbol("dropIndex"))(indexDocument, options).once()
    wrapped.expects(Symbol("dropIndex"))(clientSession, "indexName").once()
    wrapped.expects(Symbol("dropIndex"))(clientSession, indexDocument).once()
    wrapped.expects(Symbol("dropIndex"))(clientSession, "indexName", options).once()
    wrapped.expects(Symbol("dropIndex"))(clientSession, indexDocument, options).once()

    mongoCollection.dropIndex("indexName")
    mongoCollection.dropIndex(indexDocument)
    mongoCollection.dropIndex("indexName", options)
    mongoCollection.dropIndex(indexDocument, options)
    mongoCollection.dropIndex(clientSession, "indexName")
    mongoCollection.dropIndex(clientSession, indexDocument)
    mongoCollection.dropIndex(clientSession, "indexName", options)
    mongoCollection.dropIndex(clientSession, indexDocument, options)
  }

  it should "wrap the underlying dropIndexes correctly" in {

    val options = new DropIndexOptions()
    wrapped.expects(Symbol("dropIndexes"))().once()
    wrapped.expects(Symbol("dropIndexes"))(options).once()
    wrapped.expects(Symbol("dropIndexes"))(clientSession).once()
    wrapped.expects(Symbol("dropIndexes"))(clientSession, options).once()

    mongoCollection.dropIndexes()
    mongoCollection.dropIndexes(options)
    mongoCollection.dropIndexes(clientSession)
    mongoCollection.dropIndexes(clientSession, options)
  }

  it should "wrap the underlying renameCollection correctly" in {
    val newNamespace = new MongoNamespace("db", "coll")
    val options = new RenameCollectionOptions()

    wrapped.expects(Symbol("renameCollection"))(newNamespace).once()
    wrapped.expects(Symbol("renameCollection"))(newNamespace, options).once()
    wrapped.expects(Symbol("renameCollection"))(clientSession, newNamespace).once()
    wrapped.expects(Symbol("renameCollection"))(clientSession, newNamespace, options).once()

    mongoCollection.renameCollection(newNamespace)
    mongoCollection.renameCollection(newNamespace, options)
    mongoCollection.renameCollection(clientSession, newNamespace)
    mongoCollection.renameCollection(clientSession, newNamespace, options)
  }

  it should "wrap the underlying ChangeStreamPublisher correctly" in {
    val pipeline = List(Document("$match" -> 1))

    wrapped.expects(Symbol("watch"))(classOf[Document]).once()
    wrapped.expects(Symbol("watch"))(classOf[BsonDocument]).once()
    wrapped.expects(Symbol("watch"))(pipeline.asJava, classOf[Document]).once()
    wrapped.expects(Symbol("watch"))(pipeline.asJava, classOf[BsonDocument]).once()
    wrapped.expects(Symbol("watch"))(clientSession, classOf[Document]).once()
    wrapped.expects(Symbol("watch"))(clientSession, classOf[BsonDocument]).once()
    wrapped.expects(Symbol("watch"))(clientSession, pipeline.asJava, classOf[Document]).once()
    wrapped.expects(Symbol("watch"))(clientSession, pipeline.asJava, classOf[BsonDocument]).once()

    mongoCollection.watch() shouldBe a[ChangeStreamObservable[_]]
    mongoCollection.watch[BsonDocument]() shouldBe a[ChangeStreamObservable[_]]
    mongoCollection.watch(pipeline) shouldBe a[ChangeStreamObservable[_]]
    mongoCollection.watch[BsonDocument](pipeline) shouldBe a[ChangeStreamObservable[_]]
    mongoCollection.watch(clientSession) shouldBe a[ChangeStreamObservable[_]]
    mongoCollection.watch[BsonDocument](clientSession) shouldBe a[ChangeStreamObservable[_]]
    mongoCollection.watch(clientSession, pipeline) shouldBe a[ChangeStreamObservable[_]]
    mongoCollection.watch[BsonDocument](clientSession, pipeline) shouldBe a[ChangeStreamObservable[_]]
  }

}
