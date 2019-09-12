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

import java.io.File

import com.mongodb.bulk.BulkWriteError
import org.mongodb.scala.bson._
import org.mongodb.scala.model._
import org.mongodb.scala.result.UpdateResult
import org.scalatest.Inspectors.forEvery
import org.scalatest.exceptions.TestFailedException

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import scala.util.{Failure, Success, Try}

// scalastyle:off
class CrudSpec extends RequiresMongoDBISpec with FuturesSpec {
  lazy val dbTests = new File(getClass.getResource("/crud/db").toURI).listFiles
  lazy val readTests = new File(getClass.getResource("/crud/read").toURI).listFiles
  lazy val writeTests = new File(getClass.getResource("/crud/write").toURI).listFiles
  lazy val files = (dbTests ++ readTests ++ writeTests).filter(_.getName.endsWith(".json"))

  var database: Option[MongoDatabase] = None
  var collection: Option[MongoCollection[BsonDocument]] = None

  forEvery (files) { (file: File) =>
    s"Running ${file.getName} tests" should "pass all scenarios" in withClient {
      client =>

        val definition = BsonDocument(Source.fromFile(file).getLines.mkString)
        database = Some(client.getDatabase(definition.getString("database_name", BsonString(databaseName)).getValue))
        collection = Some(database.get.getCollection(collectionName))
        val data = definition.getArray("data").asScala.map(_.asDocument()).toSeq
        val tests = definition.getArray("tests").asScala.map(_.asDocument()).toSeq

        if (serverAtLeastMinVersion(definition) && serverLessThanMaxVersion(definition)) forEvery(tests) { (test: BsonDocument) =>
          val description = test.getString("description").getValue
          val operation: BsonDocument = test.getDocument("operation", BsonDocument())
          val expectedOutcome: BsonDocument = test.getDocument("outcome", BsonDocument())
          info(description)

          prepCollection(data)
          val outcome: BsonValue = runOperation(operation)
          val actualResult: BsonValue = if (outcome.isDocument && outcome.asDocument().containsKey("result")) {
            outcome.asDocument().get("result")
          } else {
            outcome
          }
          val expectedResult: BsonValue = expectedOutcome.get("result")

          // Hack to workaround the lack of insertedIds
          if (expectedResult.isDocument && !expectedResult.asDocument.containsKey("insertedIds")) {
            actualResult.asDocument.remove("insertedIds")
          }

          actualResult should equal(expectedResult)

          if (expectedOutcome.containsKey("collection")) {
            val collectionData = expectedOutcome.getDocument("collection")
            val expectedDocuments = collectionData.getArray("data").asScala.map(_.asDocument()).toSeq
            var coll = collection.get
            if (collectionData.containsKey("name")) {
              coll = database.get.getCollection[BsonDocument](collectionData.getString("name").getValue)
            }
            expectedDocuments should contain theSameElementsInOrderAs coll.find[BsonDocument]().futureValue
          }
        } else {
          info(s"Skipped $file: Server version check failed")
        }
    }
  }

  def collectionValues(database: MongoDatabase, outcome: BsonDocument): (Seq[BsonDocument], Seq[BsonDocument]) = {
    val collectionData = outcome.getDocument("collection")
    val expectedDocuments = collectionData.getArray("data").asScala.map(_.asDocument()).toSeq
    val coll = if (collectionData.containsKey("name")) {
      database.getCollection[BsonDocument](collectionData.getString("name").getValue)
    } else {
      collection.get
    }
    (expectedDocuments, coll.find[BsonDocument]().futureValue)
  }

  private def prepCollection(data: Seq[BsonDocument]): Unit = {
    collection.get.drop().futureValue
    if (!data.isEmpty) collection.get.insertMany(data).futureValue
  }

  // scalastyle:off cyclomatic.complexity
  private def runOperation(operation: BsonDocument): BsonValue = {
    val methodName = createMethodName(operation.getString("name").getValue, operation.getString("object", BsonString("")).getValue)
    val op = methodName match {
        case "doAggregate" => doAggregation _
        case "doDatabaseAggregate" => doDatabaseAggregate _
        case "doBulkWrite" => doBulkWrite _
        case "doCount" => doCount _
        case "doCountDocuments" => doCountDocuments _
        case "doEstimatedDocumentCount" => doEstimatedDocumentCount _
        case "doDistinct" => doDistinct _
        case "doFind" => doFind _
        case "doDeleteMany" => doDeleteMany _
        case "doDeleteOne" => doDeleteOne _
        case "doFindOneAndDelete" => doFindOneAndDelete _
        case "doFindOneAndReplace" => doFindOneAndReplace _
        case "doFindOneAndUpdate" => doFindOneAndUpdate _
        case "doInsertMany" => doInsertMany _
        case "doInsertOne" => doInsertOne _
        case "doReplaceOne" => doReplaceOne _
        case "doUpdateMany" => doUpdateMany _
        case "doUpdateOne" => doUpdateOne _
        case x => (args: BsonDocument) => throw new IllegalArgumentException(s"Unknown operation: $x")
    }
    op(operation.getDocument("arguments"))
  }
  // scalastyle:on cyclomatic.complexity

  private def createMethodName(name: String, obj: String): String = {
    val builder = new StringBuilder
    builder.append("do")
    if (!obj.isEmpty && !(obj == "collection")) {
      builder.append(obj.substring(0, 1).toUpperCase)
      builder.append(obj.substring(1))
    }
    builder.append(name.substring(0, 1).toUpperCase)
    builder.append(name.substring(1))
    builder.toString
  }

  private def getUpdateOptions(requestArguments: BsonDocument): UpdateOptions = {
    val options = UpdateOptions()
    if (requestArguments.containsKey("upsert")) options.upsert(true)
    if (requestArguments.containsKey("arrayFilters")) options.arrayFilters(getArrayFilters(requestArguments.getArray("arrayFilters")).asJava)
    if (requestArguments.containsKey("collation")) options.collation(getCollation(requestArguments.getDocument("collation")))
    options
  }

  private def getDeleteOptions(requestArguments: BsonDocument): DeleteOptions = {
    val options = DeleteOptions()
    if (requestArguments.containsKey("collation")) options.collation(getCollation(requestArguments.getDocument("collation")))
    options
  }

  private def getReplaceOptions(requestArguments: BsonDocument): ReplaceOptions = {
    val options = new ReplaceOptions
    if (requestArguments.containsKey("upsert")) options.upsert(true)
    if (requestArguments.containsKey("collation")) options.collation(getCollation(requestArguments.getDocument("collation")))
    options
  }

  private def getArrayFilters(bsonArray: BsonArray): Seq[BsonDocument] = {
    val arrayFilters = new mutable.ListBuffer[BsonDocument]()
    if (bsonArray != null) {
      for (cur: BsonValue <- bsonArray.asScala) {
        arrayFilters.append(cur.asDocument)
      }
    }
    arrayFilters.toSeq
  }

  private def toResult(bulkWriteResult: BulkWriteResult, writeModels: Seq[_ <: WriteModel[BsonDocument]],
                       writeErrors: Seq[BulkWriteError]): BsonDocument = {
    val resultDoc = BsonDocument()
    if (bulkWriteResult.wasAcknowledged) {
      resultDoc.append("deletedCount", BsonInt32(bulkWriteResult.getDeletedCount))
      // Determine insertedIds
      val insertedIds = BsonDocument()
      for (writeModel: (WriteModel[BsonDocument], Int) <- writeModels.zipWithIndex) {
        val cur = writeModel._1
        if (cur.isInstanceOf[InsertOneModel[_]] && !writeErrors.exists(_.getIndex == writeModel._2)) {
          val insertOneModel = cur.asInstanceOf[InsertOneModel[BsonDocument]]
          insertedIds.put(s"${writeModel._2}", insertOneModel.getDocument.get("_id"))
        }
      }
      resultDoc.append("insertedIds", insertedIds)
      resultDoc.append("insertedCount", BsonInt32(insertedIds.size))
      resultDoc.append("matchedCount", BsonInt32(bulkWriteResult.getMatchedCount))
      Try(bulkWriteResult.getModifiedCount).map(r => resultDoc.append("modifiedCount", BsonInt32(r.toInt)))
      resultDoc.append("upsertedCount", if (bulkWriteResult.getUpserts == null) BsonInt32(0)
      else BsonInt32(bulkWriteResult.getUpserts.size))
      val upserts = BsonDocument()
      bulkWriteResult.getUpserts.asScala.foreach( b => upserts.put(s"${b.getIndex}", b.getId))
      resultDoc.append("upsertedIds", upserts)
    }
    if (resultDoc.isEmpty) BsonDocument("result" -> BsonNull()) else BsonDocument("result" -> resultDoc)
  }

  private def doAggregation(arguments: BsonDocument) = {
    val pipeline = arguments.getArray("pipeline").asScala.map(_.asDocument()).toSeq
    val observable = collection.get.aggregate[BsonDocument](pipeline)
    if (arguments.containsKey("collation")) observable.collation(getCollation(arguments.getDocument("collation")))
    BsonArray.fromIterable(observable.futureValue)
  }

  private def doDatabaseAggregate(arguments: BsonDocument) = {
    assume(!isSharded)
    val pipeline = arguments.getArray("pipeline").asScala.map(_.asDocument()).toSeq
    val observable = database.get.aggregate[BsonDocument](pipeline)

    if (arguments.containsKey("allowDiskUse")) observable.allowDiskUse(arguments.getBoolean("allowDiskUse").getValue)
    if (arguments.containsKey("collation")) observable.collation(getCollation(arguments.getDocument("collation")))

    val results = observable.futureValue
    for (result <- results) {
      if (result.isDocument) {
        val command = result.asDocument.getDocument("command", BsonDocument())
        command.remove("$readPreference")
        command.remove("$clusterTime")
        command.remove("signature")
        command.remove("keyId")
        command.put("cursor", BsonDocument())
      }
    }
    BsonArray.fromIterable(results)
  }

  private def doCount(arguments: BsonDocument): BsonValue = {
    doCountDocuments(arguments) // TODO ignore/ update
  }

  private def doCountDocuments(arguments: BsonDocument): BsonValue = {
    val options: CountOptions = new CountOptions
    if (arguments.containsKey("skip")) options.skip(arguments.getNumber("skip").intValue)
    if (arguments.containsKey("limit")) options.limit(arguments.getNumber("limit").intValue)
    if (arguments.containsKey("collation")) options.collation(getCollation(arguments.getDocument("collation")))
    BsonInt32(collection.get.countDocuments(arguments.getDocument("filter"), options).futureValue.toInt)
  }

  private def doEstimatedDocumentCount(arguments: BsonDocument): BsonValue = {
    BsonInt32(collection.get.estimatedDocumentCount().futureValue.toInt)
  }

  private def doBulkWrite(arguments: BsonDocument): BsonDocument = {
    val mutableWriteModels = mutable.ListBuffer[WriteModel[BsonDocument]]()
    for (bsonValue: BsonValue <- arguments.getArray("requests").asScala) {
      val cur: BsonDocument = bsonValue.asDocument
      val name: String = cur.getString("name").getValue
      val requestArguments: BsonDocument = cur.getDocument("arguments")

      name match {
        case "insertOne" => mutableWriteModels.append(new InsertOneModel[BsonDocument](requestArguments.getDocument("document")))
        case "updateOne" => mutableWriteModels.append(new UpdateOneModel[BsonDocument](requestArguments.getDocument("filter"),
          requestArguments.getDocument("update"), getUpdateOptions(requestArguments)))
        case "updateMany" => mutableWriteModels.append(new UpdateManyModel[BsonDocument](requestArguments.getDocument("filter"),
          requestArguments.getDocument("update"), getUpdateOptions(requestArguments)))
        case "deleteOne" => mutableWriteModels.append(new DeleteOneModel[BsonDocument](requestArguments.getDocument("filter"),
          getDeleteOptions(requestArguments)))
        case "deleteMany" => mutableWriteModels.append(new DeleteManyModel[BsonDocument](requestArguments.getDocument("filter"),
          getDeleteOptions(requestArguments)))
        case "replaceOne" => mutableWriteModels.append(new ReplaceOneModel[BsonDocument](requestArguments.getDocument("filter"),
          requestArguments.getDocument("replacement"), getReplaceOptions(requestArguments)))
        case _ => throw new UnsupportedOperationException(s"Unsupported write request type: $name")
      }
    }

    val writeModels = mutableWriteModels.toSeq

    Try( collection.get.bulkWrite(writeModels, new BulkWriteOptions().ordered(arguments.getDocument("options", BsonDocument())
      .getBoolean("ordered", BsonBoolean(true)).getValue)).futureValue) match {
      case Success(bulkWriteResult: BulkWriteResult) =>  toResult(bulkWriteResult, writeModels, Seq[BulkWriteError]())
      case Failure(e: TestFailedException) if e.getCause.isInstanceOf[MongoBulkWriteException] => {
        val exception = e.getCause.asInstanceOf[MongoBulkWriteException]
        val result: BsonDocument = toResult(exception.getWriteResult, writeModels, exception.getWriteErrors.asScala.toSeq)
        result.put("error", BsonBoolean(true))
        result
      }
      case Failure(e) => throw e
    }
  }

  private def doDistinct(arguments: BsonDocument): BsonValue = {
    val observable = collection.get.distinct[BsonValue](arguments.getString("fieldName").getValue)
    if (arguments.containsKey("filter")) observable.filter(arguments.getDocument("filter"))
    if (arguments.containsKey("collation")) observable.collation(getCollation(arguments.getDocument("collation")))
    BsonArray.fromIterable(observable.futureValue)
  }

  private def doFind(arguments: BsonDocument): BsonValue = {
    val observable = collection.get.find[BsonDocument](arguments.getDocument("filter"))
    if (arguments.containsKey("skip")) observable.skip(arguments.getNumber("skip").intValue)
    if (arguments.containsKey("limit")) observable.limit(arguments.getNumber("limit").intValue)
    if (arguments.containsKey("collation")) observable.collation(getCollation(arguments.getDocument("collation")))
    BsonArray.fromIterable(observable.futureValue)
  }

  private def doDeleteMany(arguments: BsonDocument): BsonValue = {
    val options: DeleteOptions = new DeleteOptions
    if (arguments.containsKey("collation")) options.collation(getCollation(arguments.getDocument("collation")))
    val result = collection.get.deleteMany(arguments.getDocument("filter"), options).futureValue
    BsonDocument("deletedCount" -> BsonInt32(result.getDeletedCount.toInt))
  }

  private def doDeleteOne(arguments: BsonDocument): BsonValue = {
    val options: DeleteOptions = new DeleteOptions
    if (arguments.containsKey("collation")) options.collation(getCollation(arguments.getDocument("collation")))
    val result = collection.get.deleteOne(arguments.getDocument("filter"), options).futureValue
    BsonDocument("deletedCount" -> BsonInt32(result.getDeletedCount.toInt))
  }

  private def doFindOneAndDelete(arguments: BsonDocument): BsonValue = {
    val options: FindOneAndDeleteOptions = new FindOneAndDeleteOptions
    if (arguments.containsKey("projection")) options.projection(arguments.getDocument("projection"))
    if (arguments.containsKey("sort")) options.sort(arguments.getDocument("sort"))
    if (arguments.containsKey("collation")) options.collation(getCollation(arguments.getDocument("collation")))
    Option(collection.get.findOneAndDelete(arguments.getDocument("filter"), options).futureValue).getOrElse(BsonNull())
  }

  private def doFindOneAndReplace(arguments: BsonDocument): BsonValue = {
    val options: FindOneAndReplaceOptions = new FindOneAndReplaceOptions
    if (arguments.containsKey("projection")) options.projection(arguments.getDocument("projection"))
    if (arguments.containsKey("sort")) options.sort(arguments.getDocument("sort"))
    if (arguments.containsKey("upsert")) options.upsert(arguments.getBoolean("upsert").getValue)
    if (arguments.containsKey("returnDocument")) {
      val rd = if (arguments.getString("returnDocument").getValue == "After") ReturnDocument.AFTER else ReturnDocument.BEFORE
      options.returnDocument(rd)
    }
    if (arguments.containsKey("collation")) options.collation(getCollation(arguments.getDocument("collation")))
    Option(collection.get.findOneAndReplace(arguments.getDocument("filter"),
      arguments.getDocument("replacement"), options).futureValue).getOrElse(BsonNull())
  }

  private def doFindOneAndUpdate(arguments: BsonDocument): BsonValue = {
    val options: FindOneAndUpdateOptions = FindOneAndUpdateOptions()
    if (arguments.containsKey("projection")) options.projection(arguments.getDocument("projection"))
    if (arguments.containsKey("sort")) options.sort(arguments.getDocument("sort"))
    if (arguments.containsKey("upsert")) options.upsert(arguments.getBoolean("upsert").getValue)
    if (arguments.containsKey("returnDocument")) {
      val rd = if (arguments.getString("returnDocument").getValue == "After") ReturnDocument.AFTER else ReturnDocument.BEFORE
      options.returnDocument(rd)
    }
    if (arguments.containsKey("collation")) options.collation(getCollation(arguments.getDocument("collation")))
    if (arguments.containsKey("arrayFilters")) options.arrayFilters(getArrayFilters(arguments.getArray("arrayFilters")).asJava)

    Option(collection.get.findOneAndUpdate(arguments.getDocument("filter"),
      arguments.getDocument("update"), options).futureValue).getOrElse(BsonNull())
  }

  private def doInsertOne(arguments: BsonDocument): BsonValue = {
    val document = arguments.getDocument("document")
    collection.get.insertOne(document).futureValue
    Document(("insertedId", Option(document.get("_id")).getOrElse(BsonNull()))).underlying
  }

  private def doInsertMany(arguments: BsonDocument): BsonValue = {
    val documents = arguments.getArray("documents").asScala.map(_.asDocument()).toSeq
    val options = new InsertManyOptions().ordered(arguments.getDocument("options", BsonDocument())
      .getBoolean("ordered", BsonBoolean(true)).getValue)
    Try(collection.get.insertMany(documents, options).futureValue) match {
      case Success(_) => {
        val docs = Document(documents.zipWithIndex.map({ case (doc: BsonDocument, i: Int) => (i.toString, doc.get("_id", BsonNull())) }))
        Document("insertedIds" -> docs).underlying
      }
      case Failure(e: TestFailedException) if e.getCause.isInstanceOf[MongoBulkWriteException] => {
        // Test results are expecting this to look just like bulkWrite error, so translate to InsertOneModel so the result
        // translation code can be reused.
        val exception = e.getCause.asInstanceOf[MongoBulkWriteException]
        val writeModels = arguments.getArray("documents").asScala.map(doc => new InsertOneModel[BsonDocument](doc.asDocument)).toSeq
        val result: BsonDocument = toResult(exception.getWriteResult, writeModels, exception.getWriteErrors.asScala.toSeq)
        result.put("error", BsonBoolean(true))
        result
      }
      case Failure(e) => throw e
    }
  }

  private def doReplaceOne(arguments: BsonDocument): BsonValue = {
    val options: ReplaceOptions = ReplaceOptions()
    if (arguments.containsKey("upsert")) options.upsert(arguments.getBoolean("upsert").getValue)
    if (arguments.containsKey("collation")) options.collation(getCollation(arguments.getDocument("collation")))
    val rawResult = collection.get.replaceOne(arguments.getDocument("filter"), arguments.getDocument("replacement"), options).futureValue
    convertUpdateResult(rawResult)
  }

  private def doUpdateMany(arguments: BsonDocument): BsonValue = {
    val options: UpdateOptions = UpdateOptions()
    if (arguments.containsKey("upsert")) options.upsert(arguments.getBoolean("upsert").getValue)
    if (arguments.containsKey("collation")) options.collation(getCollation(arguments.getDocument("collation")))
    if (arguments.containsKey("arrayFilters")) options.arrayFilters(getArrayFilters(arguments.getArray("arrayFilters")).asJava)
    val result = collection.get.updateMany(arguments.getDocument("filter"),
      arguments.getDocument("update"), options).futureValue
    convertUpdateResult(result)
  }

  private def doUpdateOne(arguments: BsonDocument): BsonValue = {
    val options: UpdateOptions = UpdateOptions()
    if (arguments.containsKey("upsert")) options.upsert(arguments.getBoolean("upsert").getValue)
    if (arguments.containsKey("collation")) options.collation(getCollation(arguments.getDocument("collation")))
    if (arguments.containsKey("arrayFilters")) options.arrayFilters(getArrayFilters(arguments.getArray("arrayFilters")).asJava)
    val result = collection.get.updateOne(arguments.getDocument("filter"), arguments.getDocument("update"), options).futureValue
    convertUpdateResult(result)
  }

  private def getCollation(bsonCollation: BsonDocument): Collation = {
    val builder: Collation.Builder = Collation.builder
    if (bsonCollation.containsKey("locale")) builder.locale(bsonCollation.getString("locale").getValue)
    if (bsonCollation.containsKey("caseLevel")) builder.caseLevel(bsonCollation.getBoolean("caseLevel").getValue)
    if (bsonCollation.containsKey("caseFirst")) builder.collationCaseFirst(CollationCaseFirst.fromString(bsonCollation.getString("caseFirst").getValue).get)
    if (bsonCollation.containsKey("numericOrdering")) builder.numericOrdering(bsonCollation.getBoolean("numericOrdering").getValue)
    if (bsonCollation.containsKey("strength")) builder.collationStrength(CollationStrength.fromInt(bsonCollation.getInt32("strength").getValue).get)
    if (bsonCollation.containsKey("alternate")) builder.collationAlternate(CollationAlternate.fromString(bsonCollation.getString("alternate").getValue).get)
    if (bsonCollation.containsKey("maxVariable")) builder.collationMaxVariable(CollationMaxVariable.fromString(bsonCollation.getString("maxVariable").getValue).get)
    if (bsonCollation.containsKey("normalization")) builder.normalization(bsonCollation.getBoolean("normalization").getValue)
    if (bsonCollation.containsKey("backwards")) builder.backwards(bsonCollation.getBoolean("backwards").getValue)
    builder.build
  }

  private def convertUpdateResult(result: UpdateResult): BsonDocument = {
    val resultDoc: BsonDocument = BsonDocument("matchedCount" -> BsonInt32(result.getMatchedCount.toInt))
    Try(result.getModifiedCount).map(r => resultDoc.append("modifiedCount", BsonInt32(r.toInt)))

    val upsertedCount = result.getUpsertedId match {
      case id: BsonValue if !id.isObjectId => resultDoc.append("upsertedId", id); BsonInt32(1)
      case _: BsonValue  => BsonInt32(1)
      case _ /* empty */ => BsonInt32(0)
    }
    resultDoc.append("upsertedCount", upsertedCount)
  }


  private def serverAtLeastMinVersion(definition: Document): Boolean = {
    definition.get[BsonString]("minServerVersion") match {
      case Some(minServerVersion) =>
        serverVersionAtLeast(minServerVersion.getValue.split("\\.").map(_.toInt).padTo(3, 0).take(3).toList)
      case None => true
    }
  }

  private def serverLessThanMaxVersion(definition: Document): Boolean = {
    definition.get[BsonString]("maxServerVersion") match {
      case Some(maxServerVersion) =>
        serverVersionLessThan(maxServerVersion.getValue.split("\\.").map(_.toInt).padTo(3, 0).take(3).toList)
      case None => true
    }
  }

}
