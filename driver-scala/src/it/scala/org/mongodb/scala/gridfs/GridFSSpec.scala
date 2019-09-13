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

package org.mongodb.scala.gridfs

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, File, InputStream }

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Try
import org.bson.{ BsonArray, BsonBinary, BsonInt32 }
import org.mongodb.scala._
import org.mongodb.scala.bson.collection.mutable
import org.mongodb.scala.bson.{ BsonBoolean, BsonDocument, BsonInt64, BsonObjectId, BsonString }
import org.mongodb.scala.gridfs.helpers.AsyncStreamHelper
import org.scalatest.Inspectors.forEvery

class GridFSSpec extends RequiresMongoDBISpec with FuturesSpec {
  private val filesCollectionName = "fs.files"
  private val chunksCollectionName = "fs.chunks"
  lazy val files = new File(getClass.getResource("/gridfs-tests").toURI).listFiles.filter(_.getName.endsWith(".json"))
  var gridFSBucket: Option[GridFSBucket] = None
  var filesCollection: Option[MongoCollection[Document]] = None
  var chunksCollection: Option[MongoCollection[Document]] = None

  forEvery(files) { (file: File) =>
    s"Running ${file.getName} tests" should "pass all scenarios" in withDatabase(databaseName) { database =>
      gridFSBucket = Some(GridFSBucket(database))
      filesCollection = Some(database.getCollection(filesCollectionName))
      chunksCollection = Some(database.getCollection(chunksCollectionName))
      val definition = BsonDocument(Source.fromFile(file).getLines.mkString)
      val data = definition.getDocument("data")
      val tests = definition.getArray("tests").asScala.map(_.asDocument())

      forEvery(tests) { (test: BsonDocument) =>
        info(test.getString("description").getValue)
        val arrange: BsonDocument = test.getDocument("arrange", BsonDocument())
        val action: BsonDocument = test.getDocument("act", BsonDocument())
        val assertion: BsonDocument = test.getDocument("assert", BsonDocument())

        arrangeGridFS(data, arrange)
        actionGridFS(action, assertion)
      }
    }
  }

  // scalastyle:off cyclomatic.complexity
  private def arrangeGridFS(data: BsonDocument, arrange: BsonDocument): Unit = {
    gridFSBucket.map(_.drop()).get.futureValue
    val filesDocuments: List[Document] = processFiles(data.getArray("files", new BsonArray))
    val chunksDocuments: List[Document] = processChunks(data.getArray("chunks", new BsonArray))

    if (filesDocuments.nonEmpty) filesCollection.map(_.insertMany(filesDocuments)).get.futureValue
    if (chunksDocuments.nonEmpty) chunksCollection.map(_.insertMany(chunksDocuments)).get.futureValue

    for (fileToArrange <- arrange.getArray("data", new BsonArray).asScala) {
      val document = fileToArrange.asDocument
      if (document.containsKey("delete") && document.containsKey("deletes")) {
        for (toDelete <- document.getArray("deletes").asScala) {

          val collection = document.getString("delete") match {
            case isFilesCollection(_)  => filesCollection.get
            case isChunksCollection(_) => chunksCollection.get
            case x                     => throw new IllegalArgumentException(s"Unknown collection to delete: $x")
          }

          val query = toDelete.asDocument.getDocument("q")
          val limit: Int = toDelete.asDocument.getInt32("limit").getValue
          limit match {
            case 1 => collection.deleteOne(query).futureValue
            case _ => collection.deleteMany(query).futureValue
          }
        }
      } else if (document.containsKey("insert") && document.containsKey("documents")) {
        document.getString("insert") match {
          case isFilesCollection(_) =>
            filesCollection.map(_.insertMany(processFiles(document.getArray("documents")))).get.futureValue
          case isChunksCollection(_) =>
            chunksCollection.map(_.insertMany(processChunks(document.getArray("documents")))).get.futureValue
          case x => throw new IllegalArgumentException(s"Unknown collection to insert data into: $x")
        }
      } else if (document.containsKey("update") && document.containsKey("updates")) {
        val collection = document.getString("update") match {
          case isFilesCollection(_)  => filesCollection.get
          case isChunksCollection(_) => chunksCollection.get
          case x                     => throw new IllegalArgumentException(s"Unknown collection to update: $x")
        }
        for (rawUpdate <- document.getArray("updates").asScala) {
          val query: Document = rawUpdate.asDocument.getDocument("q")
          val update: mutable.Document = mutable.Document(rawUpdate.asDocument.getDocument("u"))
          update.put("$set", parseHexDocument(update.get[BsonDocument]("$set").get))
          collection.updateMany(query, update).head().futureValue
        }
      } else {
        throw new IllegalArgumentException("Unsupported arrange: " + document)
      }
    }
  }
  // scalastyle:on cyclomatic.complexity

  private def actionGridFS(action: BsonDocument, assertion: BsonDocument): Unit = {
    if (!action.isEmpty) {
      action.getString("operation").getValue match {
        case "delete"           => doDelete(action.getDocument("arguments"), assertion)
        case "download"         => doDownload(action.getDocument("arguments"), assertion)
        case "download_by_name" => doDownloadByName(action.getDocument("arguments"), assertion)
        case "upload"           => doUpload(action.getDocument("arguments"), assertion)
        case x                  => throw new IllegalArgumentException(s"Unknown operation: $x")
      }
    }
  }

  private def doDelete(arguments: BsonDocument, assertion: BsonDocument): Unit = {
    val result = Try(gridFSBucket.map(_.delete(arguments.getObjectId("id").getValue)).get.futureValue)
    assertion.containsKey("error") match {
      case true => result should be a Symbol("failure")
      case false =>
        result should be a Symbol("success")
        for (rawDataItem <- assertion.getArray("data").asScala) {
          val dataItem: BsonDocument = rawDataItem.asDocument
          for (deletedItem <- dataItem.getArray("deletes", new BsonArray).asScala) {
            val delete: String = dataItem.getString("delete", new BsonString("none")).getValue
            val id: BsonObjectId = delete match {
              case "expected.files"  => deletedItem.asDocument.getDocument("q").getObjectId("_id")
              case "expected.chunks" => deletedItem.asDocument.getDocument("q").getObjectId("files_id")
            }

            val filesCount: Long = getFilesCount(new BsonDocument("_id", id))
            val chunksCount: Long = getChunksCount(new BsonDocument("files_id", id))

            filesCount should equal(0)
            chunksCount should equal(0)
          }
        }
    }
  }

  private def doDownload(arguments: BsonDocument, assertion: BsonDocument): Unit = {
    val outputStream: ByteArrayOutputStream = new ByteArrayOutputStream
    val result = Try(
      gridFSBucket
        .map(
          _.downloadToStream(arguments.getObjectId("id").getValue, AsyncStreamHelper.toAsyncOutputStream(outputStream))
            .head()
        )
        .get
        .futureValue
    )
    outputStream.close()

    assertion.containsKey("error") match {
      case true =>
        result should be a Symbol("failure")
      case false =>
        result should be a Symbol("success")
        outputStream.toByteArray.map("%02x".format(_)).mkString.toLowerCase should equal(
          assertion.getDocument("result").getString("$hex").getValue
        )
    }
  }

  private def doDownloadByName(arguments: BsonDocument, assertion: BsonDocument): Unit = {
    val outputStream: ByteArrayOutputStream = new ByteArrayOutputStream
    val options: GridFSDownloadOptions = new GridFSDownloadOptions()
    Option(arguments.get("options")).map(opts => options.revision(opts.asDocument().getInt32("revision").getValue))

    val result = Try(
      gridFSBucket
        .map(
          _.downloadToStream(
            arguments.getString("filename").getValue,
            AsyncStreamHelper.toAsyncOutputStream(outputStream),
            options
          ).head()
        )
        .get
        .futureValue
    )
    outputStream.close()

    assertion.containsKey("error") match {
      case true =>
        result should be a Symbol("failure")
      case false =>
        result should be a Symbol("success")
        outputStream.toByteArray.map("%02x".format(_)).mkString.toLowerCase should equal(
          assertion.getDocument("result").getString("$hex").getValue
        )
    }
  }

  //scalastyle:off method.length
  private def doUpload(rawArguments: BsonDocument, assertion: BsonDocument): Unit = {

    val arguments: BsonDocument = parseHexDocument(rawArguments, "source")

    val filename: String = arguments.getString("filename").getValue
    val inputStream: InputStream = new ByteArrayInputStream(arguments.getBinary("source").getData)
    val rawOptions: Document = arguments.getDocument("options", new BsonDocument())
    val options: GridFSUploadOptions = new GridFSUploadOptions()
    rawOptions.get[BsonInt32]("chunkSizeBytes").map(chunkSize => options.chunkSizeBytes(chunkSize.getValue))
    rawOptions.get[BsonDocument]("metadata").map(doc => options.metadata(doc))
    val disableMD5: Boolean = rawOptions.get[BsonBoolean]("disableMD5").getOrElse(BsonBoolean(false)).getValue

    val result = Try(
      gridFSBucket
        .map(
          bucket => bucket.uploadFromStream(filename, AsyncStreamHelper.toAsyncInputStream(inputStream), options).head()
        )
        .get
        .futureValue
    )

    assertion.containsKey("error") match {
      case true =>
        result should be a Symbol("failure")
      /*
         // We don't need to read anything more so don't see the extra chunk
         if (!assertion.getString("error").getValue == "ExtraChunk") assertNotNull("Should have thrown an exception", error)
       */
      case false =>
        result should be a Symbol("success")
        val objectId = result.get
        for (rawDataItem <- assertion.getArray("data", new BsonArray).asScala) {
          val dataItem: BsonDocument = rawDataItem.asDocument
          val insert: String = dataItem.getString("insert", new BsonString("none")).getValue
          insert match {
            case "expected.files" =>
              val documents: List[Document] = processFiles(dataItem.getArray("documents", new BsonArray))
              getFilesCount(new BsonDocument) should equal(documents.size)

              val actual: Document = filesCollection.map(_.find().first().head()).get.futureValue
              for (expected <- documents) {
                expected.get("length") should equal(actual.get("length"))
                expected.get("chunkSize") should equal(actual.get("chunkSize"))
                expected.get("filename") should equal(actual.get("filename"))
                if (expected.contains("metadata")) expected.get("metadata") should equal(actual.get("metadata"))
              }
            case "expected.chunks" =>
              val documents: List[Document] = processChunks(dataItem.getArray("documents", new BsonArray))
              getChunksCount(new BsonDocument) should equal(documents.size)

              val actualDocuments: Seq[Document] = chunksCollection.map(_.find()).get.futureValue

              for ((expected, actual) <- documents zip actualDocuments) {
                new BsonObjectId(objectId) should equal(actual.get[BsonObjectId]("files_id").get)
                expected.get("n") should equal(actual.get("n"))
                expected.get("data") should equal(actual.get("data"))
              }
          }
        }
    }
  }
  //scalastyle:on method.length

  private def getChunksCount(filter: BsonDocument): Long =
    chunksCollection.map(col => col.countDocuments(filter).head()).get.futureValue
  private def getFilesCount(filter: BsonDocument): Long =
    filesCollection.map(col => col.countDocuments(filter).head()).get.futureValue

  private def processFiles(bsonArray: BsonArray): List[Document] = {
    val documents = ListBuffer[Document]()
    for (rawDocument <- bsonArray.getValues.asScala) {
      if (rawDocument.isDocument) {
        val document: BsonDocument = rawDocument.asDocument
        if (document.get("length").isInt32) document.put("length", BsonInt64(document.getInt32("length").getValue))
        if (document.containsKey("metadata") && document.getDocument("metadata").isEmpty) document.remove("metadata")
        if (document.containsKey("aliases") && document.getArray("aliases").getValues.size == 0)
          document.remove("aliases")
        if (document.containsKey("contentType") && document.getString("contentType").getValue.length == 0)
          document.remove("contentType")
        documents += document
      }
    }
    documents.toList
  }

  private def processChunks(bsonArray: BsonArray): List[Document] = {
    val documents = ListBuffer[Document]()
    for (rawDocument <- bsonArray.getValues.asScala) {
      if (rawDocument.isDocument) documents += parseHexDocument(rawDocument.asDocument)
    }
    documents.toList
  }

  private def parseHexDocument(document: BsonDocument): BsonDocument = parseHexDocument(document, "data")

  private def parseHexDocument(document: BsonDocument, hexDocument: String): BsonDocument = {
    if (document.contains(hexDocument) && document.get(hexDocument).isDocument) {
      val bytes: Array[Byte] = document
        .getDocument(hexDocument)
        .getString("$hex")
        .getValue
        .sliding(2, 2)
        .map(i => Integer.parseInt(i, 16).toByte)
        .toArray
      document.put(hexDocument, new BsonBinary(bytes))
    }
    document
  }

  private object isFilesCollection {
    def unapply(name: BsonString): Option[Boolean] = if (name.getValue == filesCollectionName) Some(true) else None
  }
  private object isChunksCollection {
    def unapply(name: BsonString): Option[Boolean] = if (name.getValue == chunksCollectionName) Some(true) else None
  }

}
