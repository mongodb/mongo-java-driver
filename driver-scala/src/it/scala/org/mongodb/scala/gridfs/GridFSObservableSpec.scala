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

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.util.UUID

import org.bson.UuidRepresentation
import org.bson.codecs.UuidCodec
import org.bson.codecs.configuration.CodecRegistries
import org.mongodb.scala._
import org.mongodb.scala.bson.{ BsonBinary, BsonDocument, BsonString, ObjectId }
import org.mongodb.scala.model.{ Filters, Updates }
import org.scalatest.BeforeAndAfterEach
import org.scalatest.exceptions.TestFailedException

import scala.annotation.tailrec

class GridFSObservableSpec extends RequiresMongoDBISpec with FuturesSpec with BeforeAndAfterEach {
  private val filesCollectionName = "fs.files"
  private val chunksCollectionName = "fs.chunks"
  private var _gridFSBucket: Option[GridFSBucket] = None
  private var _filesCollection: Option[MongoCollection[GridFSFile]] = None
  private var _chunksCollection: Option[MongoCollection[Document]] = None
  private val singleChunkString = "GridFS"
  private val multiChunkString = f"${singleChunkString}%1305600s"

  override def beforeEach(): Unit = {
    val mongoDatabase = mongoClient().getDatabase(databaseName)
    _filesCollection = Some(mongoDatabase.getCollection[GridFSFile](filesCollectionName))
    _chunksCollection = Some(mongoDatabase.getCollection(chunksCollectionName))
    _filesCollection.map(_.drop())
    _chunksCollection.map(_.drop())
    _gridFSBucket = Some(GridFSBucket(mongoDatabase))
  }

  override def afterEach(): Unit = {
    withDatabase(db => db.drop())
  }

  private def gridFSBucket = _gridFSBucket.get

  private def filesCollection = _filesCollection.get

  private def chunksCollection = _chunksCollection.get

  "The Scala driver" should "round trip a small file" in {
    val contentBytes = singleChunkString.getBytes()
    val expectedLength = contentBytes.length

    val fileId =
      gridFSBucket.uploadFromObservable("myFile", Observable(Seq(ByteBuffer.wrap(contentBytes)))).head().futureValue
    filesCollection.countDocuments().head().futureValue should equal(1)
    chunksCollection.countDocuments().head().futureValue should equal(1)

    val fileInfo = gridFSBucket.find().filter(Filters.eq("_id", fileId)).head().futureValue
    fileInfo.getObjectId should equal(fileId)
    fileInfo.getChunkSize should equal(gridFSBucket.chunkSizeBytes)
    fileInfo.getLength should equal(expectedLength)
    Option(fileInfo.getMetadata) should equal(None)
  }

  it should "round trip a large file" in {
    val contentBytes = multiChunkString.getBytes()
    val expectedLength = contentBytes.length

    val fileId =
      gridFSBucket.uploadFromObservable("myFile", Observable(Seq(ByteBuffer.wrap(contentBytes)))).head().futureValue
    filesCollection.countDocuments().head().futureValue should equal(1)
    chunksCollection.countDocuments().head().futureValue should equal(5)

    val fileInfo = gridFSBucket.find().filter(Filters.eq("_id", fileId)).head().futureValue
    fileInfo.getObjectId should equal(fileId)
    fileInfo.getChunkSize should equal(gridFSBucket.chunkSizeBytes)
    fileInfo.getLength should equal(expectedLength)
    Option(fileInfo.getMetadata) should equal(None)
  }

  it should "round trip with small chunks" in {
    val contentSize = 1024 * 500
    val chunkSize = 10
    val contentBytes = new Array[Byte](contentSize / 2)
    scala.util.Random.nextBytes(contentBytes)
    val options = new GridFSUploadOptions().chunkSizeBytes(chunkSize)

    val fileId = gridFSBucket
      .uploadFromObservable(
        "myFile",
        Observable(Seq(ByteBuffer.wrap(contentBytes), ByteBuffer.wrap(contentBytes))),
        options
      )
      .head()
      .futureValue
    filesCollection.countDocuments().head().futureValue should equal(1)
    chunksCollection.countDocuments().head().futureValue should equal(contentSize / chunkSize)

    val data = gridFSBucket.downloadToObservable(fileId).futureValue
    concatByteBuffers(data) should equal(
      concatByteBuffers(Seq(ByteBuffer.wrap(contentBytes), ByteBuffer.wrap(contentBytes)))
    )
  }

  it should "round trip with data larger than the internal bufferSize" in {
    val contentSize = 1024 * 1024 * 5
    val chunkSize = 1024 * 1024
    val contentBytes = new Array[Byte](contentSize)
    scala.util.Random.nextBytes(contentBytes)
    val options = new GridFSUploadOptions().chunkSizeBytes(chunkSize)

    val fileId = gridFSBucket
      .uploadFromObservable("myFile", Observable(Seq(ByteBuffer.wrap(contentBytes))), options)
      .head()
      .futureValue
    filesCollection.countDocuments().head().futureValue should equal(1)
    chunksCollection.countDocuments().head().futureValue should equal(contentSize / chunkSize)

    val data = gridFSBucket.downloadToObservable(fileId).futureValue
    concatByteBuffers(data) should equal(contentBytes)
  }

  it should "handle custom ids" in {
    val contentBytes = multiChunkString.getBytes()
    val fileId = BsonString("myFile")
    gridFSBucket
      .uploadFromObservable(fileId, "myFile", Observable(Seq(ByteBuffer.wrap(contentBytes))))
      .head()
      .futureValue
    var data = gridFSBucket.downloadToObservable(fileId).futureValue

    concatByteBuffers(data) should equal(contentBytes)

    gridFSBucket.rename(fileId, "newName").futureValue
    data = gridFSBucket.downloadToObservable("newName").futureValue

    concatByteBuffers(data) should equal(contentBytes)

    gridFSBucket.delete(fileId).futureValue
    filesCollection.countDocuments().head().futureValue should equal(0)
    chunksCollection.countDocuments().head().futureValue should equal(0)
  }

  it should "throw a chunk not found error when there are no chunks" in {
    val contentSize = 1024 * 1024
    val contentBytes = new Array[Byte](contentSize)
    scala.util.Random.nextBytes(contentBytes)

    val fileId = gridFSBucket.uploadFromObservable("myFile", Observable(Seq(ByteBuffer.wrap(contentBytes)))).futureValue
    chunksCollection.deleteMany(Filters.eq("files_id", fileId)).futureValue

    val caught = intercept[TestFailedException] {
      gridFSBucket.downloadToObservable(fileId).futureValue
    }

    caught.cause.exists(t => t.isInstanceOf[MongoGridFSException]) should equal(true)
  }

  it should "round trip with a bufferSizeBytes of 4096" in {
    val contentSize = 1024 * 1024
    val chunkSize = 1024
    val bufferSizeBytes = 4096
    val contentBytes = new Array[Byte](contentSize)
    scala.util.Random.nextBytes(contentBytes)
    val options = new GridFSUploadOptions().chunkSizeBytes(chunkSize)

    val fileId = gridFSBucket
      .uploadFromObservable("myFile", Observable(Seq(ByteBuffer.wrap(contentBytes))), options)
      .head()
      .futureValue
    filesCollection.countDocuments().head().futureValue should equal(1)
    chunksCollection.countDocuments().head().futureValue should equal(contentSize / chunkSize)

    val fileInfo = gridFSBucket.find().filter(Filters.eq("_id", fileId)).head().futureValue
    fileInfo.getObjectId should equal(fileId)
    fileInfo.getChunkSize should equal(chunkSize)
    fileInfo.getLength should equal(contentSize)
    Option(fileInfo.getMetadata) should equal(None)

    val data = gridFSBucket.downloadToObservable(fileId).bufferSizeBytes(bufferSizeBytes).futureValue
    concatByteBuffers(data) should equal(concatByteBuffers(Seq(ByteBuffer.wrap(contentBytes))))
  }

  it should "handle uploading publisher erroring" in {
    val errorMessage = "Failure Propagated"
    val source = new Observable[ByteBuffer] {
      override def subscribe(observer: Observer[_ >: ByteBuffer]): Unit =
        observer.onError(new IllegalArgumentException(errorMessage))
    }

    val caught = intercept[TestFailedException] {
      gridFSBucket.uploadFromObservable("myFile", source).futureValue
    }

    caught.cause.exists(t => t.isInstanceOf[IllegalArgumentException]) should equal(true)
    caught.cause.get.getMessage should equal(errorMessage)
  }

  it should "use custom uploadOptions when uploading" in {
    val chunkSize = 20
    val metaData = Document("archived" -> false)
    val options = new GridFSUploadOptions().chunkSizeBytes(chunkSize).metadata(metaData)
    val contentBytes = multiChunkString.getBytes()
    val expectedLength = contentBytes.length
    val expectedNoChunks = Math.ceil((expectedLength.toDouble) / chunkSize).toInt

    val fileId = gridFSBucket
      .uploadFromObservable("myFile", Observable(Seq(ByteBuffer.wrap(contentBytes))), options)
      .head()
      .futureValue
    filesCollection.countDocuments().head().futureValue should equal(1)
    chunksCollection.countDocuments().head().futureValue should equal(expectedNoChunks)

    val fileInfo = gridFSBucket.find().filter(Filters.eq("_id", fileId)).head().futureValue
    fileInfo.getObjectId should equal(fileId)
    fileInfo.getChunkSize should equal(chunkSize)
    fileInfo.getLength should equal(expectedLength)
    Option(fileInfo.getMetadata).isEmpty should equal(false)
    fileInfo.getMetadata.get("archived") should equal(false)

    val data = gridFSBucket.downloadToObservable(fileId).futureValue
    concatByteBuffers(data) should equal(concatByteBuffers(Seq(ByteBuffer.wrap(contentBytes))))
  }

  it should "be able to open by name" in {
    val filename = "myFile"
    val contentBytes = singleChunkString.getBytes()

    gridFSBucket.uploadFromObservable(filename, Observable(Seq(ByteBuffer.wrap(contentBytes)))).head().futureValue

    val data = gridFSBucket.downloadToObservable(filename).futureValue
    concatByteBuffers(data) should equal(concatByteBuffers(Seq(ByteBuffer.wrap(contentBytes))))
  }

  it should "be able to handle missing file" in {
    val caught = intercept[TestFailedException] {
      gridFSBucket.downloadToObservable("myFile").futureValue
    }

    caught.cause.exists(t => t.isInstanceOf[MongoGridFSException]) should equal(true)
  }

  it should "create the indexes as expected" in {
    val filesIndexKey: BsonDocument = Document("filename" -> 1, "uploadDate" -> 1).toBsonDocument
    val chunksIndexKey: BsonDocument = Document("files_id" -> 1, "n" -> 1).toBsonDocument

    filesCollection.listIndexes().futureValue.map(_.getOrElse("key", Document())) should not contain (filesIndexKey)
    chunksCollection.listIndexes().futureValue.map(_.getOrElse("key", Document())) should not contain (chunksIndexKey)

    gridFSBucket
      .uploadFromObservable("myFile", Observable(Seq(ByteBuffer.wrap(multiChunkString.getBytes()))))
      .futureValue

    filesCollection.listIndexes().futureValue.map(_.getOrElse("key", Document())) should contain(filesIndexKey)
    chunksCollection.listIndexes().futureValue.map(_.getOrElse("key", Document())) should contain(chunksIndexKey)
  }

  it should "not create indexes if the files collection is not empty" in {
    filesCollection.withDocumentClass[Document].insertOne(Document("filename" -> "bad file")).futureValue

    filesCollection.listIndexes().futureValue.size should equal(1)
    chunksCollection.listIndexes().futureValue.size should equal(0)

    gridFSBucket
      .uploadFromObservable("myFile", Observable(Seq(ByteBuffer.wrap(multiChunkString.getBytes()))))
      .futureValue

    filesCollection.listIndexes().futureValue.size should equal(1)
    chunksCollection.listIndexes().futureValue.size should equal(1)
  }

  it should "use the user provided codec registries for encoding / decoding data" in {
    val client = MongoClient(
      mongoClientSettingsBuilder
        .uuidRepresentation(UuidRepresentation.STANDARD)
        .build()
    )

    val database = client.getDatabase(databaseName)
    val uuid = UUID.randomUUID()
    val fileMeta = new org.bson.Document("uuid", uuid)
    val bucket = GridFSBucket(database)

    val fileId = bucket
      .uploadFromObservable(
        "myFile",
        Observable(Seq(ByteBuffer.wrap(multiChunkString.getBytes()))),
        new GridFSUploadOptions().metadata(fileMeta)
      )
      .head()
      .futureValue

    val fileAsDocument = filesCollection.find[BsonDocument]().head().futureValue
    fileAsDocument.getDocument("metadata").getBinary("uuid").getType should equal(4.toByte)
    fileAsDocument.getDocument("metadata").getBinary("uuid").asUuid() should equal(uuid)
  }

  it should "handle missing file name data when downloading" in {
    val fileId = gridFSBucket
      .uploadFromObservable("myFile", Observable(Seq(ByteBuffer.wrap(multiChunkString.getBytes()))))
      .head()
      .futureValue

    filesCollection.updateOne(Filters.eq("_id", fileId), Updates.unset("filename")).futureValue
    val data = gridFSBucket.downloadToObservable(fileId).futureValue

    concatByteBuffers(data) should equal(multiChunkString.getBytes())
  }

  it should "cleanup when unsubscribing" in {
    val contentSize = 1024 * 1024
    val contentBytes = new Array[Byte](contentSize)
    scala.util.Random.nextBytes(contentBytes)

    trait SubscriptionObserver[T] extends Observer[T] {
      def subscription(): Subscription
    }

    val observer = new SubscriptionObserver[ObjectId] {
      var s: Option[Subscription] = None
      var completed: Boolean = false
      def subscription(): Subscription = s.get
      override def onSubscribe(subscription: Subscription): Unit =
        s = Some(subscription)

      override def onNext(result: ObjectId): Unit = {}

      override def onError(e: Throwable): Unit = {}

      override def onComplete(): Unit = completed = true
    }
    gridFSBucket
      .uploadFromObservable("myFile", Observable(List.fill(1024)(ByteBuffer.wrap(contentBytes))))
      .subscribe(observer)

    observer.subscription().request(1)

    retry(10)(() => chunksCollection.countDocuments().futureValue should be > 0L)
    filesCollection.countDocuments().futureValue should equal(0)

    observer.subscription().unsubscribe()

    if (!observer.completed) {
      retry(50)(() => chunksCollection.countDocuments().futureValue should equal(0))
      filesCollection.countDocuments().futureValue should equal(0)
    }
  }

  @tailrec
  private def retry[T](n: Int)(fn: () => T): T = {
    try {
      fn()
    } catch {
      case e: Exception =>
        if (n > 1) {
          Thread.sleep(250)
          retry(n - 1)(fn)
        } else {
          throw e
        }
    }
  }

  private def concatByteBuffers(buffers: Seq[ByteBuffer]): Array[Byte] = {
    val outputStream = new ByteArrayOutputStream()
    val channel = Channels.newChannel(outputStream)
    buffers.map(channel.write)
    outputStream.close()
    channel.close()
    outputStream.toByteArray
  }

}
