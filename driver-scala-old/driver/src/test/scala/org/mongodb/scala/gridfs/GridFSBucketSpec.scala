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

import org.bson.types.ObjectId
import com.mongodb.reactivestreams.client.gridfs.{GridFSBucket => JGridFSBucket}
import org.mongodb.scala.bson.BsonObjectId
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.{ClientSession, ReadConcern, ReadPreference, WriteConcern}
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{FlatSpec, Matchers}

class GridFSBucketSpec extends FlatSpec with Matchers with MockFactory {
  val wrapper = mock[JGridFSBucket]
  val clientSession = mock[ClientSession]
  val gridFSBucket = new GridFSBucket(wrapper)

  "GridFSBucket" should "have the same methods as the wrapped GridFSBucket" in {
    val wrapped = classOf[JGridFSBucket].getMethods.map(_.getName).toSet
    val local = classOf[GridFSBucket].getMethods.map(_.getName).toSet

    wrapped.foreach((name: String) => {
      val cleanedName = name.stripPrefix("get")
      assert(local.contains(name) | local.contains(cleanedName.head.toLower + cleanedName.tail), s"Missing: $name")
    })
  }

  it should "call the underlying methods to get bucket values" in {
    wrapper.expects(Symbol("getBucketName"))().once()
    wrapper.expects(Symbol("getDisableMD5"))().returning(false).once()
    wrapper.expects(Symbol("getChunkSizeBytes"))().returning(1).once()
    wrapper.expects(Symbol("getReadConcern"))().once()
    wrapper.expects(Symbol("getReadPreference"))().once()
    wrapper.expects(Symbol("getWriteConcern"))().once()

    gridFSBucket.bucketName
    gridFSBucket.disableMD5
    gridFSBucket.chunkSizeBytes
    gridFSBucket.readConcern
    gridFSBucket.readPreference
    gridFSBucket.writeConcern
  }

  it should "call the underlying methods to set bucket values" in {
    val chunkSizeInBytes = 1024 * 1024
    val readConcern = ReadConcern.MAJORITY
    val readPreference = ReadPreference.secondaryPreferred()
    val writeConcern = WriteConcern.W2

    wrapper.expects(Symbol("withDisableMD5"))(true).once()
    wrapper.expects(Symbol("withChunkSizeBytes"))(chunkSizeInBytes).once()
    wrapper.expects(Symbol("withReadConcern"))(readConcern).once()
    wrapper.expects(Symbol("withReadPreference"))(readPreference).once()
    wrapper.expects(Symbol("withWriteConcern"))(writeConcern).once()

    gridFSBucket.withDisableMD5(true)
    gridFSBucket.withChunkSizeBytes(chunkSizeInBytes)
    gridFSBucket.withReadConcern(readConcern)
    gridFSBucket.withReadPreference(readPreference)
    gridFSBucket.withWriteConcern(writeConcern)
  }

  it should "call the underlying delete method" in {
    val objectId = new ObjectId()
    val bsonValue = new BsonObjectId(objectId)

    wrapper.expects(Symbol("delete"))(objectId).once()
    wrapper.expects(Symbol("delete"))(bsonValue).once()
    wrapper.expects(Symbol("delete"))(clientSession, objectId).once()
    wrapper.expects(Symbol("delete"))(clientSession, bsonValue).once()

    gridFSBucket.delete(objectId)
    gridFSBucket.delete(bsonValue)
    gridFSBucket.delete(clientSession, objectId)
    gridFSBucket.delete(clientSession, bsonValue)
  }

  it should "call the underlying drop method" in {
    wrapper.expects(Symbol("drop"))().once()
    wrapper.expects(Symbol("drop"))(clientSession).once()

    gridFSBucket.drop()
    gridFSBucket.drop(clientSession)
  }

  it should "call the underlying rename method" in {
    val objectId = new ObjectId()
    val bsonValue = new BsonObjectId(objectId)
    val newName = "newName"

    wrapper.expects(Symbol("rename"))(objectId, newName).once()
    wrapper.expects(Symbol("rename"))(bsonValue, newName).once()
    wrapper.expects(Symbol("rename"))(clientSession, objectId, newName).once()
    wrapper.expects(Symbol("rename"))(clientSession, bsonValue, newName).once()

    gridFSBucket.rename(objectId, newName)
    gridFSBucket.rename(bsonValue, newName)
    gridFSBucket.rename(clientSession, objectId, newName)
    gridFSBucket.rename(clientSession, bsonValue, newName)
  }

  it should "return the expected findObservable" in {
    val filter = Document("{a: 1}")

    wrapper.expects(Symbol("find"))().once()
    wrapper.expects(Symbol("find"))(filter).once()
    wrapper.expects(Symbol("find"))(clientSession).once()
    wrapper.expects(Symbol("find"))(clientSession, filter).once()

    gridFSBucket.find()
    gridFSBucket.find(filter)
    gridFSBucket.find(clientSession)
    gridFSBucket.find(clientSession, filter)
  }

  it should "create the expected GridFSDownloadStream" in {
    val filename = "fileName"
    val options = new GridFSDownloadOptions()
    val objectId = new ObjectId()
    val bsonValue = new BsonObjectId(objectId)

    wrapper.expects(Symbol("openDownloadStream"))(filename)
    wrapper.expects(Symbol("openDownloadStream"))(filename, options)
    wrapper.expects(Symbol("openDownloadStream"))(objectId)
    wrapper.expects(Symbol("openDownloadStream"))(bsonValue)
    wrapper.expects(Symbol("openDownloadStream"))(clientSession, filename)
    wrapper.expects(Symbol("openDownloadStream"))(clientSession, filename, options)
    wrapper.expects(Symbol("openDownloadStream"))(clientSession, objectId)
    wrapper.expects(Symbol("openDownloadStream"))(clientSession, bsonValue)

    gridFSBucket.openDownloadStream(filename)
    gridFSBucket.openDownloadStream(filename, options)
    gridFSBucket.openDownloadStream(objectId)
    gridFSBucket.openDownloadStream(bsonValue)
    gridFSBucket.openDownloadStream(clientSession, filename)
    gridFSBucket.openDownloadStream(clientSession, filename, options)
    gridFSBucket.openDownloadStream(clientSession, objectId)
    gridFSBucket.openDownloadStream(clientSession, bsonValue)
  }

  it should "downloadToStream as expected" in {
    val filename = "fileName"
    val options = new GridFSDownloadOptions()
    val objectId = new ObjectId()
    val bsonValue = new BsonObjectId(objectId)
    val outputStream = mock[AsyncOutputStream]

    wrapper.expects(Symbol("downloadToStream"))(filename, *)
    wrapper.expects(Symbol("downloadToStream"))(filename, *, options)
    wrapper.expects(Symbol("downloadToStream"))(objectId, *)
    wrapper.expects(Symbol("downloadToStream"))(bsonValue, *)
    wrapper.expects(Symbol("downloadToStream"))(clientSession, filename, *)
    wrapper.expects(Symbol("downloadToStream"))(clientSession, filename, *, options)
    wrapper.expects(Symbol("downloadToStream"))(clientSession, objectId, *)
    wrapper.expects(Symbol("downloadToStream"))(clientSession, bsonValue, *)

    gridFSBucket.downloadToStream(filename, outputStream)
    gridFSBucket.downloadToStream(filename, outputStream, options)
    gridFSBucket.downloadToStream(objectId, outputStream)
    gridFSBucket.downloadToStream(bsonValue, outputStream)
    gridFSBucket.downloadToStream(clientSession, filename, outputStream)
    gridFSBucket.downloadToStream(clientSession, filename, outputStream, options)
    gridFSBucket.downloadToStream(clientSession, objectId, outputStream)
    gridFSBucket.downloadToStream(clientSession, bsonValue, outputStream)
  }

  it should "create the expected GridFSUploadStream" in {
    val filename = "fileName"
    val options = new GridFSUploadOptions()
    val bsonValue = new BsonObjectId()

    wrapper.expects(Symbol("openUploadStream"))(filename)
    wrapper.expects(Symbol("openUploadStream"))(filename, options)
    wrapper.expects(Symbol("openUploadStream"))(bsonValue, filename)
    wrapper.expects(Symbol("openUploadStream"))(bsonValue, filename, options)
    wrapper.expects(Symbol("openUploadStream"))(clientSession, filename)
    wrapper.expects(Symbol("openUploadStream"))(clientSession, filename, options)
    wrapper.expects(Symbol("openUploadStream"))(clientSession, bsonValue, filename)
    wrapper.expects(Symbol("openUploadStream"))(clientSession, bsonValue, filename, options)

    gridFSBucket.openUploadStream(filename)
    gridFSBucket.openUploadStream(filename, options)
    gridFSBucket.openUploadStream(bsonValue, filename)
    gridFSBucket.openUploadStream(bsonValue, filename, options)
    gridFSBucket.openUploadStream(clientSession, filename)
    gridFSBucket.openUploadStream(clientSession, filename, options)
    gridFSBucket.openUploadStream(clientSession, bsonValue, filename)
    gridFSBucket.openUploadStream(clientSession, bsonValue, filename, options)
  }

  it should "uploadFromStream as expected" in {
    val filename = "fileName"
    val options = new GridFSUploadOptions()
    val bsonValue = new BsonObjectId()
    val inputStream = mock[AsyncInputStream]

    wrapper.expects(Symbol("uploadFromStream"))(filename, *)
    wrapper.expects(Symbol("uploadFromStream"))(filename, *, options)
    wrapper.expects(Symbol("uploadFromStream"))(bsonValue, filename, *)
    wrapper.expects(Symbol("uploadFromStream"))(bsonValue, filename, *, options)
    wrapper.expects(Symbol("uploadFromStream"))(clientSession, filename, *)
    wrapper.expects(Symbol("uploadFromStream"))(clientSession, filename, *, options)
    wrapper.expects(Symbol("uploadFromStream"))(clientSession, bsonValue, filename, *)
    wrapper.expects(Symbol("uploadFromStream"))(clientSession, bsonValue, filename, *, options)

    gridFSBucket.uploadFromStream(filename, inputStream)
    gridFSBucket.uploadFromStream(filename, inputStream, options)
    gridFSBucket.uploadFromStream(bsonValue, filename, inputStream)
    gridFSBucket.uploadFromStream(bsonValue, filename, inputStream, options)
    gridFSBucket.uploadFromStream(clientSession, filename, inputStream)
    gridFSBucket.uploadFromStream(clientSession, filename, inputStream, options)
    gridFSBucket.uploadFromStream(clientSession, bsonValue, filename, inputStream)
    gridFSBucket.uploadFromStream(clientSession, bsonValue, filename, inputStream, options)
  }

}
