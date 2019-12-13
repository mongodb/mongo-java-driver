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

import java.nio.ByteBuffer

import com.mongodb.reactivestreams.client.gridfs.{ GridFSBucket => JGridFSBucket }
import org.mongodb.scala.bson.BsonObjectId
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.{ BaseSpec, ClientSession, Observable, ReadConcern, ReadPreference, WriteConcern }
import org.scalamock.scalatest.proxy.MockFactory

class GridFSBucketSpec extends BaseSpec with MockFactory {
  val wrapper = mock[JGridFSBucket]
  val clientSession = mock[ClientSession]
  val gridFSBucket = new GridFSBucket(wrapper)

  "GridFSBucket" should "have the same methods as the wrapped GridFSBucket" in {
    val wrapped = classOf[JGridFSBucket].getMethods.map(_.getName).toSet
    val local = classOf[GridFSBucket].getMethods.map(_.getName).toSet

    wrapped.foreach((name: String) => {
      val cleanedName = name.stripPrefix("get").replace("Publisher", "Observable")
      assert(local.contains(name) | local.contains(cleanedName.head.toLower + cleanedName.tail), s"Missing: $name")
    })
  }

  it should "call the underlying methods to get bucket values" in {
    wrapper.expects(Symbol("getBucketName"))().once()
    wrapper.expects(Symbol("getChunkSizeBytes"))().returning(1).once()
    wrapper.expects(Symbol("getReadConcern"))().once()
    wrapper.expects(Symbol("getReadPreference"))().once()
    wrapper.expects(Symbol("getWriteConcern"))().once()

    gridFSBucket.bucketName
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

    wrapper.expects(Symbol("withChunkSizeBytes"))(chunkSizeInBytes).once()
    wrapper.expects(Symbol("withReadConcern"))(readConcern).once()
    wrapper.expects(Symbol("withReadPreference"))(readPreference).once()
    wrapper.expects(Symbol("withWriteConcern"))(writeConcern).once()

    gridFSBucket.withChunkSizeBytes(chunkSizeInBytes)
    gridFSBucket.withReadConcern(readConcern)
    gridFSBucket.withReadPreference(readPreference)
    gridFSBucket.withWriteConcern(writeConcern)
  }

  it should "call the underlying delete method" in {
    val bsonValue = BsonObjectId()
    val objectId = bsonValue.getValue

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
    val bsonValue = BsonObjectId()
    val objectId = bsonValue.getValue
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

  it should "return the expected GridFSDownloadObservable" in {
    val fileName = "myFile"
    val bsonValue = BsonObjectId()
    val objectId = bsonValue.getValue
    val options = new GridFSDownloadOptions()
    val clientSession = mock[ClientSession]

    wrapper.expects(Symbol("downloadToPublisher"))(objectId).once()
    wrapper.expects(Symbol("downloadToPublisher"))(bsonValue).once()
    wrapper.expects(Symbol("downloadToPublisher"))(fileName).once()
    wrapper.expects(Symbol("downloadToPublisher"))(fileName, options).once()

    gridFSBucket.downloadToObservable(objectId)
    gridFSBucket.downloadToObservable(bsonValue)
    gridFSBucket.downloadToObservable(fileName)
    gridFSBucket.downloadToObservable(fileName, options)

    wrapper.expects(Symbol("downloadToPublisher"))(clientSession, objectId).once()
    wrapper.expects(Symbol("downloadToPublisher"))(clientSession, bsonValue).once()
    wrapper.expects(Symbol("downloadToPublisher"))(clientSession, fileName).once()
    wrapper.expects(Symbol("downloadToPublisher"))(clientSession, fileName, options).once()

    gridFSBucket.downloadToObservable(clientSession, objectId)
    gridFSBucket.downloadToObservable(clientSession, bsonValue)
    gridFSBucket.downloadToObservable(clientSession, fileName)
    gridFSBucket.downloadToObservable(clientSession, fileName, options)

  }

  it should "return the expected GridFSUploadObservable" in {
    val publisher = Observable(Seq(ByteBuffer.wrap("123".getBytes)))
    val fileName = "myFile"
    val bsonValue = BsonObjectId()
    val options = new GridFSUploadOptions()
    val clientSession = mock[ClientSession]

    wrapper.expects(Symbol("uploadFromPublisher"))(fileName, publisher).once()
    wrapper.expects(Symbol("uploadFromPublisher"))(fileName, publisher, options).once()
    wrapper.expects(Symbol("uploadFromPublisher"))(bsonValue, fileName, publisher).once()
    wrapper.expects(Symbol("uploadFromPublisher"))(bsonValue, fileName, publisher, options).once()

    gridFSBucket.uploadFromObservable(fileName, publisher)
    gridFSBucket.uploadFromObservable(fileName, publisher, options)
    gridFSBucket.uploadFromObservable(bsonValue, fileName, publisher)
    gridFSBucket.uploadFromObservable(bsonValue, fileName, publisher, options)

    wrapper.expects(Symbol("uploadFromPublisher"))(clientSession, fileName, publisher).once()
    wrapper.expects(Symbol("uploadFromPublisher"))(clientSession, fileName, publisher, options).once()
    wrapper.expects(Symbol("uploadFromPublisher"))(clientSession, bsonValue, fileName, publisher).once()
    wrapper.expects(Symbol("uploadFromPublisher"))(clientSession, bsonValue, fileName, publisher, options).once()

    gridFSBucket.uploadFromObservable(clientSession, fileName, publisher)
    gridFSBucket.uploadFromObservable(clientSession, fileName, publisher, options)
    gridFSBucket.uploadFromObservable(clientSession, bsonValue, fileName, publisher)
    gridFSBucket.uploadFromObservable(clientSession, bsonValue, fileName, publisher, options)
  }

}
