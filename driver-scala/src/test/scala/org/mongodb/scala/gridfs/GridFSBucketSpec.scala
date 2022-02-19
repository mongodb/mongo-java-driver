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
import org.mockito.Mockito.{ verify, when }
import org.mongodb.scala.bson.BsonObjectId
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.{ BaseSpec, ClientSession, Observable, ReadConcern, ReadPreference, WriteConcern }
import org.scalatestplus.mockito.MockitoSugar

class GridFSBucketSpec extends BaseSpec with MockitoSugar {
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
    gridFSBucket.bucketName
    gridFSBucket.chunkSizeBytes
    gridFSBucket.readConcern
    gridFSBucket.readPreference
    gridFSBucket.writeConcern

    verify(wrapper).getBucketName
    when(wrapper.getChunkSizeBytes).thenReturn(1)
    verify(wrapper).getReadConcern
    verify(wrapper).getReadPreference
    verify(wrapper).getWriteConcern
  }

  it should "call the underlying methods to set bucket values" in {
    val chunkSizeInBytes = 1024 * 1024
    val readConcern = ReadConcern.MAJORITY
    val readPreference = ReadPreference.secondaryPreferred()
    val writeConcern = WriteConcern.W2

    gridFSBucket.withChunkSizeBytes(chunkSizeInBytes)
    gridFSBucket.withReadConcern(readConcern)
    gridFSBucket.withReadPreference(readPreference)
    gridFSBucket.withWriteConcern(writeConcern)

    verify(wrapper).withChunkSizeBytes(chunkSizeInBytes)
    verify(wrapper).withReadConcern(readConcern)
    verify(wrapper).withReadPreference(readPreference)
    verify(wrapper).withWriteConcern(writeConcern)
  }

  it should "call the underlying delete method" in {
    val bsonValue = BsonObjectId()
    val objectId = bsonValue.getValue

    gridFSBucket.delete(objectId)
    gridFSBucket.delete(bsonValue)
    gridFSBucket.delete(clientSession, objectId)
    gridFSBucket.delete(clientSession, bsonValue)

    verify(wrapper).delete(objectId)
    verify(wrapper).delete(bsonValue)
    verify(wrapper).delete(clientSession, objectId)
    verify(wrapper).delete(clientSession, bsonValue)
  }

  it should "call the underlying drop method" in {
    gridFSBucket.drop()
    gridFSBucket.drop(clientSession)

    verify(wrapper).drop()
    verify(wrapper).drop(clientSession)
  }

  it should "call the underlying rename method" in {
    val bsonValue = BsonObjectId()
    val objectId = bsonValue.getValue
    val newName = "newName"

    gridFSBucket.rename(objectId, newName)
    gridFSBucket.rename(bsonValue, newName)
    gridFSBucket.rename(clientSession, objectId, newName)
    gridFSBucket.rename(clientSession, bsonValue, newName)

    verify(wrapper).rename(objectId, newName)
    verify(wrapper).rename(bsonValue, newName)
    verify(wrapper).rename(clientSession, objectId, newName)
    verify(wrapper).rename(clientSession, bsonValue, newName)
  }

  it should "return the expected findObservable" in {
    val filter = Document("{a: 1}")

    gridFSBucket.find()
    gridFSBucket.find(filter)
    gridFSBucket.find(clientSession)
    gridFSBucket.find(clientSession, filter)

    verify(wrapper).find()
    verify(wrapper).find(filter)
    verify(wrapper).find(clientSession)
    verify(wrapper).find(clientSession, filter)
  }

  it should "return the expected GridFSDownloadObservable" in {
    val fileName = "myFile"
    val bsonValue = BsonObjectId()
    val objectId = bsonValue.getValue
    val options = new GridFSDownloadOptions()
    val clientSession = mock[ClientSession]

    gridFSBucket.downloadToObservable(objectId)
    gridFSBucket.downloadToObservable(bsonValue)
    gridFSBucket.downloadToObservable(fileName)
    gridFSBucket.downloadToObservable(fileName, options)

    verify(wrapper).downloadToPublisher(objectId)
    verify(wrapper).downloadToPublisher(bsonValue)
    verify(wrapper).downloadToPublisher(fileName)
    verify(wrapper).downloadToPublisher(fileName, options)

    gridFSBucket.downloadToObservable(clientSession, objectId)
    gridFSBucket.downloadToObservable(clientSession, bsonValue)
    gridFSBucket.downloadToObservable(clientSession, fileName)
    gridFSBucket.downloadToObservable(clientSession, fileName, options)

    verify(wrapper).downloadToPublisher(clientSession, objectId)
    verify(wrapper).downloadToPublisher(clientSession, bsonValue)
    verify(wrapper).downloadToPublisher(clientSession, fileName)
    verify(wrapper).downloadToPublisher(clientSession, fileName, options)

  }

  it should "return the expected GridFSUploadObservable" in {
    val publisher = Observable(Seq(ByteBuffer.wrap("123".getBytes)))
    val fileName = "myFile"
    val bsonValue = BsonObjectId()
    val options = new GridFSUploadOptions()
    val clientSession = mock[ClientSession]

    gridFSBucket.uploadFromObservable(fileName, publisher)
    gridFSBucket.uploadFromObservable(fileName, publisher, options)
    gridFSBucket.uploadFromObservable(bsonValue, fileName, publisher)
    gridFSBucket.uploadFromObservable(bsonValue, fileName, publisher, options)

    verify(wrapper).uploadFromPublisher(fileName, publisher)
    verify(wrapper).uploadFromPublisher(fileName, publisher, options)
    verify(wrapper).uploadFromPublisher(bsonValue, fileName, publisher)
    verify(wrapper).uploadFromPublisher(bsonValue, fileName, publisher, options)

    gridFSBucket.uploadFromObservable(clientSession, fileName, publisher)
    gridFSBucket.uploadFromObservable(clientSession, fileName, publisher, options)
    gridFSBucket.uploadFromObservable(clientSession, bsonValue, fileName, publisher)
    gridFSBucket.uploadFromObservable(clientSession, bsonValue, fileName, publisher, options)

    verify(wrapper).uploadFromPublisher(clientSession, fileName, publisher)
    verify(wrapper).uploadFromPublisher(clientSession, fileName, publisher, options)
    verify(wrapper).uploadFromPublisher(clientSession, bsonValue, fileName, publisher)
    verify(wrapper).uploadFromPublisher(clientSession, bsonValue, fileName, publisher, options)
  }

}
