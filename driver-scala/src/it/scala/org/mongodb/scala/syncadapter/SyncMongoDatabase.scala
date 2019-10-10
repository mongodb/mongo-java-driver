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
package org.mongodb.scala.syncadapter

import com.mongodb.{ ReadConcern, ReadPreference, WriteConcern }
import com.mongodb.client.model.{ CreateCollectionOptions, CreateViewOptions }
import com.mongodb.client.{ ClientSession, MongoDatabase => JMongoDatabase }
import org.bson.Document
import org.bson.codecs.configuration.CodecRegistry
import org.bson.conversions.Bson
import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.bson.DefaultHelper.DefaultsTo

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

case class SyncMongoDatabase(wrapped: MongoDatabase) extends JMongoDatabase {

  override def getName: String = wrapped.name

  override def getCodecRegistry: CodecRegistry = wrapped.codecRegistry

  override def getReadPreference: ReadPreference = wrapped.readPreference

  override def getWriteConcern: WriteConcern = wrapped.writeConcern

  override def getReadConcern: ReadConcern = wrapped.readConcern

  override def withCodecRegistry(codecRegistry: CodecRegistry) =
    SyncMongoDatabase(wrapped.withCodecRegistry(codecRegistry))

  override def withReadPreference(readPreference: ReadPreference) = throw new UnsupportedOperationException

  override def withWriteConcern(writeConcern: WriteConcern) = throw new UnsupportedOperationException

  override def withReadConcern(readConcern: ReadConcern) = throw new UnsupportedOperationException

  override def getCollection(collectionName: String) =
    SyncMongoCollection[Document](wrapped.getCollection(collectionName))

  override def getCollection[TDocument](collectionName: String, documentClass: Class[TDocument]) =
    SyncMongoCollection[TDocument](
      wrapped.getCollection[TDocument](collectionName)(
        DefaultsTo.overrideDefault[TDocument, org.mongodb.scala.Document],
        ClassTag(documentClass)
      )
    )

  override def runCommand(command: Bson): Document = wrapped.runCommand(command).toFuture().get()

  override def runCommand(command: Bson, readPreference: ReadPreference): Document =
    wrapped.runCommand(command, readPreference).toFuture().get()

  override def runCommand[TResult](command: Bson, resultClass: Class[TResult]): TResult =
    wrapped
      .runCommand[TResult](command)(
        DefaultsTo.overrideDefault[TResult, org.mongodb.scala.Document],
        ClassTag(resultClass)
      )
      .toFuture()
      .get()

  override def runCommand[TResult](
      command: Bson,
      readPreference: ReadPreference,
      resultClass: Class[TResult]
  ): TResult =
    wrapped
      .runCommand[TResult](command, readPreference)(
        DefaultsTo.overrideDefault[TResult, org.mongodb.scala.Document],
        ClassTag(resultClass)
      )
      .toFuture()
      .get()

  override def runCommand(clientSession: ClientSession, command: Bson): Document =
    wrapped.runCommand[Document](unwrap(clientSession), command).toFuture().get()

  override def runCommand(clientSession: ClientSession, command: Bson, readPreference: ReadPreference): Document =
    wrapped.runCommand[Document](unwrap(clientSession), command, readPreference).toFuture().get()

  override def runCommand[TResult](clientSession: ClientSession, command: Bson, resultClass: Class[TResult]): TResult =
    wrapped
      .runCommand[TResult](unwrap(clientSession), command)(
        DefaultsTo.overrideDefault[TResult, org.mongodb.scala.Document],
        ClassTag(resultClass)
      )
      .toFuture()
      .get()

  override def runCommand[TResult](
      clientSession: ClientSession,
      command: Bson,
      readPreference: ReadPreference,
      resultClass: Class[TResult]
  ): TResult =
    wrapped
      .runCommand[TResult](unwrap(clientSession), command, readPreference)(
        DefaultsTo.overrideDefault[TResult, org.mongodb.scala.Document],
        ClassTag(resultClass)
      )
      .toFuture()
      .get()

  override def drop(): Unit = wrapped.drop().toFuture().get()

  override def drop(clientSession: ClientSession): Unit = wrapped.drop(unwrap(clientSession)).toFuture().get()

  override def listCollectionNames = throw new UnsupportedOperationException

  override def listCollections = new SyncListCollectionsIterable[Document](wrapped.listCollections[Document]())

  override def listCollections[TResult](resultClass: Class[TResult]) =
    new SyncListCollectionsIterable[TResult](
      wrapped.listCollections[TResult]()(
        DefaultsTo.overrideDefault[TResult, org.mongodb.scala.Document],
        ClassTag(resultClass)
      )
    )

  override def listCollectionNames(clientSession: ClientSession) = throw new UnsupportedOperationException

  override def listCollections(clientSession: ClientSession) = throw new UnsupportedOperationException

  override def listCollections[TResult](clientSession: ClientSession, resultClass: Class[TResult]) =
    throw new UnsupportedOperationException

  override def createCollection(collectionName: String): Unit = {
    throw new UnsupportedOperationException
  }

  override def createCollection(collectionName: String, createCollectionOptions: CreateCollectionOptions): Unit = {
    throw new UnsupportedOperationException
  }

  override def createCollection(clientSession: ClientSession, collectionName: String): Unit = {
    throw new UnsupportedOperationException
  }

  override def createCollection(
      clientSession: ClientSession,
      collectionName: String,
      createCollectionOptions: CreateCollectionOptions
  ): Unit = {
    throw new UnsupportedOperationException
  }

  override def createView(viewName: String, viewOn: String, pipeline: java.util.List[_ <: Bson]): Unit = {
    throw new UnsupportedOperationException
  }

  override def createView(
      viewName: String,
      viewOn: String,
      pipeline: java.util.List[_ <: Bson],
      createViewOptions: CreateViewOptions
  ): Unit = {
    throw new UnsupportedOperationException
  }

  override def createView(
      clientSession: ClientSession,
      viewName: String,
      viewOn: String,
      pipeline: java.util.List[_ <: Bson]
  ): Unit = {
    throw new UnsupportedOperationException
  }

  override def createView(
      clientSession: ClientSession,
      viewName: String,
      viewOn: String,
      pipeline: java.util.List[_ <: Bson],
      createViewOptions: CreateViewOptions
  ): Unit = {
    throw new UnsupportedOperationException
  }

  override def watch = new SyncChangeStreamIterable[Document](wrapped.watch[Document]())

  override def watch[TResult](resultClass: Class[TResult]) =
    new SyncChangeStreamIterable[TResult](
      wrapped.watch[TResult]()(DefaultsTo.overrideDefault[TResult, org.mongodb.scala.Document], ClassTag(resultClass))
    )

  override def watch(pipeline: java.util.List[_ <: Bson]) =
    new SyncChangeStreamIterable[Document](wrapped.watch[Document](pipeline.asScala.toSeq))

  override def watch[TResult](pipeline: java.util.List[_ <: Bson], resultClass: Class[TResult]) =
    new SyncChangeStreamIterable[TResult](
      wrapped.watch(pipeline.asScala.toSeq)(
        DefaultsTo.overrideDefault[TResult, org.mongodb.scala.Document],
        ClassTag(resultClass)
      )
    )

  override def watch(clientSession: ClientSession) =
    new SyncChangeStreamIterable[Document](wrapped.watch(unwrap(clientSession)))

  override def watch[TResult](clientSession: ClientSession, resultClass: Class[TResult]) =
    new SyncChangeStreamIterable[TResult](
      wrapped.watch(unwrap(clientSession))(
        DefaultsTo.overrideDefault[TResult, org.mongodb.scala.Document],
        ClassTag(resultClass)
      )
    )

  override def watch(clientSession: ClientSession, pipeline: java.util.List[_ <: Bson]) =
    new SyncChangeStreamIterable[Document](wrapped.watch(unwrap(clientSession), pipeline.asScala.toSeq))

  override def watch[TResult](
      clientSession: ClientSession,
      pipeline: java.util.List[_ <: Bson],
      resultClass: Class[TResult]
  ) =
    new SyncChangeStreamIterable[TResult](
      wrapped.watch[TResult](unwrap(clientSession), pipeline.asScala.toSeq)(
        DefaultsTo.overrideDefault[TResult, org.mongodb.scala.Document],
        ClassTag(resultClass)
      )
    )

  override def aggregate(pipeline: java.util.List[_ <: Bson]) =
    new SyncAggregateIterable[Document](wrapped.aggregate(pipeline.asScala.toSeq))

  override def aggregate[TResult](pipeline: java.util.List[_ <: Bson], resultClass: Class[TResult]) =
    new SyncAggregateIterable[TResult](
      wrapped.aggregate[TResult](pipeline.asScala.toSeq)(
        DefaultsTo.overrideDefault[TResult, org.mongodb.scala.Document],
        ClassTag(resultClass)
      )
    )

  override def aggregate(clientSession: ClientSession, pipeline: java.util.List[_ <: Bson]) =
    new SyncAggregateIterable[Document](wrapped.aggregate(unwrap(clientSession), pipeline.asScala.toSeq))

  override def aggregate[TResult](
      clientSession: ClientSession,
      pipeline: java.util.List[_ <: Bson],
      resultClass: Class[TResult]
  ) =
    new SyncAggregateIterable[TResult](
      wrapped.aggregate[TResult](unwrap(clientSession), pipeline.asScala.toSeq)(
        DefaultsTo.overrideDefault[TResult, org.mongodb.scala.Document],
        ClassTag(resultClass)
      )
    )

  private def unwrap(clientSession: ClientSession) = clientSession.asInstanceOf[SyncClientSession].wrapped

}
