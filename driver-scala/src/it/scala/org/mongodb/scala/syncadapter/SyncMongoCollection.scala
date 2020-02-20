/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb.scala.syncadapter

import com.mongodb.bulk.BulkWriteResult
import com.mongodb.client.model._
import com.mongodb.client.result.{ DeleteResult, UpdateResult }
import com.mongodb.client.{ ChangeStreamIterable, ClientSession, MongoCollection => JMongoCollection }
import com.mongodb.{ MongoNamespace, ReadConcern, ReadPreference, WriteConcern }
import org.bson.codecs.configuration.CodecRegistry
import org.bson.conversions.Bson
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.bson.DefaultHelper.DefaultsTo
import org.mongodb.scala.result.{ InsertManyResult, InsertOneResult }

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

case class SyncMongoCollection[T](wrapped: MongoCollection[T]) extends JMongoCollection[T] {

  private def unwrap(clientSession: ClientSession) = clientSession.asInstanceOf[SyncClientSession].wrapped

  override def getNamespace: MongoNamespace = wrapped.namespace

  override def getDocumentClass: Class[T] = wrapped.documentClass

  override def getCodecRegistry: CodecRegistry = wrapped.codecRegistry

  override def getReadPreference: ReadPreference = wrapped.readPreference

  override def getWriteConcern: WriteConcern = wrapped.writeConcern

  override def getReadConcern: ReadConcern = wrapped.readConcern

  override def withDocumentClass[NewTDocument](clazz: Class[NewTDocument]): JMongoCollection[NewTDocument] =
    SyncMongoCollection[NewTDocument](
      wrapped.withDocumentClass[NewTDocument]()(
        DefaultsTo.overrideDefault[NewTDocument, org.mongodb.scala.Document],
        ClassTag(clazz)
      )
    )

  override def withCodecRegistry(codecRegistry: CodecRegistry): JMongoCollection[T] =
    SyncMongoCollection[T](wrapped.withCodecRegistry(codecRegistry))

  override def withReadPreference(readPreference: ReadPreference): JMongoCollection[T] =
    SyncMongoCollection[T](wrapped.withReadPreference(readPreference))

  override def withWriteConcern(writeConcern: WriteConcern): JMongoCollection[T] =
    SyncMongoCollection[T](wrapped.withWriteConcern(writeConcern))

  override def withReadConcern(readConcern: ReadConcern): JMongoCollection[T] =
    SyncMongoCollection[T](wrapped.withReadConcern(readConcern))

  override def countDocuments: Long = wrapped.countDocuments().toFuture().get()

  override def countDocuments(filter: Bson): Long = wrapped.countDocuments(filter).toFuture().get()

  override def countDocuments(filter: Bson, options: CountOptions): Long =
    wrapped.countDocuments(filter, options).toFuture().get()

  override def countDocuments(clientSession: ClientSession): Long =
    wrapped.countDocuments(unwrap(clientSession)).toFuture().get()

  override def countDocuments(clientSession: ClientSession, filter: Bson): Long =
    wrapped.countDocuments(unwrap(clientSession), filter).toFuture().get()

  override def countDocuments(clientSession: ClientSession, filter: Bson, options: CountOptions): Long =
    wrapped.countDocuments(unwrap(clientSession), filter, options).toFuture().get()

  override def estimatedDocumentCount: Long = wrapped.estimatedDocumentCount().toFuture().get()

  override def estimatedDocumentCount(options: EstimatedDocumentCountOptions): Long =
    wrapped.estimatedDocumentCount(options).toFuture().get()

  override def distinct[TResult](fieldName: String, resultClass: Class[TResult]) =
    SyncDistinctIterable[TResult](wrapped.distinct[TResult](fieldName)(ClassTag(resultClass)))

  override def distinct[TResult](fieldName: String, filter: Bson, resultClass: Class[TResult]) =
    SyncDistinctIterable[TResult](wrapped.distinct[TResult](fieldName, filter)(ClassTag(resultClass)))

  override def distinct[TResult](clientSession: ClientSession, fieldName: String, resultClass: Class[TResult]) =
    SyncDistinctIterable[TResult](wrapped.distinct[TResult](unwrap(clientSession), fieldName)(ClassTag(resultClass)))

  override def distinct[TResult](
      clientSession: ClientSession,
      fieldName: String,
      filter: Bson,
      resultClass: Class[TResult]
  ) =
    SyncDistinctIterable[TResult](
      wrapped.distinct[TResult](unwrap(clientSession), fieldName, filter)(ClassTag(resultClass))
    )

  override def find = SyncFindIterable[T](wrapped.find[T]()(DefaultsTo.default[T], ClassTag(getDocumentClass)))

  override def find[TResult](resultClass: Class[TResult]) =
    SyncFindIterable[TResult](wrapped.find[TResult]()(DefaultsTo.overrideDefault[TResult, T], ClassTag(resultClass)))

  override def find(filter: Bson) =
    SyncFindIterable[T](wrapped.find(filter)(DefaultsTo.default[T], ClassTag(getDocumentClass)))

  override def find[TResult](filter: Bson, resultClass: Class[TResult]) =
    SyncFindIterable[TResult](
      wrapped.find[TResult](filter)(DefaultsTo.overrideDefault[TResult, T], ClassTag(resultClass))
    )

  override def find(clientSession: ClientSession) =
    SyncFindIterable[T](wrapped.find[T](unwrap(clientSession))(DefaultsTo.default[T], ClassTag(getDocumentClass)))

  override def find[TResult](clientSession: ClientSession, resultClass: Class[TResult]) =
    SyncFindIterable[TResult](
      wrapped.find[TResult](unwrap(clientSession))(DefaultsTo.overrideDefault[TResult, T], ClassTag(resultClass))
    )

  override def find(clientSession: ClientSession, filter: Bson) =
    SyncFindIterable[T](
      wrapped.find[T](unwrap(clientSession), filter)(DefaultsTo.default[T], ClassTag(getDocumentClass))
    )

  override def find[TResult](clientSession: ClientSession, filter: Bson, resultClass: Class[TResult]) =
    SyncFindIterable[TResult](
      wrapped
        .find[TResult](unwrap(clientSession), filter)(DefaultsTo.overrideDefault[TResult, T], ClassTag(resultClass))
    )

  override def aggregate(pipeline: java.util.List[_ <: Bson]) =
    SyncAggregateIterable[T](
      wrapped.aggregate(pipeline.asScala.toSeq)(DefaultsTo.default[T], ClassTag(getDocumentClass))
    )

  override def aggregate[TResult](pipeline: java.util.List[_ <: Bson], resultClass: Class[TResult]) =
    SyncAggregateIterable[TResult](
      wrapped.aggregate[TResult](pipeline.asScala.toSeq)(DefaultsTo.overrideDefault[TResult, T], ClassTag(resultClass))
    )

  override def aggregate(clientSession: ClientSession, pipeline: java.util.List[_ <: Bson]) =
    SyncAggregateIterable[T](
      wrapped
        .aggregate[T](unwrap(clientSession), pipeline.asScala.toSeq)(DefaultsTo.default[T], ClassTag(getDocumentClass))
    )

  override def aggregate[TResult](
      clientSession: ClientSession,
      pipeline: java.util.List[_ <: Bson],
      resultClass: Class[TResult]
  ) =
    SyncAggregateIterable[TResult](
      wrapped.aggregate[TResult](unwrap(clientSession), pipeline.asScala.toSeq)(
        DefaultsTo.overrideDefault[TResult, T],
        ClassTag(resultClass)
      )
    )

  override def watch =
    SyncChangeStreamIterable[T](wrapped.watch[T]()(DefaultsTo.default[T], ClassTag(getDocumentClass)))

  override def watch[TResult](resultClass: Class[TResult]) =
    SyncChangeStreamIterable[TResult](
      wrapped.watch[TResult]()(DefaultsTo.overrideDefault[TResult, T], ClassTag(resultClass))
    )

  override def watch(pipeline: java.util.List[_ <: Bson]) =
    SyncChangeStreamIterable[T](
      wrapped.watch[T](pipeline.asScala.toSeq)(DefaultsTo.default[T], ClassTag(getDocumentClass))
    )

  override def watch[TResult](pipeline: java.util.List[_ <: Bson], resultClass: Class[TResult]) =
    SyncChangeStreamIterable[TResult](
      wrapped.watch[TResult](pipeline.asScala.toSeq)(DefaultsTo.overrideDefault[TResult, T], ClassTag(resultClass))
    )

  override def watch(clientSession: ClientSession) =
    SyncChangeStreamIterable[T](
      wrapped.watch[T](unwrap(clientSession))(DefaultsTo.default[T], ClassTag(getDocumentClass))
    )

  override def watch[TResult](clientSession: ClientSession, resultClass: Class[TResult]) =
    SyncChangeStreamIterable[TResult](
      wrapped.watch[TResult](unwrap(clientSession))(DefaultsTo.overrideDefault[TResult, T], ClassTag(resultClass))
    )

  override def watch(clientSession: ClientSession, pipeline: java.util.List[_ <: Bson]): ChangeStreamIterable[T] =
    SyncChangeStreamIterable[T](
      wrapped.watch[T](unwrap(clientSession), pipeline.asScala.toSeq)(DefaultsTo.default[T], ClassTag(getDocumentClass))
    )

  override def watch[TResult](
      clientSession: ClientSession,
      pipeline: java.util.List[_ <: Bson],
      resultClass: Class[TResult]
  ) =
    SyncChangeStreamIterable[TResult](
      wrapped.watch(unwrap(clientSession), pipeline.asScala.toSeq)(
        DefaultsTo.overrideDefault[TResult, T],
        ClassTag(resultClass)
      )
    )

  override def mapReduce(mapFunction: String, reduceFunction: String) =
    SyncMapReduceIterable[T](
      wrapped.mapReduce[T](mapFunction, reduceFunction)(DefaultsTo.default[T], ClassTag(getDocumentClass))
    )

  override def mapReduce[TResult](mapFunction: String, reduceFunction: String, resultClass: Class[TResult]) =
    SyncMapReduceIterable[TResult](
      wrapped
        .mapReduce[TResult](mapFunction, reduceFunction)(DefaultsTo.overrideDefault[TResult, T], ClassTag(resultClass))
    )

  override def mapReduce(clientSession: ClientSession, mapFunction: String, reduceFunction: String) =
    SyncMapReduceIterable[T](
      wrapped.mapReduce[T](unwrap(clientSession), mapFunction, reduceFunction)(
        DefaultsTo.default[T],
        ClassTag(getDocumentClass)
      )
    )

  override def mapReduce[TResult](
      clientSession: ClientSession,
      mapFunction: String,
      reduceFunction: String,
      resultClass: Class[TResult]
  ) =
    SyncMapReduceIterable[TResult](
      wrapped.mapReduce[TResult](unwrap(clientSession), mapFunction, reduceFunction)(
        DefaultsTo.overrideDefault[TResult, T],
        ClassTag(resultClass)
      )
    )

  override def bulkWrite(requests: java.util.List[_ <: WriteModel[_ <: T]]): BulkWriteResult =
    wrapped.bulkWrite(requests.asScala.toSeq).toFuture().get()

  override def bulkWrite(
      requests: java.util.List[_ <: WriteModel[_ <: T]],
      options: BulkWriteOptions
  ): BulkWriteResult = wrapped.bulkWrite(requests.asScala.toSeq, options).toFuture().get()

  override def bulkWrite(
      clientSession: ClientSession,
      requests: java.util.List[_ <: WriteModel[_ <: T]]
  ): BulkWriteResult =
    wrapped.bulkWrite(unwrap(clientSession), requests.asScala.toSeq).toFuture().get()

  override def bulkWrite(
      clientSession: ClientSession,
      requests: java.util.List[_ <: WriteModel[_ <: T]],
      options: BulkWriteOptions
  ): BulkWriteResult =
    wrapped.bulkWrite(unwrap(clientSession), requests.asScala.toSeq, options).toFuture().get()

  override def insertOne(t: T): InsertOneResult = wrapped.insertOne(t).toFuture().get()

  override def insertOne(t: T, options: InsertOneOptions): InsertOneResult =
    wrapped.insertOne(t, options).toFuture().get()

  override def insertOne(clientSession: ClientSession, t: T): InsertOneResult =
    wrapped.insertOne(unwrap(clientSession), t).toFuture().get()

  override def insertOne(clientSession: ClientSession, t: T, options: InsertOneOptions): InsertOneResult =
    wrapped.insertOne(unwrap(clientSession), t, options).toFuture().get()

  override def insertMany(documents: java.util.List[_ <: T]): InsertManyResult =
    wrapped.insertMany(documents.asScala.toSeq).toFuture().get()

  override def insertMany(documents: java.util.List[_ <: T], options: InsertManyOptions): InsertManyResult =
    wrapped.insertMany(documents.asScala.toSeq, options).toFuture().get()

  override def insertMany(clientSession: ClientSession, documents: java.util.List[_ <: T]): InsertManyResult =
    wrapped.insertMany(unwrap(clientSession), documents.asScala.toSeq).toFuture().get()

  override def insertMany(
      clientSession: ClientSession,
      documents: java.util.List[_ <: T],
      options: InsertManyOptions
  ): InsertManyResult =
    wrapped.insertMany(unwrap(clientSession), documents.asScala.toSeq, options).toFuture().get()

  override def deleteOne(filter: Bson): DeleteResult =
    wrapped.deleteOne(filter).toFuture().get()

  override def deleteOne(filter: Bson, options: DeleteOptions): DeleteResult =
    wrapped.deleteOne(filter, options).toFuture().get()

  override def deleteOne(clientSession: ClientSession, filter: Bson): DeleteResult =
    wrapped.deleteOne(unwrap(clientSession), filter).toFuture().get()

  override def deleteOne(clientSession: ClientSession, filter: Bson, options: DeleteOptions): DeleteResult =
    wrapped.deleteOne(unwrap(clientSession), filter, options).toFuture().get()

  override def deleteMany(filter: Bson): DeleteResult =
    wrapped.deleteMany(filter).toFuture().get()

  override def deleteMany(filter: Bson, options: DeleteOptions): DeleteResult =
    wrapped.deleteMany(filter, options).toFuture().get()

  override def deleteMany(clientSession: ClientSession, filter: Bson): DeleteResult =
    wrapped.deleteMany(unwrap(clientSession), filter).toFuture().get()

  override def deleteMany(clientSession: ClientSession, filter: Bson, options: DeleteOptions): DeleteResult =
    wrapped.deleteMany(unwrap(clientSession), filter, options).toFuture().get()

  override def replaceOne(filter: Bson, replacement: T): UpdateResult =
    wrapped.replaceOne(filter, replacement).toFuture().get()

  override def replaceOne(filter: Bson, replacement: T, replaceOptions: ReplaceOptions): UpdateResult =
    wrapped.replaceOne(filter, replacement, replaceOptions).toFuture().get()

  override def replaceOne(clientSession: ClientSession, filter: Bson, replacement: T): UpdateResult =
    wrapped.replaceOne(unwrap(clientSession), filter, replacement).toFuture().get()

  override def replaceOne(
      clientSession: ClientSession,
      filter: Bson,
      replacement: T,
      replaceOptions: ReplaceOptions
  ): UpdateResult =
    wrapped.replaceOne(unwrap(clientSession), filter, replacement, replaceOptions).toFuture().get()

  override def updateOne(filter: Bson, update: Bson): UpdateResult =
    wrapped.updateOne(filter, update).toFuture().get()

  override def updateOne(filter: Bson, update: Bson, updateOptions: UpdateOptions): UpdateResult =
    wrapped.updateOne(filter, update, updateOptions).toFuture().get()

  override def updateOne(clientSession: ClientSession, filter: Bson, update: Bson): UpdateResult =
    wrapped.updateOne(unwrap(clientSession), filter, update).toFuture().get()

  override def updateOne(
      clientSession: ClientSession,
      filter: Bson,
      update: Bson,
      updateOptions: UpdateOptions
  ): UpdateResult =
    wrapped.updateOne(unwrap(clientSession), filter, update, updateOptions).toFuture().get()

  override def updateOne(filter: Bson, update: java.util.List[_ <: Bson]): UpdateResult =
    wrapped.updateOne(filter, update.asScala.toSeq).toFuture().get()

  override def updateOne(filter: Bson, update: java.util.List[_ <: Bson], updateOptions: UpdateOptions): UpdateResult =
    wrapped.updateOne(filter, update.asScala.toSeq, updateOptions).toFuture().get()

  override def updateOne(clientSession: ClientSession, filter: Bson, update: java.util.List[_ <: Bson]): UpdateResult =
    wrapped.updateOne(unwrap(clientSession), filter, update.asScala.toSeq).toFuture().get()

  override def updateOne(
      clientSession: ClientSession,
      filter: Bson,
      update: java.util.List[_ <: Bson],
      updateOptions: UpdateOptions
  ): UpdateResult =
    wrapped.updateOne(unwrap(clientSession), filter, update.asScala.toSeq, updateOptions).toFuture().get()

  override def updateMany(filter: Bson, update: Bson): UpdateResult =
    wrapped.updateMany(filter, update).toFuture().get()

  override def updateMany(filter: Bson, update: Bson, updateOptions: UpdateOptions): UpdateResult =
    wrapped.updateMany(filter, update, updateOptions).toFuture().get()

  override def updateMany(clientSession: ClientSession, filter: Bson, update: Bson): UpdateResult =
    wrapped.updateMany(unwrap(clientSession), filter, update).toFuture().get()

  override def updateMany(
      clientSession: ClientSession,
      filter: Bson,
      update: Bson,
      updateOptions: UpdateOptions
  ): UpdateResult =
    wrapped.updateMany(unwrap(clientSession), filter, update, updateOptions).toFuture().get()

  override def updateMany(filter: Bson, update: java.util.List[_ <: Bson]): UpdateResult =
    wrapped.updateMany(filter, update.asScala.toSeq).toFuture().get()

  override def updateMany(filter: Bson, update: java.util.List[_ <: Bson], updateOptions: UpdateOptions): UpdateResult =
    wrapped.updateMany(filter, update.asScala.toSeq, updateOptions).toFuture().get()

  override def updateMany(clientSession: ClientSession, filter: Bson, update: java.util.List[_ <: Bson]): UpdateResult =
    wrapped.updateMany(unwrap(clientSession), filter, update.asScala.toSeq).toFuture().get()

  override def updateMany(
      clientSession: ClientSession,
      filter: Bson,
      update: java.util.List[_ <: Bson],
      updateOptions: UpdateOptions
  ): UpdateResult =
    wrapped.updateMany(unwrap(clientSession), filter, update.asScala.toSeq, updateOptions).toFuture().get()

  override def findOneAndDelete(filter: Bson): T =
    wrapped.findOneAndDelete(filter).toFuture().get()

  override def findOneAndDelete(filter: Bson, options: FindOneAndDeleteOptions): T =
    wrapped.findOneAndDelete(filter, options).toFuture().get()

  override def findOneAndDelete(clientSession: ClientSession, filter: Bson): T =
    wrapped.findOneAndDelete(unwrap(clientSession), filter).toFuture().get()

  override def findOneAndDelete(clientSession: ClientSession, filter: Bson, options: FindOneAndDeleteOptions): T =
    wrapped.findOneAndDelete(unwrap(clientSession), filter, options).toFuture().get()

  override def findOneAndReplace(filter: Bson, replacement: T): T =
    wrapped.findOneAndReplace(filter, replacement).toFuture().get()

  override def findOneAndReplace(filter: Bson, replacement: T, options: FindOneAndReplaceOptions): T =
    wrapped.findOneAndReplace(filter, replacement, options).toFuture().get()

  override def findOneAndReplace(clientSession: ClientSession, filter: Bson, replacement: T): T =
    wrapped.findOneAndReplace(unwrap(clientSession), filter, replacement).toFuture().get()

  override def findOneAndReplace(
      clientSession: ClientSession,
      filter: Bson,
      replacement: T,
      options: FindOneAndReplaceOptions
  ): T =
    wrapped.findOneAndReplace(unwrap(clientSession), filter, replacement, options).toFuture().get()

  override def findOneAndUpdate(filter: Bson, update: Bson): T =
    wrapped.findOneAndUpdate(filter, update).toFuture().get()

  override def findOneAndUpdate(filter: Bson, update: Bson, options: FindOneAndUpdateOptions): T =
    wrapped.findOneAndUpdate(filter, update, options).toFuture().get()

  override def findOneAndUpdate(clientSession: ClientSession, filter: Bson, update: Bson): T =
    wrapped.findOneAndUpdate(unwrap(clientSession), filter, update).toFuture().get()

  override def findOneAndUpdate(
      clientSession: ClientSession,
      filter: Bson,
      update: Bson,
      options: FindOneAndUpdateOptions
  ): T =
    wrapped.findOneAndUpdate(unwrap(clientSession), filter, update, options).toFuture().get()

  override def findOneAndUpdate(filter: Bson, update: java.util.List[_ <: Bson]): T =
    wrapped.findOneAndUpdate(filter, update.asScala.toSeq).toFuture().get()

  override def findOneAndUpdate(filter: Bson, update: java.util.List[_ <: Bson], options: FindOneAndUpdateOptions): T =
    wrapped.findOneAndUpdate(filter, update.asScala.toSeq, options).toFuture().get()

  override def findOneAndUpdate(clientSession: ClientSession, filter: Bson, update: java.util.List[_ <: Bson]): T =
    wrapped.findOneAndUpdate(unwrap(clientSession), filter, update.asScala.toSeq).toFuture().get()

  override def findOneAndUpdate(
      clientSession: ClientSession,
      filter: Bson,
      update: java.util.List[_ <: Bson],
      options: FindOneAndUpdateOptions
  ): T =
    wrapped.findOneAndUpdate(unwrap(clientSession), filter, update.asScala.toSeq, options).toFuture().get()

  override def drop(): Unit = wrapped.drop().toFuture().get()

  override def drop(clientSession: ClientSession): Unit = wrapped.drop(unwrap(clientSession)).toFuture().get()

  override def createIndex(keys: Bson): String = wrapped.createIndex(keys).toFuture().get()

  override def createIndex(keys: Bson, indexOptions: IndexOptions) =
    wrapped.createIndex(keys, indexOptions).toFuture().get()

  override def createIndex(clientSession: ClientSession, keys: Bson) =
    wrapped.createIndex(unwrap(clientSession), keys).toFuture().get()

  override def createIndex(clientSession: ClientSession, keys: Bson, indexOptions: IndexOptions) =
    wrapped.createIndex(unwrap(clientSession), keys, indexOptions).toFuture().get()

  override def createIndexes(indexes: java.util.List[IndexModel]) = throw new UnsupportedOperationException

  override def createIndexes(indexes: java.util.List[IndexModel], createIndexOptions: CreateIndexOptions) =
    throw new UnsupportedOperationException

  override def createIndexes(clientSession: ClientSession, indexes: java.util.List[IndexModel]) =
    throw new UnsupportedOperationException

  override def createIndexes(
      clientSession: ClientSession,
      indexes: java.util.List[IndexModel],
      createIndexOptions: CreateIndexOptions
  ) = throw new UnsupportedOperationException

  override def listIndexes = throw new UnsupportedOperationException

  override def listIndexes[TResult](resultClass: Class[TResult]) =
    SyncListIndexesIterable[TResult](
      wrapped
        .listIndexes[TResult]()(DefaultsTo.overrideDefault[TResult, org.mongodb.scala.Document], ClassTag(resultClass))
    )

  override def listIndexes(clientSession: ClientSession) = throw new UnsupportedOperationException

  override def listIndexes[TResult](clientSession: ClientSession, resultClass: Class[TResult]) =
    throw new UnsupportedOperationException

  override def dropIndex(indexName: String): Unit = wrapped.dropIndex(indexName).toFuture().get()

  override def dropIndex(indexName: String, dropIndexOptions: DropIndexOptions): Unit =
    wrapped.dropIndex(indexName, dropIndexOptions).toFuture().get()

  override def dropIndex(keys: Bson): Unit =
    wrapped.dropIndex(keys).toFuture().get()

  override def dropIndex(keys: Bson, dropIndexOptions: DropIndexOptions): Unit =
    wrapped.dropIndex(keys, dropIndexOptions).toFuture().get()

  override def dropIndex(clientSession: ClientSession, indexName: String): Unit =
    wrapped.dropIndex(unwrap(clientSession), indexName).toFuture().get()

  override def dropIndex(clientSession: ClientSession, keys: Bson): Unit =
    wrapped.dropIndex(unwrap(clientSession), keys).toFuture().get()

  override def dropIndex(clientSession: ClientSession, indexName: String, dropIndexOptions: DropIndexOptions): Unit =
    wrapped.dropIndex(unwrap(clientSession), indexName, dropIndexOptions).toFuture().get()

  override def dropIndex(clientSession: ClientSession, keys: Bson, dropIndexOptions: DropIndexOptions): Unit =
    wrapped.dropIndex(unwrap(clientSession), keys, dropIndexOptions).toFuture().get()

  override def dropIndexes(): Unit = {
    throw new UnsupportedOperationException
  }

  override def dropIndexes(clientSession: ClientSession): Unit = {
    throw new UnsupportedOperationException
  }

  override def dropIndexes(dropIndexOptions: DropIndexOptions): Unit = {
    throw new UnsupportedOperationException
  }

  override def dropIndexes(clientSession: ClientSession, dropIndexOptions: DropIndexOptions): Unit = {
    throw new UnsupportedOperationException
  }

  override def renameCollection(newCollectionNamespace: MongoNamespace): Unit = {
    throw new UnsupportedOperationException
  }

  override def renameCollection(
      newCollectionNamespace: MongoNamespace,
      renameCollectionOptions: RenameCollectionOptions
  ): Unit = {
    throw new UnsupportedOperationException
  }

  override def renameCollection(clientSession: ClientSession, newCollectionNamespace: MongoNamespace): Unit = {
    throw new UnsupportedOperationException
  }

  override def renameCollection(
      clientSession: ClientSession,
      newCollectionNamespace: MongoNamespace,
      renameCollectionOptions: RenameCollectionOptions
  ): Unit = {
    throw new UnsupportedOperationException
  }
}
