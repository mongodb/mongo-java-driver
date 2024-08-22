package org.mongodb.scala.syncadapter

import com.mongodb.assertions.Assertions
import com.mongodb.client.model.bulk.{ ClientBulkWriteOptions, ClientNamespacedWriteModel }
import com.mongodb.client.result.bulk.ClientBulkWriteResult
import com.mongodb.{ ClientSessionOptions, ReadConcern, ReadPreference, WriteConcern }
import com.mongodb.client.{ ClientSession, MongoCluster => JMongoCluster, MongoDatabase => JMongoDatabase }
import org.bson.Document
import org.bson.codecs.configuration.CodecRegistry
import org.bson.conversions.Bson
import org.mongodb.scala.MongoCluster
import org.mongodb.scala.bson.DefaultHelper.DefaultsTo

import java.util
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

object SyncMongoCluster {

  def apply(wrapped: MongoCluster): SyncMongoCluster = new SyncMongoCluster(wrapped)
}

class SyncMongoCluster(wrapped: MongoCluster) extends JMongoCluster {

  override def getCodecRegistry: CodecRegistry = wrapped.codecRegistry

  override def getReadPreference: ReadPreference = wrapped.readPreference

  override def getWriteConcern: WriteConcern = wrapped.writeConcern

  override def getReadConcern: ReadConcern = wrapped.readConcern

  override def getTimeout(timeUnit: TimeUnit): java.lang.Long = {
    val timeout = wrapped.timeout.map(d => timeUnit.convert(d.toMillis, TimeUnit.MILLISECONDS))
    if (timeout.isDefined) timeout.get else null
  }

  override def withCodecRegistry(codecRegistry: CodecRegistry): JMongoCluster =
    SyncMongoCluster(wrapped.withCodecRegistry(codecRegistry))

  override def withReadPreference(readPreference: ReadPreference): JMongoCluster =
    SyncMongoCluster(wrapped.withReadPreference(readPreference))

  override def withWriteConcern(writeConcern: WriteConcern): JMongoCluster =
    SyncMongoCluster(wrapped.withWriteConcern(writeConcern))

  override def withReadConcern(readConcern: ReadConcern): JMongoCluster =
    SyncMongoCluster(wrapped.withReadConcern(readConcern))

  override def withTimeout(timeout: Long, timeUnit: TimeUnit): JMongoCluster =
    SyncMongoCluster(wrapped.withTimeout(Duration(timeout, timeUnit)))

  override def getDatabase(databaseName: String): JMongoDatabase =
    SyncMongoDatabase(wrapped.getDatabase(databaseName))

  override def startSession: ClientSession =
    SyncClientSession(Await.result(wrapped.startSession().head(), WAIT_DURATION), this)

  override def startSession(options: ClientSessionOptions): ClientSession =
    SyncClientSession(Await.result(wrapped.startSession(options).head(), WAIT_DURATION), this)

  override def listDatabaseNames = throw new UnsupportedOperationException

  override def listDatabaseNames(clientSession: ClientSession) = throw new UnsupportedOperationException

  override def listDatabases = new SyncListDatabasesIterable[Document](wrapped.listDatabases[Document]())

  override def listDatabases(clientSession: ClientSession) = throw new UnsupportedOperationException

  override def listDatabases[TResult](resultClass: Class[TResult]) =
    new SyncListDatabasesIterable[TResult](
      wrapped.listDatabases[TResult]()(
        DefaultsTo.overrideDefault[TResult, org.mongodb.scala.Document],
        ClassTag(resultClass)
      )
    )

  override def listDatabases[TResult](clientSession: ClientSession, resultClass: Class[TResult]) =
    throw new UnsupportedOperationException

  override def watch = new SyncChangeStreamIterable[Document](wrapped.watch[Document]())

  override def watch[TResult](resultClass: Class[TResult]) =
    new SyncChangeStreamIterable[TResult](
      wrapped.watch[TResult]()(DefaultsTo.overrideDefault[TResult, org.mongodb.scala.Document], ClassTag(resultClass))
    )

  override def watch(pipeline: java.util.List[_ <: Bson]) =
    new SyncChangeStreamIterable[Document](wrapped.watch[Document](pipeline.asScala.toSeq))

  override def watch[TResult](pipeline: java.util.List[_ <: Bson], resultClass: Class[TResult]) =
    new SyncChangeStreamIterable[TResult](
      wrapped.watch[TResult](pipeline.asScala.toSeq)(
        DefaultsTo.overrideDefault[TResult, org.mongodb.scala.Document],
        ClassTag(resultClass)
      )
    )

  override def watch(clientSession: ClientSession) =
    new SyncChangeStreamIterable[Document](wrapped.watch[Document](unwrap(clientSession)))

  override def watch[TResult](clientSession: ClientSession, resultClass: Class[TResult]) =
    new SyncChangeStreamIterable[TResult](
      wrapped.watch(unwrap(clientSession))(
        DefaultsTo.overrideDefault[TResult, org.mongodb.scala.Document],
        ClassTag(resultClass)
      )
    )

  override def watch(clientSession: ClientSession, pipeline: java.util.List[_ <: Bson]) =
    new SyncChangeStreamIterable[Document](wrapped.watch[Document](unwrap(clientSession), pipeline.asScala.toSeq))

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

  private def unwrap(clientSession: ClientSession): org.mongodb.scala.ClientSession =
    clientSession.asInstanceOf[SyncClientSession].wrapped

  override def bulkWrite(
      models: util.List[_ <: ClientNamespacedWriteModel]
  ): ClientBulkWriteResult =
    throw Assertions.fail("BULK-TODO implement")

  override def bulkWrite(
      models: util.List[_ <: ClientNamespacedWriteModel],
      options: ClientBulkWriteOptions
  ): ClientBulkWriteResult =
    throw Assertions.fail("BULK-TODO implement")

  override def bulkWrite(
      clientSession: ClientSession,
      models: util.List[_ <: ClientNamespacedWriteModel]
  ): ClientBulkWriteResult =
    throw Assertions.fail("BULK-TODO implement")

  override def bulkWrite(
      clientSession: ClientSession,
      models: util.List[_ <: ClientNamespacedWriteModel],
      options: ClientBulkWriteOptions
  ): ClientBulkWriteResult =
    throw Assertions.fail("BULK-TODO implement")
}
