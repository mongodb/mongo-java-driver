package org.mongodb.scala.syncadapter

import scala.collection.JavaConverters._
import com.mongodb.ClientSessionOptions
import com.mongodb.client.{ ClientSession, MongoClient => JMongoClient, MongoDatabase => JMongoDatabase }
import org.bson.Document
import org.bson.conversions.Bson
import org.mongodb.scala.MongoClient
import org.mongodb.scala.bson.DefaultHelper.DefaultsTo

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

case class SyncMongoClient(wrapped: MongoClient) extends JMongoClient {

  override def getDatabase(databaseName: String): JMongoDatabase =
    new SyncMongoDatabase(wrapped.getDatabase(databaseName))

  override def startSession: ClientSession =
    SyncClientSession(Await.result(wrapped.startSession().head(), Duration(10, "second")), this)

  override def startSession(options: ClientSessionOptions): ClientSession =
    SyncClientSession(Await.result(wrapped.startSession(options).head(), Duration(10, "second")), this)

  override def close(): Unit = wrapped.close()

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

  override def getClusterDescription = throw new UnsupportedOperationException

  private def unwrap(clientSession: ClientSession): org.mongodb.scala.ClientSession =
    clientSession.asInstanceOf[SyncClientSession].wrapped
}
