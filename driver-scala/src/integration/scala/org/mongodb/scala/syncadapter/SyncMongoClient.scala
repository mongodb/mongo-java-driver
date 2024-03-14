package org.mongodb.scala.syncadapter

import com.mongodb.ClientSessionOptions
import com.mongodb.client.{ ClientSession, MongoClient => JMongoClient, MongoDatabase => JMongoDatabase }
import org.bson.Document
import org.bson.conversions.Bson
import org.mongodb.scala.MongoClient
import org.mongodb.scala.bson.DefaultHelper.DefaultsTo

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.reflect.ClassTag

case class SyncMongoClient(wrapped: MongoClient) extends SyncMongoCluster(wrapped) with JMongoClient {

  override def close(): Unit = wrapped.close()

  override def getClusterDescription = throw new UnsupportedOperationException

}
