package org.mongodb.scala.syncadapter

import com.mongodb.MongoDriverInformation
import com.mongodb.client.{ MongoClient => JMongoClient }
import org.mongodb.scala.MongoClient

case class SyncMongoClient(wrapped: MongoClient) extends SyncMongoCluster(wrapped) with JMongoClient {

  override def close(): Unit = wrapped.close()

  override def getClusterDescription = throw new UnsupportedOperationException

  override def appendMetadata(mongoDriverInformation: MongoDriverInformation): Unit =
    wrapped.appendMetadata(mongoDriverInformation)
}
