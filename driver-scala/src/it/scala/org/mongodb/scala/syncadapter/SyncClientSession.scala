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

import com.mongodb.{ ClientSessionOptions, MongoInterruptedException, ServerAddress, TransactionOptions }
import com.mongodb.client.{ ClientSession => JClientSession, TransactionBody }
import com.mongodb.session.ServerSession
import org.bson.{ BsonDocument, BsonTimestamp }
import org.mongodb.scala._
import com.mongodb.reactivestreams.client.syncadapter.{ SyncMongoClient => JSyncMongoClient }

case class SyncClientSession(wrapped: ClientSession, originator: Object) extends JClientSession {

  override def getPinnedServerAddress: ServerAddress = wrapped.getPinnedServerAddress

  override def getRecoveryToken: BsonDocument = wrapped.getRecoveryToken

  override def setRecoveryToken(recoveryToken: BsonDocument): Unit = wrapped.setRecoveryToken(recoveryToken)

  override def getOptions: ClientSessionOptions = wrapped.getOptions

  override def isCausallyConsistent: Boolean = wrapped.isCausallyConsistent

  override def getOriginator: Object = originator

  override def getServerSession: ServerSession = wrapped.getServerSession

  override def getOperationTime: BsonTimestamp = wrapped.getOperationTime

  override def advanceOperationTime(operationTime: BsonTimestamp): Unit = wrapped.advanceOperationTime(operationTime)

  override def advanceClusterTime(clusterTime: BsonDocument): Unit = wrapped.advanceClusterTime(clusterTime)

  override def getClusterTime: BsonDocument = wrapped.getClusterTime

  override def setSnapshotTimestamp(snapshotTimestamp: BsonTimestamp): Unit =
    wrapped.setSnapshotTimestamp(snapshotTimestamp)

  override def getSnapshotTimestamp: BsonTimestamp = wrapped.getSnapshotTimestamp

  override def close(): Unit = {
    wrapped.close()
    sleep(JSyncMongoClient.getSleepAfterSessionClose)
  }

  override def hasActiveTransaction: Boolean = wrapped.hasActiveTransaction

  override def notifyMessageSent: Boolean = wrapped.notifyMessageSent

  override def notifyOperationInitiated(operation: Object): Unit = wrapped.notifyOperationInitiated(operation)

  override def getTransactionOptions: TransactionOptions = wrapped.getTransactionOptions

  override def startTransaction(): Unit = wrapped.startTransaction()

  override def startTransaction(transactionOptions: TransactionOptions): Unit =
    wrapped.startTransaction(transactionOptions)

  override def commitTransaction(): Unit = wrapped.commitTransaction().toSingle().toFuture().get()

  override def abortTransaction(): Unit = wrapped.abortTransaction().toSingle().toFuture().get()

  override def withTransaction[T](transactionBody: TransactionBody[T]) = throw new UnsupportedOperationException

  override def withTransaction[T](transactionBody: TransactionBody[T], options: TransactionOptions) =
    throw new UnsupportedOperationException

  override def getTransactionContext: AnyRef = wrapped.getTransactionContext

  override def setTransactionContext(address: ServerAddress, transactionContext: Any): Unit =
    wrapped.setTransactionContext(address, transactionContext)

  override def clearTransactionContext(): Unit = wrapped.clearTransactionContext()

  private def sleep(millis: Long): Unit = {
    try Thread.sleep(millis)
    catch {
      case e: InterruptedException =>
        throw new MongoInterruptedException(null, e)
    }
  }
}
