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
package com.mongodb.kotlin.client.coroutine.syncadapter

import com.mongodb.ClientSessionOptions
import com.mongodb.ServerAddress
import com.mongodb.TransactionOptions
import com.mongodb.client.ClientSession as JClientSession
import com.mongodb.client.TransactionBody
import com.mongodb.kotlin.client.coroutine.ClientSession
import com.mongodb.session.ServerSession
import kotlinx.coroutines.runBlocking
import org.bson.BsonDocument
import org.bson.BsonTimestamp

class SyncClientSession(internal val wrapped: ClientSession, private val originator: Any) : JClientSession {
    override fun close(): Unit = wrapped.close()

    override fun getPinnedServerAddress(): ServerAddress? = wrapped.pinnedServerAddress

    override fun getTransactionContext(): Any? = wrapped.transactionContext

    override fun setTransactionContext(address: ServerAddress, transactionContext: Any): Unit =
        wrapped.setTransactionContext(address, transactionContext)

    override fun clearTransactionContext(): Unit = wrapped.clearTransactionContext()

    override fun getRecoveryToken(): BsonDocument? = wrapped.recoveryToken

    override fun setRecoveryToken(recoveryToken: BsonDocument): Unit = wrapped.setRecoveryToken(recoveryToken)

    override fun getOptions(): ClientSessionOptions = wrapped.options

    override fun isCausallyConsistent(): Boolean = wrapped.isCausallyConsistent

    override fun getOriginator(): Any = originator

    override fun getServerSession(): ServerSession = wrapped.serverSession

    override fun getOperationTime(): BsonTimestamp = wrapped.operationTime

    override fun advanceOperationTime(operationTime: BsonTimestamp?): Unit = wrapped.advanceOperationTime(operationTime)

    override fun advanceClusterTime(clusterTime: BsonDocument?): Unit = wrapped.advanceClusterTime(clusterTime)

    override fun setSnapshotTimestamp(snapshotTimestamp: BsonTimestamp?) {
        wrapped.snapshotTimestamp = snapshotTimestamp
    }

    override fun getSnapshotTimestamp(): BsonTimestamp? = wrapped.snapshotTimestamp

    override fun getClusterTime(): BsonDocument = wrapped.clusterTime

    override fun hasActiveTransaction(): Boolean = wrapped.hasActiveTransaction()

    override fun notifyMessageSent(): Boolean = wrapped.notifyMessageSent()

    override fun notifyOperationInitiated(operation: Any): Unit = wrapped.notifyOperationInitiated(operation)

    override fun getTransactionOptions(): TransactionOptions = wrapped.getTransactionOptions()

    override fun startTransaction(): Unit = wrapped.startTransaction()

    override fun startTransaction(transactionOptions: TransactionOptions): Unit =
        wrapped.startTransaction(transactionOptions)

    override fun commitTransaction(): Unit = runBlocking { wrapped.commitTransaction() }

    override fun abortTransaction(): Unit = runBlocking { wrapped.abortTransaction() }

    override fun <T : Any> withTransaction(transactionBody: TransactionBody<T>): T =
        throw UnsupportedOperationException()

    override fun <T : Any> withTransaction(transactionBody: TransactionBody<T>, options: TransactionOptions): T =
        throw UnsupportedOperationException()
}
