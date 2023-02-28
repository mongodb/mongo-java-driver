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
package com.mongodb.kotlin.client.syncadapter

import com.mongodb.ClientSessionOptions
import com.mongodb.ServerAddress
import com.mongodb.TransactionOptions
import com.mongodb.client.ClientSession as JClientSession
import com.mongodb.client.TransactionBody
import com.mongodb.kotlin.client.ClientSession
import com.mongodb.session.ServerSession
import org.bson.BsonDocument
import org.bson.BsonTimestamp

internal class SyncClientSession(internal val wrapped: ClientSession, private val originator: Any) : JClientSession {
    private val delegate: JClientSession = wrapped.wrapped

    override fun close(): Unit = delegate.close()

    override fun getPinnedServerAddress(): ServerAddress? = delegate.pinnedServerAddress

    override fun getTransactionContext(): Any? = delegate.transactionContext

    override fun setTransactionContext(address: ServerAddress, transactionContext: Any): Unit =
        delegate.setTransactionContext(address, transactionContext)

    override fun clearTransactionContext(): Unit = delegate.clearTransactionContext()

    override fun getRecoveryToken(): BsonDocument? = delegate.recoveryToken

    override fun setRecoveryToken(recoveryToken: BsonDocument): Unit {
        delegate.recoveryToken = recoveryToken
    }

    override fun getOptions(): ClientSessionOptions = delegate.options

    override fun isCausallyConsistent(): Boolean = delegate.isCausallyConsistent

    override fun getOriginator(): Any = originator

    override fun getServerSession(): ServerSession = delegate.serverSession

    override fun getOperationTime(): BsonTimestamp = delegate.operationTime

    override fun advanceOperationTime(operationTime: BsonTimestamp?): Unit =
        delegate.advanceOperationTime(operationTime)

    override fun advanceClusterTime(clusterTime: BsonDocument?): Unit = delegate.advanceClusterTime(clusterTime)

    override fun setSnapshotTimestamp(snapshotTimestamp: BsonTimestamp?) {
        delegate.snapshotTimestamp = snapshotTimestamp
    }

    override fun getSnapshotTimestamp(): BsonTimestamp? = delegate.snapshotTimestamp

    override fun getClusterTime(): BsonDocument = delegate.clusterTime

    override fun hasActiveTransaction(): Boolean = delegate.hasActiveTransaction()

    override fun notifyMessageSent(): Boolean = delegate.notifyMessageSent()

    override fun notifyOperationInitiated(operation: Any): Unit = delegate.notifyOperationInitiated(operation)

    override fun getTransactionOptions(): TransactionOptions = delegate.transactionOptions

    override fun startTransaction(): Unit = delegate.startTransaction()

    override fun startTransaction(transactionOptions: TransactionOptions): Unit =
        delegate.startTransaction(transactionOptions)

    override fun commitTransaction(): Unit = delegate.commitTransaction()

    override fun abortTransaction(): Unit = delegate.abortTransaction()

    override fun <T : Any> withTransaction(transactionBody: TransactionBody<T>): T =
        throw UnsupportedOperationException()

    override fun <T : Any> withTransaction(transactionBody: TransactionBody<T>, options: TransactionOptions): T =
        throw UnsupportedOperationException()
}
