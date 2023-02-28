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
package com.mongodb.kotlin.client

import com.mongodb.ClientSessionOptions
import com.mongodb.ServerAddress
import com.mongodb.TransactionOptions
import com.mongodb.client.ClientSession as clientClientSession
import com.mongodb.session.ClientSession as jClientSession
import com.mongodb.session.ServerSession
import java.util.concurrent.TimeUnit
import org.bson.BsonDocument
import org.bson.BsonTimestamp

/** A client session that supports transactions. */
public class ClientSession(internal val wrapped: clientClientSession) : jClientSession {

    public override fun close(): Unit = wrapped.close()

    /**
     * Returns true if there is an active transaction on this session, and false otherwise
     *
     * @return true if there is an active transaction on this session
     */
    public fun hasActiveTransaction(): Boolean = wrapped.hasActiveTransaction()

    /**
     * Notify the client session that a message has been sent.
     *
     * For internal use only
     *
     * @return true if this is the first message sent, false otherwise
     */
    public fun notifyMessageSent(): Boolean = wrapped.notifyMessageSent()

    /**
     * Notify the client session that command execution is being initiated. This should be called before server
     * selection occurs.
     *
     * For internal use only
     *
     * @param operation the operation
     */
    public fun notifyOperationInitiated(operation: Any): Unit = wrapped.notifyOperationInitiated(operation)

    /**
     * Get the server address of the pinned mongos on this session. For internal use only.
     *
     * @return the server address of the pinned mongos
     */
    public override fun getPinnedServerAddress(): ServerAddress? = wrapped.pinnedServerAddress

    /**
     * Gets the transaction context.
     *
     * For internal use only
     *
     * @return the transaction context
     */
    public override fun getTransactionContext(): Any? = wrapped.transactionContext

    /**
     * Sets the transaction context.
     *
     * For internal use only
     *
     * Implementations may place additional restrictions on the type of the transaction context
     *
     * @param address the server address
     * @param transactionContext the transaction context
     */
    public override fun setTransactionContext(address: ServerAddress, transactionContext: Any): Unit =
        wrapped.setTransactionContext(address, transactionContext)

    /**
     * Clears the transaction context.
     *
     * For internal use only
     */
    public override fun clearTransactionContext(): Unit = wrapped.clearTransactionContext()

    /**
     * Get the recovery token from the latest outcome in a sharded transaction. For internal use only.
     *
     * @return the recovery token @mongodb.server.release 4.2
     * @since 3.11
     */
    public override fun getRecoveryToken(): BsonDocument? = wrapped.recoveryToken

    /**
     * Set the recovery token. For internal use only.
     *
     * @param recoveryToken the recovery token
     */
    public override fun setRecoveryToken(recoveryToken: BsonDocument) {
        wrapped.recoveryToken = recoveryToken
    }

    /**
     * Get the options for this session.
     *
     * @return the options
     */
    public override fun getOptions(): ClientSessionOptions = wrapped.options

    /**
     * Returns true if operations in this session must be causally consistent
     *
     * @return whether operations in this session must be causally consistent.
     */
    public override fun isCausallyConsistent(): Boolean = wrapped.isCausallyConsistent

    /**
     * Gets the originator for the session.
     *
     * Important because sessions must only be used by their own originator.
     *
     * @return the sessions originator
     */
    public override fun getOriginator(): Any = wrapped.originator

    /** @return the server session */
    public override fun getServerSession(): ServerSession = wrapped.serverSession

    /**
     * Gets the operation time of the last operation executed in this session.
     *
     * @return the operation time
     */
    public override fun getOperationTime(): BsonTimestamp = wrapped.operationTime

    /**
     * Set the operation time of the last operation executed in this session.
     *
     * @param operationTime the operation time
     */
    public override fun advanceOperationTime(operationTime: BsonTimestamp?): Unit =
        wrapped.advanceOperationTime(operationTime)

    /** @param clusterTime the cluster time to advance to */
    public override fun advanceClusterTime(clusterTime: BsonDocument?): Unit = wrapped.advanceClusterTime(clusterTime)

    /**
     * For internal use only.
     *
     * @param snapshotTimestamp the snapshot timestamp
     */
    public override fun setSnapshotTimestamp(snapshotTimestamp: BsonTimestamp?) {
        wrapped.snapshotTimestamp = snapshotTimestamp
    }

    /**
     * For internal use only.
     *
     * @return the snapshot timestamp
     */
    public override fun getSnapshotTimestamp(): BsonTimestamp? = wrapped.snapshotTimestamp

    /** @return the latest cluster time seen by this session */
    public override fun getClusterTime(): BsonDocument = wrapped.clusterTime

    /**
     * Gets the transaction options. Only call this method of the session has an active transaction
     *
     * @return the transaction options
     */
    public fun getTransactionOptions(): TransactionOptions = wrapped.transactionOptions

    /**
     * Start a transaction in the context of this session with default transaction options. A transaction can not be
     * started if there is already an active transaction on this session.
     */
    public fun startTransaction(): Unit = wrapped.startTransaction()

    /**
     * Start a transaction in the context of this session with the given transaction options. A transaction can not be
     * started if there is already an active transaction on this session.
     *
     * @param transactionOptions the options to apply to the transaction
     */
    public fun startTransaction(transactionOptions: TransactionOptions): Unit =
        wrapped.startTransaction(transactionOptions)

    /**
     * Commit a transaction in the context of this session. A transaction can only be commmited if one has first been
     * started.
     */
    public fun commitTransaction(): Unit = wrapped.commitTransaction()

    /**
     * Abort a transaction in the context of this session. A transaction can only be aborted if one has first been
     * started.
     */
    public fun abortTransaction(): Unit = wrapped.abortTransaction()

    /**
     * Execute the given function within a transaction.
     *
     * @param T the return type of the transaction body
     * @param transactionBody the body of the transaction
     * @param options the transaction options
     * @return the return value of the transaction body
     */
    public fun <T : Any> withTransaction(
        transactionBody: () -> T,
        options: TransactionOptions = TransactionOptions.builder().build()
    ): T = wrapped.withTransaction(transactionBody, options)
}

/**
 * maxCommitTime extension function
 *
 * @param maxCommitTime time in milliseconds
 * @return the options
 */
public fun TransactionOptions.Builder.maxCommitTime(maxCommitTime: Long): TransactionOptions.Builder =
    this.apply { maxCommitTime(maxCommitTime, TimeUnit.MILLISECONDS) }
