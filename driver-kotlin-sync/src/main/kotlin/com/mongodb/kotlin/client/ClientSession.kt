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
import com.mongodb.TransactionOptions
import com.mongodb.client.ClientSession as JClientSession
import java.io.Closeable
import java.util.concurrent.TimeUnit

/** A client session that supports transactions. */
public class ClientSession(public val wrapped: JClientSession) : Closeable {

    public override fun close(): Unit = wrapped.close()

    /** The options for this session. */
    public val options: ClientSessionOptions
        get() = wrapped.options

    /**
     * Returns true if operations in this session must be causally consistent
     *
     * @return whether operations in this session must be causally consistent.
     */
    public fun isCausallyConsistent(): Boolean = wrapped.isCausallyConsistent

    /**
     * Returns true if there is an active transaction on this session, and false otherwise
     *
     * @return true if there is an active transaction on this session
     */
    public fun hasActiveTransaction(): Boolean = wrapped.hasActiveTransaction()

    /**
     * Gets the transaction options.
     *
     * Only call this method of the session has an active transaction
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
     * Abort a transaction in the context of this session.
     *
     * A transaction can only be aborted if one has first been started.
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
