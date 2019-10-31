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

package com.mongodb.internal.async.client;

import com.mongodb.TransactionOptions;
import com.mongodb.internal.async.SingleResultCallback;

/**
 * A client session that supports transactions.
 */
public interface AsyncClientSession extends com.mongodb.session.ClientSession {
    /**
     * Returns true if there is an active transaction on this session, and false otherwise
     *
     * @return true if there is an active transaction on this session
     * @mongodb.server.release 4.0
     */
    boolean hasActiveTransaction();

    /**
     *  Notify the client session that a message has been sent.
     *  <p>
     *      For internal use only
     *  </p>
     *
     * @return true if this is the first message sent, false otherwise
     */
    boolean notifyMessageSent();

    /**
     * Gets the transaction options.  Only call this method of the session has an active transaction
     *
     * @return the transaction options
     */
    TransactionOptions getTransactionOptions();

    /**
     * Start a transaction in the context of this session with default transaction options. A transaction can not be started if there is
     * already an active transaction on this session.
     *
     * @mongodb.server.release 4.0
     */
    void startTransaction();

    /**
     * Start a transaction in the context of this session with the given transaction options. A transaction can not be started if there is
     * already an active transaction on this session.
     *
     * @param transactionOptions the options to apply to the transaction
     *
     * @mongodb.server.release 4.0
     */
    void startTransaction(TransactionOptions transactionOptions);

    /**
     * Commit a transaction in the context of this session.  A transaction can only be commmited if one has first been started.
     *
     * @param callback the result callback
     * @mongodb.server.release 4.0
     */
    void commitTransaction(SingleResultCallback<Void> callback);

    /**
     * Abort a transaction in the context of this session.  A transaction can only be aborted if one has first been started.
     *
     * @param callback the result callback
     * @mongodb.server.release 4.0
     */
    void abortTransaction(SingleResultCallback<Void> callback);
}
