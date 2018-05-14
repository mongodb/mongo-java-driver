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

package com.mongodb.async.client;

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoInternalException;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.internal.session.BaseClientSessionImpl;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.operation.AbortTransactionOperation;
import com.mongodb.operation.CommitTransactionOperation;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;

class ClientSessionImpl extends BaseClientSessionImpl implements ClientSession {

    private final OperationExecutor executor;
    private boolean inTransaction;
    private boolean messageSent;
    private TransactionOptions transactionOptions;

    ClientSessionImpl(final ServerSessionPool serverSessionPool, final MongoClient mongoClient, final ClientSessionOptions options,
                      final OperationExecutor executor) {
        super(serverSessionPool, mongoClient, options);
        this.executor = executor;
   }

    @Override
    public boolean hasActiveTransaction() {
        return inTransaction;
    }

    @Override
    public boolean notifyMessageSent() {
        boolean firstMessage = !messageSent;
        messageSent = true;
        return firstMessage;
    }

    @Override
    public TransactionOptions getTransactionOptions() {
        isTrue("in transaction", inTransaction);
        return transactionOptions;
    }

    @Override
    public void startTransaction() {
        startTransaction(TransactionOptions.builder().build());
    }

    @Override
    public void startTransaction(final TransactionOptions transactionOptions) {
        notNull("transactionOptions", transactionOptions);
        if (inTransaction) {
            throw new IllegalStateException("Transaction already in progress");
        }
        inTransaction = true;
        this.transactionOptions = TransactionOptions.merge(transactionOptions, getOptions().getDefaultTransactionOptions());
    }

    @Override
    public void commitTransaction(final SingleResultCallback<Void> callback) {
        if (!canCommitOrAbort()) {
            throw new IllegalStateException("There is no transaction started");
        }
        if (!messageSent) {
            cleanupTransaction();
            callback.onResult(null, null);
        } else {
            ReadConcern readConcern = transactionOptions.getReadConcern();
            if (readConcern == null) {
                throw new MongoInternalException("Invariant violated.  Transaction options read concern can not be null");
            }
            executor.execute(new CommitTransactionOperation(transactionOptions.getWriteConcern()),
                    readConcern, this,
                    new SingleResultCallback<Void>() {
                        @Override
                        public void onResult(final Void result, final Throwable t) {
                            cleanupTransaction();
                            callback.onResult(result, t);
                        }
                    });
        }
    }

    @Override
    public void abortTransaction(final SingleResultCallback<Void> callback) {
        if (!canCommitOrAbort()) {
            throw new IllegalStateException("There is no transaction started");
        }
        if (!messageSent) {
            cleanupTransaction();
            callback.onResult(null, null);
        } else {
            ReadConcern readConcern = transactionOptions.getReadConcern();
            if (readConcern == null) {
                throw new MongoInternalException("Invariant violated.  Transaction options read concern can not be null");
            }
            executor.execute(new AbortTransactionOperation(transactionOptions.getWriteConcern()),
                    readConcern, this,
                    new SingleResultCallback<Void>() {
                        @Override
                        public void onResult(final Void result, final Throwable t) {
                            cleanupTransaction();
                            callback.onResult(null, null);
                        }
                    });
        }
    }

    private boolean canCommitOrAbort() {
        return inTransaction || getOptions().getAutoStartTransaction();
    }

    // TODO: should there be a version of this that takes a callback?
    @Override
    public void close() {
        if (inTransaction) {
            abortTransaction(new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    ClientSessionImpl.super.close();
                }
            });
        } else {
            super.close();
        }
    }

    private void cleanupTransaction() {
        inTransaction = false;
        messageSent = false;
        transactionOptions = null;
        getServerSession().advanceTransactionNumber();
    }
}
