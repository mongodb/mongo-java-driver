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

    private enum TransactionState {
        NONE, IN, DONE, ABORTED
    }

    private final OperationExecutor executor;
    private TransactionState transactionState = TransactionState.NONE;
    private boolean messageSent;
    private boolean commitInProgress;

    private TransactionOptions transactionOptions;

    ClientSessionImpl(final ServerSessionPool serverSessionPool, final MongoClient mongoClient, final ClientSessionOptions options,
                      final OperationExecutor executor) {
        super(serverSessionPool, mongoClient, options);
        this.executor = executor;
   }

    @Override
    public boolean hasActiveTransaction() {
        return transactionState == TransactionState.IN || (transactionState == TransactionState.DONE && commitInProgress);
    }

    @Override
    public boolean notifyMessageSent() {
        boolean firstMessage = !messageSent;
        messageSent = true;
        return firstMessage;
    }

    @Override
    public TransactionOptions getTransactionOptions() {
        isTrue("in transaction", transactionState == TransactionState.IN || transactionState == TransactionState.DONE);
        return transactionOptions;
    }

    @Override
    public void startTransaction() {
        startTransaction(TransactionOptions.builder().build());
    }

    @Override
    public void startTransaction(final TransactionOptions transactionOptions) {
        notNull("transactionOptions", transactionOptions);
        if (transactionState == TransactionState.IN) {
            throw new IllegalStateException("Transaction already in progress");
        }
        if (transactionState == TransactionState.DONE) {
            cleanupTransaction(TransactionState.IN);
        } else {
            transactionState = TransactionState.IN;
        }
        getServerSession().advanceTransactionNumber();
        this.transactionOptions = TransactionOptions.merge(transactionOptions, getOptions().getDefaultTransactionOptions());
    }

    @Override
    public void commitTransaction(final SingleResultCallback<Void> callback) {
        if (transactionState == TransactionState.ABORTED) {
            throw new IllegalStateException("Cannot call commitTransaction after calling abortTransaction");
        }
        if (transactionState == TransactionState.NONE) {
            throw new IllegalStateException("There is no transaction started");
        }
        if (!messageSent) {
            cleanupTransaction(TransactionState.DONE);
            callback.onResult(null, null);
        } else {
            ReadConcern readConcern = transactionOptions.getReadConcern();
            if (readConcern == null) {
                throw new MongoInternalException("Invariant violated.  Transaction options read concern can not be null");
            }
            commitInProgress = true;
            executor.execute(new CommitTransactionOperation(transactionOptions.getWriteConcern()),
                    readConcern, this,
                    new SingleResultCallback<Void>() {
                        @Override
                        public void onResult(final Void result, final Throwable t) {
                            commitInProgress = false;
                            transactionState = TransactionState.DONE;
                            callback.onResult(result, t);
                        }
                    });
        }
    }

    @Override
    public void abortTransaction(final SingleResultCallback<Void> callback) {
        if (transactionState == TransactionState.ABORTED) {
            throw new IllegalStateException("Cannot call abortTransaction twice");
        }
        if (transactionState == TransactionState.DONE) {
            throw new IllegalStateException("Cannot call abortTransaction after calling commitTransaction");
        }
        if (transactionState == TransactionState.NONE) {
            throw new IllegalStateException("There is no transaction started");
        }
        if (!messageSent) {
            cleanupTransaction(TransactionState.ABORTED);
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
                            cleanupTransaction(TransactionState.ABORTED);
                            callback.onResult(null, null);
                        }
                    });
        }
    }

    // TODO: should there be a version of this that takes a callback?
    @Override
    public void close() {
        if (transactionState == TransactionState.IN) {
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

    private void cleanupTransaction(final TransactionState nextState) {
        messageSent = false;
        transactionOptions = null;
        transactionState = nextState;
    }
}
