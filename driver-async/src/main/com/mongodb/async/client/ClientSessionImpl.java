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
import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.internal.session.BaseClientSessionImpl;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.operation.AbortTransactionOperation;
import com.mongodb.operation.CommitTransactionOperation;

import static com.mongodb.MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL;
import static com.mongodb.MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL;
import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;

class ClientSessionImpl extends BaseClientSessionImpl implements ClientSession {

    private enum TransactionState {
        NONE, IN, COMMITTED, ABORTED
    }

    private final OperationExecutor executor;
    private TransactionState transactionState = TransactionState.NONE;
    private boolean messageSentInCurrentTransaction;
    private boolean commitInProgress;

    private TransactionOptions transactionOptions;

    ClientSessionImpl(final ServerSessionPool serverSessionPool, final MongoClient mongoClient, final ClientSessionOptions options,
                      final OperationExecutor executor) {
        super(serverSessionPool, mongoClient, options);
        this.executor = executor;
   }

    @Override
    public boolean hasActiveTransaction() {
        return transactionState == TransactionState.IN || (transactionState == TransactionState.COMMITTED && commitInProgress);
    }

    @Override
    public boolean notifyMessageSent() {
        if (hasActiveTransaction()) {
            boolean firstMessageInCurrentTransaction = !messageSentInCurrentTransaction;
            messageSentInCurrentTransaction = true;
            return firstMessageInCurrentTransaction;
        } else {
            if (transactionState == TransactionState.COMMITTED || transactionState == TransactionState.ABORTED) {
                cleanupTransaction(TransactionState.NONE);
            }
            return false;
        }
    }

    @Override
    public TransactionOptions getTransactionOptions() {
        isTrue("in transaction", transactionState == TransactionState.IN || transactionState == TransactionState.COMMITTED);
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
        if (transactionState == TransactionState.COMMITTED) {
            cleanupTransaction(TransactionState.IN);
        } else {
            transactionState = TransactionState.IN;
        }
        getServerSession().advanceTransactionNumber();
        this.transactionOptions = TransactionOptions.merge(transactionOptions, getOptions().getDefaultTransactionOptions());
        WriteConcern writeConcern = this.transactionOptions.getWriteConcern();
        if (writeConcern == null) {
            throw new MongoInternalException("Invariant violated.  Transaction options write concern can not be null");
        }
        if (!writeConcern.isAcknowledged()) {
            throw new MongoClientException("Transactions do not support unacknowledged write concern");
        }
    }

    @Override
    public void commitTransaction(final SingleResultCallback<Void> callback) {
        if (transactionState == TransactionState.ABORTED) {
            throw new IllegalStateException("Cannot call commitTransaction after calling abortTransaction");
        }
        if (transactionState == TransactionState.NONE) {
            throw new IllegalStateException("There is no transaction started");
        }
        if (!messageSentInCurrentTransaction) {
            cleanupTransaction(TransactionState.COMMITTED);
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
                            transactionState = TransactionState.COMMITTED;
                            if (t instanceof MongoException) {
                                MongoException e = (MongoException) t;
                                if (e.hasErrorLabel(TRANSIENT_TRANSACTION_ERROR_LABEL)) {
                                    e.removeLabel(TRANSIENT_TRANSACTION_ERROR_LABEL);
                                    e.addLabel(UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL);
                                }
                            }
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
        if (transactionState == TransactionState.COMMITTED) {
            throw new IllegalStateException("Cannot call abortTransaction after calling commitTransaction");
        }
        if (transactionState == TransactionState.NONE) {
            throw new IllegalStateException("There is no transaction started");
        }
        if (!messageSentInCurrentTransaction) {
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
        messageSentInCurrentTransaction = false;
        transactionOptions = null;
        transactionState = nextState;
    }
}
