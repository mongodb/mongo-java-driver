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
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.internal.session.BaseClientSessionImpl;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.operation.AbortTransactionOperation;
import com.mongodb.operation.CommitTransactionOperation;
import com.mongodb.operation.TransactionOperation;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;

class ClientSessionImpl extends BaseClientSessionImpl implements ClientSession {

    private final OperationExecutor executor;
    private boolean inTransaction;
    private TransactionOptions transactionOptions;

    ClientSessionImpl(final ServerSessionPool serverSessionPool, final MongoClient mongoClient, final ClientSessionOptions options,
                      final OperationExecutor executor) {
        super(serverSessionPool, mongoClient, options);
        this.executor = executor;
        if (options.getAutoStartTransaction()) {
            startTransaction(options.getDefaultTransactionOptions());
        }
   }

    @Override
    public boolean hasActiveTransaction() {
        return inTransaction;
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
        if (!inTransaction) {
            throw new IllegalStateException("There is no transaction started");
        }
        endTransaction(new CommitTransactionOperation(transactionOptions.getWriteConcern()), callback);
    }

    @Override
    public void abortTransaction(final SingleResultCallback<Void> callback) {
        if (!inTransaction) {
            throw new IllegalStateException("There is no transaction started");
        }
        endTransaction(new AbortTransactionOperation(transactionOptions.getWriteConcern()), new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                // Don't report failure to abort the transaction
                callback.onResult(null, null);
            }
        });
    }

    private void endTransaction(final TransactionOperation operation, final SingleResultCallback<Void> callback) {
        if (getServerSession().getStatementId() == 0) {
            cleanupTransaction();
            callback.onResult(null, null);
        } else {
            ReadConcern readConcern = transactionOptions.getReadConcern();
            if (readConcern == null) {
                throw new MongoInternalException("Invariant violated.  Transaction options read concern can not be null");
            }
            executor.execute(operation,
                    ReadPreference.primary(), readConcern, this,
                    new SingleResultCallback<Void>() {
                        @Override
                        public void onResult(final Void result, final Throwable t) {
                            cleanupTransaction();
                            callback.onResult(result, t);
                        }
                    });
        }
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
        transactionOptions = null;
        if (getOptions().getAutoStartTransaction()) {
            startTransaction(getOptions().getDefaultTransactionOptions());
        }
        getServerSession().advanceTransactionNumber();
    }
}
