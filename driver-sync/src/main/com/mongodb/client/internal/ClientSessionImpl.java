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

package com.mongodb.client.internal;

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.TransactionBody;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.operation.AbortTransactionOperation;
import com.mongodb.internal.operation.CommitTransactionOperation;
import com.mongodb.internal.operation.OperationHelper;
import com.mongodb.internal.operation.ReadOperation;
import com.mongodb.internal.operation.WriteConcernHelper;
import com.mongodb.internal.operation.WriteOperation;
import com.mongodb.internal.session.BaseClientSessionImpl;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.lang.Nullable;

import static com.mongodb.MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL;
import static com.mongodb.MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;

final class ClientSessionImpl extends BaseClientSessionImpl implements ClientSession {

    private static final int MAX_RETRY_TIME_LIMIT_MS = 120000;

    private final OperationExecutor operationExecutor;
    private TransactionState transactionState = TransactionState.NONE;
    private boolean messageSentInCurrentTransaction;
    private boolean commitInProgress;
    private TransactionOptions transactionOptions;

    ClientSessionImpl(final ServerSessionPool serverSessionPool, final Object originator, final ClientSessionOptions options,
                      final OperationExecutor operationExecutor) {
        super(serverSessionPool, originator, options);
        this.operationExecutor = operationExecutor;
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
    public void notifyOperationInitiated(final Object operation) {
        assertTrue(operation instanceof ReadOperation || operation instanceof WriteOperation);
        if (!(hasActiveTransaction() || operation instanceof CommitTransactionOperation)) {
            assertTrue(getPinnedServerAddress() == null
                    || (transactionState != TransactionState.ABORTED && transactionState != TransactionState.NONE));
            clearTransactionContext();
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
        startTransaction(transactionOptions, createTimeoutContext(transactionOptions));
    }

    @Override
    public void commitTransaction() {
        commitTransaction(true);
    }

    @Override
    public void abortTransaction() {
        if (transactionState == TransactionState.ABORTED) {
            throw new IllegalStateException("Cannot call abortTransaction twice");
        }
        if (transactionState == TransactionState.COMMITTED) {
            throw new IllegalStateException("Cannot call abortTransaction after calling commitTransaction");
        }
        if (transactionState == TransactionState.NONE) {
            throw new IllegalStateException("There is no transaction started");
        }
        try {
            if (messageSentInCurrentTransaction) {
                ReadConcern readConcern = transactionOptions.getReadConcern();
                if (readConcern == null) {
                    throw new MongoInternalException("Invariant violated.  Transaction options read concern can not be null");
                }
                resetTimeout();
                TimeoutContext timeoutContext = getTimeoutContext();
                WriteConcern writeConcern = assertNotNull(getWriteConcern(timeoutContext));
                operationExecutor
                        .execute(new AbortTransactionOperation(writeConcern)
                                .recoveryToken(getRecoveryToken()), readConcern, this);
            }
        } catch (RuntimeException e) {
            // ignore exceptions from abort
        } finally {
            clearTransactionContext();
            cleanupTransaction(TransactionState.ABORTED);
        }
    }

    private void startTransaction(final TransactionOptions transactionOptions, final TimeoutContext timeoutContext) {
        Boolean snapshot = getOptions().isSnapshot();
        if (snapshot != null && snapshot) {
            throw new IllegalArgumentException("Transactions are not supported in snapshot sessions");
        }
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
        WriteConcern writeConcern = getWriteConcern(timeoutContext);
        if (writeConcern == null) {
            throw new MongoInternalException("Invariant violated.  Transaction options write concern can not be null");
        }
        if (!writeConcern.isAcknowledged()) {
            throw new MongoClientException("Transactions do not support unacknowledged write concern");
        }
        clearTransactionContext();
        setTimeoutContext(timeoutContext);
    }

    @Nullable
    private WriteConcern getWriteConcern(@Nullable final TimeoutContext timeoutContext) {
        WriteConcern writeConcern = transactionOptions.getWriteConcern();
        if (hasTimeoutMS(timeoutContext) && hasWTimeoutMS(writeConcern)) {
            return WriteConcernHelper.cloneWithoutTimeout(writeConcern);
        }
        return writeConcern;
    }

    private void commitTransaction(final boolean resetTimeout) {
        if (transactionState == TransactionState.ABORTED) {
            throw new IllegalStateException("Cannot call commitTransaction after calling abortTransaction");
        }
        if (transactionState == TransactionState.NONE) {
            throw new IllegalStateException("There is no transaction started");
        }

        try {
            if (messageSentInCurrentTransaction) {
                ReadConcern readConcern = transactionOptions.getReadConcern();
                if (readConcern == null) {
                    throw new MongoInternalException("Invariant violated.  Transaction options read concern can not be null");
                }
                commitInProgress = true;
                if (resetTimeout) {
                    resetTimeout();
                }
                TimeoutContext timeoutContext = getTimeoutContext();
                WriteConcern writeConcern = assertNotNull(getWriteConcern(timeoutContext));
                operationExecutor
                        .execute(new CommitTransactionOperation(writeConcern,
                                transactionState == TransactionState.COMMITTED)
                                .recoveryToken(getRecoveryToken()), readConcern, this);
            }
        } catch (MongoException e) {
            clearTransactionContextOnError(e);
            throw e;
        } finally {
            transactionState = TransactionState.COMMITTED;
            commitInProgress = false;
        }
    }

    private void clearTransactionContextOnError(final MongoException e) {
        if (e.hasErrorLabel(TRANSIENT_TRANSACTION_ERROR_LABEL) || e.hasErrorLabel(UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
            clearTransactionContext();
        }
    }

    @Override
    public <T> T withTransaction(final TransactionBody<T> transactionBody) {
        return withTransaction(transactionBody, TransactionOptions.builder().build());
    }

    @Override
    public <T> T withTransaction(final TransactionBody<T> transactionBody, final TransactionOptions options) {
        notNull("transactionBody", transactionBody);
        long startTime = ClientSessionClock.INSTANCE.now();
        TimeoutContext withTransactionTimeoutContext = createTimeoutContext(options);

        outer:
        while (true) {
            T retVal;
            try {
                startTransaction(options, withTransactionTimeoutContext.copyTimeoutContext());
                retVal = transactionBody.execute();
            } catch (Throwable e) {
                if (transactionState == TransactionState.IN) {
                    abortTransaction();
                }
                if (e instanceof MongoException && !(e instanceof MongoOperationTimeoutException)) {
                    MongoException exceptionToHandle = OperationHelper.unwrap((MongoException) e);
                    if (exceptionToHandle.hasErrorLabel(TRANSIENT_TRANSACTION_ERROR_LABEL)
                            && ClientSessionClock.INSTANCE.now() - startTime < MAX_RETRY_TIME_LIMIT_MS) {
                        continue;
                    }
                }
                throw e;
            }
            if (transactionState == TransactionState.IN) {
                while (true) {
                    try {
                        commitTransaction(false);
                        break;
                    } catch (MongoException e) {
                        clearTransactionContextOnError(e);
                        if (!(e instanceof MongoOperationTimeoutException)
                                && ClientSessionClock.INSTANCE.now() - startTime < MAX_RETRY_TIME_LIMIT_MS) {
                            applyMajorityWriteConcernToTransactionOptions();

                            if (!(e instanceof MongoExecutionTimeoutException)
                                    && e.hasErrorLabel(UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
                                continue;
                            } else if (e.hasErrorLabel(TRANSIENT_TRANSACTION_ERROR_LABEL)) {
                                continue outer;
                            }
                        }
                        throw e;
                    }
                }
            }
            return retVal;
        }
    }

    @Override
    public void close() {
        try {
            if (transactionState == TransactionState.IN) {
                abortTransaction();
            }
        } finally {
            clearTransactionContext();
            super.close();
        }
    }

    // Apply majority write concern if the commit is to be retried.
    private void applyMajorityWriteConcernToTransactionOptions() {
        if (transactionOptions != null) {
            TimeoutContext timeoutContext = getTimeoutContext();
            WriteConcern writeConcern = getWriteConcern(timeoutContext);
            if (writeConcern != null) {
                transactionOptions = TransactionOptions.merge(TransactionOptions.builder()
                        .writeConcern(writeConcern.withW("majority")).build(), transactionOptions);
            } else {
                transactionOptions = TransactionOptions.merge(TransactionOptions.builder()
                        .writeConcern(WriteConcern.MAJORITY).build(), transactionOptions);
            }
        } else {
            transactionOptions = TransactionOptions.builder().writeConcern(WriteConcern.MAJORITY).build();
        }
    }

    private void cleanupTransaction(final TransactionState nextState) {
        messageSentInCurrentTransaction = false;
        transactionOptions = null;
        transactionState = nextState;
        setTimeoutContext(null);
    }

    private TimeoutContext createTimeoutContext(final TransactionOptions transactionOptions) {
        return new TimeoutContext(getTimeoutSettings(
                TransactionOptions.merge(transactionOptions, getOptions().getDefaultTransactionOptions()),
                operationExecutor.getTimeoutSettings()));
    }
}
