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
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WithTransactionTimeoutException;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.TransactionBody;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.observability.micrometer.TracingManager;
import com.mongodb.internal.observability.micrometer.TransactionSpan;
import com.mongodb.internal.operation.AbortTransactionOperation;
import com.mongodb.internal.operation.CommitTransactionOperation;
import com.mongodb.internal.operation.OperationHelper;
import com.mongodb.internal.operation.ReadOperation;
import com.mongodb.internal.operation.WriteConcernHelper;
import com.mongodb.internal.operation.WriteOperation;
import com.mongodb.internal.session.BaseClientSessionImpl;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.internal.time.ExponentialBackoff;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static com.mongodb.MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL;
import static com.mongodb.MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.TimeoutContext.createMongoTimeoutException;
import static com.mongodb.internal.thread.InterruptionUtil.interruptAndCreateMongoInterruptedException;

final class ClientSessionImpl extends BaseClientSessionImpl implements ClientSession {

    private static final long MAX_RETRY_TIME_LIMIT_MS = 120000;

    private final OperationExecutor operationExecutor;
    private TransactionState transactionState = TransactionState.NONE;
    private boolean messageSentInCurrentTransaction;
    private boolean commitInProgress;
    private TransactionOptions transactionOptions;
    private final TracingManager tracingManager;
    private TransactionSpan transactionSpan = null;

    ClientSessionImpl(final ServerSessionPool serverSessionPool, final Object originator, final ClientSessionOptions options,
            final OperationExecutor operationExecutor, final TracingManager tracingManager) {
        super(serverSessionPool, originator, options);
        this.operationExecutor = operationExecutor;
        this.tracingManager = tracingManager;
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
            if (transactionSpan != null) {
                transactionSpan.finalizeTransactionSpan(TransactionState.ABORTED.name());
            }
        }
    }

    private void abortIfInTransaction() {
        if (transactionState == TransactionState.IN) {
            abortTransaction();
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

        if (tracingManager.isEnabled()) {
            transactionSpan = new TransactionSpan(tracingManager);
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
        boolean exceptionThrown = false;
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
            exceptionThrown = true;
            clearTransactionContextOnError(e);
            if (transactionSpan != null) {
                transactionSpan.handleTransactionSpanError(e);
            }
            throw e;
        } finally {
            transactionState = TransactionState.COMMITTED;
            commitInProgress = false;
            if (!exceptionThrown) {
                if (transactionSpan != null) {
                    transactionSpan.finalizeTransactionSpan(TransactionState.COMMITTED.name());
                }
            }
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
        TimeoutContext withTransactionTimeoutContext = createTimeoutContext(options);
        boolean timeoutMsConfigured = withTransactionTimeoutContext.hasTimeoutMS();
        Timeout withTransactionTimeout = assertNotNull(timeoutMsConfigured
                ? withTransactionTimeoutContext.getTimeout()
                : TimeoutContext.startTimeout(MAX_RETRY_TIME_LIMIT_MS));
        BooleanSupplier withTransactionTimeoutExpired = () -> withTransactionTimeout.call(TimeUnit.MILLISECONDS,
                () -> false, ms -> false, () -> true);
        int transactionAttempt = 0;
        MongoException lastError = null;

        try {
            outer:
            while (true) {
                if (transactionAttempt > 0) {
                    backoff(transactionAttempt, withTransactionTimeout, assertNotNull(lastError), timeoutMsConfigured);
                }
                try {
                    startTransaction(options, withTransactionTimeoutContext);
                    transactionAttempt++;
                    if (transactionSpan != null) {
                        transactionSpan.setIsConvenientTransaction();
                    }
                } catch (Throwable e) {
                    abortIfInTransaction();
                    throw e;
                }
                T retVal;
                try {
                    retVal = transactionBody.execute();
                } catch (Throwable e) {
                    abortIfInTransaction();
                    if (e instanceof MongoException) {
                        MongoException mongoException = (MongoException) e;
                        MongoException labelCarryingException = OperationHelper.unwrap(mongoException);
                        if (labelCarryingException.hasErrorLabel(TRANSIENT_TRANSACTION_ERROR_LABEL)) {
                            if (transactionSpan != null) {
                                transactionSpan.spanFinalizing(false);
                            }
                            lastError = mongoException;
                            continue;
                        } else if (labelCarryingException.hasErrorLabel(UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
                            throw e;
                        } else {
                            throw mongoException;
                        }
                    }
                    throw e;
                }
                if (transactionState == TransactionState.IN) {
                    while (true) {
                        try {
                            commitTransaction(false);
                            break;
                        } catch (MongoException mongoException) {
                            if (mongoException.hasErrorLabel(UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)
                                    && !(mongoException instanceof MongoExecutionTimeoutException)) {
                                if (withTransactionTimeoutExpired.getAsBoolean()) {
                                    throw wrapInMongoTimeoutException(mongoException, timeoutMsConfigured);
                                }
                                applyMajorityWriteConcernToTransactionOptions();
                                continue;
                            } else if (mongoException.hasErrorLabel(TRANSIENT_TRANSACTION_ERROR_LABEL)) {
                                if (transactionSpan != null) {
                                    transactionSpan.spanFinalizing(true);
                                }
                                lastError = mongoException;
                                continue outer;
                            }
                            throw mongoException;
                        }
                    }
                }
                return retVal;
            }
        } finally {
            if (transactionSpan != null) {
                transactionSpan.spanFinalizing(true);
            }
        }
    }

    @Override
    @Nullable
    public TransactionSpan getTransactionSpan() {
        return transactionSpan;
    }

    @Override
    public void close() {
        try {
            abortIfInTransaction();
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

    private static void backoff(final int transactionAttempt,
            final Timeout withTransactionTimeout, final MongoException lastError, final boolean timeoutMsConfigured) {
        long backoffMs = ExponentialBackoff.calculateTransactionBackoffMs(transactionAttempt);
        withTransactionTimeout.shortenBy(backoffMs, TimeUnit.MILLISECONDS).onExpired(() -> {
            throw wrapInMongoTimeoutException(lastError, timeoutMsConfigured);
        });
        try {
            if (backoffMs > 0) {
                Thread.sleep(backoffMs);
            }
        } catch (InterruptedException e) {
            throw interruptAndCreateMongoInterruptedException("Transaction retry interrupted", e);
        }
    }

    private static MongoClientException wrapInMongoTimeoutException(final MongoException cause, final boolean timeoutMsConfigured) {
        MongoClientException timeoutException = timeoutMsConfigured
                ? createMongoTimeoutException(cause)
                : wrapInNonTimeoutMsMongoTimeoutException(cause);
        //TODO-JAVA-6154 constructor should be used. 
        if (timeoutException != cause) {
            cause.getErrorLabels().forEach(timeoutException::addLabel);
        }
        return timeoutException;
    }

    private static MongoClientException wrapInNonTimeoutMsMongoTimeoutException(final MongoException cause) {
        return cause instanceof MongoTimeoutException
                ? (MongoTimeoutException) cause
                : new WithTransactionTimeoutException("Operation exceeded the timeout limit.", cause);
    }
}
