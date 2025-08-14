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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.operation.AbortTransactionOperation;
import com.mongodb.internal.operation.CommitTransactionOperation;
import com.mongodb.internal.operation.ReadOperation;
import com.mongodb.internal.operation.WriteConcernHelper;
import com.mongodb.internal.operation.WriteOperation;
import com.mongodb.internal.session.BaseClientSessionImpl;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import static com.mongodb.MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL;
import static com.mongodb.MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;

final class ClientSessionPublisherImpl extends BaseClientSessionImpl implements ClientSession {

    private final MongoClientImpl mongoClient;
    private final OperationExecutor executor;
    private TransactionState transactionState = TransactionState.NONE;
    private boolean messageSentInCurrentTransaction;
    private boolean commitInProgress;
    private TransactionOptions transactionOptions;


    ClientSessionPublisherImpl(final ServerSessionPool serverSessionPool, final MongoClientImpl mongoClient,
            final ClientSessionOptions options, final OperationExecutor executor) {
        super(serverSessionPool, mongoClient, options);
        this.executor = executor;
        this.mongoClient = mongoClient;
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
        notNull("transactionOptions", transactionOptions);

        Boolean snapshot = getOptions().isSnapshot();
        if (snapshot != null && snapshot) {
            throw new IllegalArgumentException("Transactions are not supported in snapshot sessions");
        }
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

        TimeoutContext timeoutContext = createTimeoutContext();
        WriteConcern writeConcern = getWriteConcern(timeoutContext);
        if (writeConcern == null) {
            throw new MongoInternalException("Invariant violated. Transaction options write concern can not be null");
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

    @Override
    public Publisher<Void> commitTransaction() {
        if (transactionState == TransactionState.ABORTED) {
            throw new IllegalStateException("Cannot call commitTransaction after calling abortTransaction");
        }
        if (transactionState == TransactionState.NONE) {
            throw new IllegalStateException("There is no transaction started");
        }
        if (!messageSentInCurrentTransaction) {
            cleanupTransaction(TransactionState.COMMITTED);
            return Mono.create(MonoSink::success);
        } else {
            ReadConcern readConcern = transactionOptions.getReadConcern();
            if (readConcern == null) {
                throw new MongoInternalException("Invariant violated. Transaction options read concern can not be null");
            }
            boolean alreadyCommitted = commitInProgress || transactionState == TransactionState.COMMITTED;
            commitInProgress = true;
            resetTimeout();
            TimeoutContext timeoutContext = getTimeoutContext();
            WriteConcern writeConcern = assertNotNull(getWriteConcern(timeoutContext));
            return executor
                    .execute(
                            new CommitTransactionOperation(writeConcern, alreadyCommitted)
                                    .recoveryToken(getRecoveryToken()), readConcern, this)
                    .doOnTerminate(() -> {
                        commitInProgress = false;
                        transactionState = TransactionState.COMMITTED;
                    })
                    .doOnError(MongoException.class, this::clearTransactionContextOnError);
        }
    }

    @Override
    public Publisher<Void> abortTransaction() {
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
            return Mono.create(MonoSink::success);
        } else {
            ReadConcern readConcern = transactionOptions.getReadConcern();
            if (readConcern == null) {
                throw new MongoInternalException("Invariant violated. Transaction options read concern can not be null");
            }

            resetTimeout();
            TimeoutContext timeoutContext = getTimeoutContext();
            WriteConcern writeConcern = assertNotNull(getWriteConcern(timeoutContext));
            return executor
                    .execute(new AbortTransactionOperation(writeConcern)
                                    .recoveryToken(getRecoveryToken()), readConcern, this)
                    .onErrorResume(Throwable.class, (e) -> Mono.empty())
                    .doOnTerminate(() -> {
                        clearTransactionContext();
                        cleanupTransaction(TransactionState.ABORTED);
                    });
        }
    }

    private void clearTransactionContextOnError(final MongoException e) {
        if (e.hasErrorLabel(TRANSIENT_TRANSACTION_ERROR_LABEL) || e.hasErrorLabel(UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
            clearTransactionContext();
        }
    }

    @Override
    public void close() {
        if (transactionState == TransactionState.IN) {
            Mono.from(abortTransaction()).doFinally(it -> super.close()).subscribe();
        } else {
            super.close();
        }
    }

    private void cleanupTransaction(final TransactionState nextState) {
        messageSentInCurrentTransaction = false;
        transactionOptions = null;
        transactionState = nextState;
        setTimeoutContext(null);
    }

    private TimeoutContext createTimeoutContext() {
        return new TimeoutContext(getTimeoutSettings(transactionOptions, executor.getTimeoutSettings()));
    }
}
