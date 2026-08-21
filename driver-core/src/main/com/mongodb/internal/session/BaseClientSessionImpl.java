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

package com.mongodb.internal.session;

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.async.function.RetryControl;
import com.mongodb.internal.async.function.RetryPolicy;
import com.mongodb.internal.binding.ReferenceCounted;
import com.mongodb.lang.Nullable;
import com.mongodb.session.ClientSession;
import com.mongodb.session.ServerSession;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.fail;
import static com.mongodb.assertions.Assertions.isTrue;
import static java.lang.Boolean.TRUE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public class BaseClientSessionImpl implements ClientSession {
    private static final String CLUSTER_TIME_KEY = "clusterTime";

    private final ServerSessionPool serverSessionPool;
    private ServerSession serverSession;
    private final Object originator;
    private final ClientSessionOptions options;
    private final DefaultOverloadRetryPolicyState overloadRetryPolicyState = new DefaultOverloadRetryPolicyState();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private BsonDocument clusterTime;
    private BsonTimestamp operationTime;
    private BsonTimestamp snapshotTimestamp;
    private ServerAddress pinnedServerAddress;
    private BsonDocument recoveryToken;
    private ReferenceCounted transactionContext;
    @Nullable
    private TimeoutContext timeoutContext;

    protected static boolean hasTimeoutMS(@Nullable final TimeoutContext timeoutContext) {
        return timeoutContext != null && timeoutContext.hasTimeoutMS();
    }

    protected static boolean hasWTimeoutMS(@Nullable final WriteConcern writeConcern) {
        return writeConcern != null && writeConcern.getWTimeout(TimeUnit.MILLISECONDS) != null;
    }

    public BaseClientSessionImpl(final ServerSessionPool serverSessionPool, final Object originator, final ClientSessionOptions options) {
        this.serverSessionPool = serverSessionPool;
        this.originator = originator;
        this.options = options;
        this.pinnedServerAddress = null;
    }

    @Override
    @Nullable
    public ServerAddress getPinnedServerAddress() {
        return pinnedServerAddress;
    }

    @Override
    public Object getTransactionContext() {
        return transactionContext;
    }

    @Override
    public void setTransactionContext(final ServerAddress address, final Object transactionContext) {
        assertTrue(transactionContext instanceof ReferenceCounted);
        pinnedServerAddress = address;
        this.transactionContext = (ReferenceCounted) transactionContext;
        this.transactionContext.retain();
    }

    @Override
    public void clearTransactionContext() {
        pinnedServerAddress = null;
        if (transactionContext != null) {
            transactionContext.release();
            transactionContext = null;
        }
    }

    @Override
    public BsonDocument getRecoveryToken() {
        return recoveryToken;
    }

    @Override
    public void setRecoveryToken(final BsonDocument recoveryToken) {
        this.recoveryToken = recoveryToken;
    }

    @Override
    public ClientSessionOptions getOptions() {
        return options;
    }

    @Override
    public boolean isCausallyConsistent() {
        Boolean causallyConsistent = options.isCausallyConsistent();
        return causallyConsistent == null || causallyConsistent;
    }

    @Override
    public Object getOriginator() {
        return originator;
    }

    @Override
    public BsonDocument getClusterTime() {
        return clusterTime;
    }

    @Override
    public BsonTimestamp getOperationTime() {
        return operationTime;
    }

    @Override
    public ServerSession getServerSession() {
        isTrue("open", !closed.get());
        if (serverSession == null) {
            serverSession = serverSessionPool.get();
        }
        return serverSession;
    }

    @Override
    public void advanceOperationTime(@Nullable final BsonTimestamp newOperationTime) {
        isTrue("open", !closed.get());
        this.operationTime = greaterOf(newOperationTime);
    }

    @Override
    public void advanceClusterTime(@Nullable final BsonDocument newClusterTime) {
        isTrue("open", !closed.get());
        this.clusterTime = greaterOf(newClusterTime);
    }

    @Override
    public void setSnapshotTimestamp(@Nullable final BsonTimestamp snapshotTimestamp) {
        isTrue("open", !closed.get());
        if (snapshotTimestamp != null) {
            if (this.snapshotTimestamp != null && !snapshotTimestamp.equals(this.snapshotTimestamp)) {
                throw new MongoClientException("Snapshot timestamps should not change during the lifetime of the session.  Current "
                        + "timestamp is " + this.snapshotTimestamp + ", and attempting to set it to " + snapshotTimestamp);
            }
            this.snapshotTimestamp = snapshotTimestamp;
        }
    }

    @Override
    @Nullable
    public BsonTimestamp getSnapshotTimestamp() {
        isTrue("open", !closed.get());
        return snapshotTimestamp;
    }

    private BsonDocument greaterOf(@Nullable final BsonDocument newClusterTime) {
        if (newClusterTime == null) {
            return clusterTime;
        } else if (clusterTime == null) {
            return newClusterTime;
        } else {
            return newClusterTime.getTimestamp(CLUSTER_TIME_KEY).compareTo(clusterTime.getTimestamp(CLUSTER_TIME_KEY)) > 0
                    ? newClusterTime : clusterTime;
        }
    }

    private BsonTimestamp greaterOf(@Nullable final BsonTimestamp newOperationTime) {
        if (newOperationTime == null) {
            return operationTime;
        } else if (operationTime == null) {
            return newOperationTime;
        } else {
            return newOperationTime.compareTo(operationTime) > 0 ? newOperationTime : operationTime;
        }
    }

    @Override
    public void close() {
        // While the interface implemented by this class  is documented as not thread safe, it's still useful to provide thread safety here
        // in order to prevent the code within the conditional from executing more than once. Doing so protects the server session pool from
        // corruption, by preventing the same server session from being released to the pool more than once.
        if (closed.compareAndSet(false, true)) {
            if (serverSession != null) {
                serverSessionPool.release(serverSession);
            }
            clearTransactionContext();
        }
    }

    @Override
    @Nullable
    public TimeoutContext getTimeoutContext() {
        return timeoutContext;
    }

    protected void setTimeoutContext(@Nullable final TimeoutContext timeoutContext) {
        this.timeoutContext = timeoutContext;
    }

    protected void resetTimeout() {
        if (timeoutContext != null) {
            timeoutContext = timeoutContext.withNewlyStartedTimeout();
        }
    }

    protected TimeoutSettings getTimeoutSettings(final TransactionOptions transactionOptions, final TimeoutSettings timeoutSettings) {
        Long transactionTimeoutMS = transactionOptions.getTimeout(MILLISECONDS);
        Long defaultTimeoutMS = getOptions().getDefaultTimeout(MILLISECONDS);
        Long clientTimeoutMS =  timeoutSettings.getTimeoutMS();

        Long timeoutMS = transactionTimeoutMS != null ? transactionTimeoutMS
                : defaultTimeoutMS != null ? defaultTimeoutMS : clientTimeoutMS;

        return timeoutSettings
                .withMaxCommitMS(transactionOptions.getMaxCommitTime(MILLISECONDS))
                .withTimeout(timeoutMS, MILLISECONDS);
    }

    @Override
    public OverloadRetryPolicyState getOverloadRetryPolicyState() {
        return overloadRetryPolicyState;
    }

    protected enum TransactionState {
        NONE,
        /**
         * TODO-JAVA-6126 We miss the `STARTING` state, it is combined with the {@link #IN} state.
         * See <a href="https://jira.mongodb.org/browse/JAVA-6126">JAVA-6126</a> for more details.
         */
        IN,
        COMMITTED,
        ABORTED
    }

    /**
     * The {@link ClientSession}-scoped state of the overload retry policy.
     * <p>
     * This class is not part of the public API and may be removed or changed at any time.
     */
    public interface OverloadRetryPolicyState {
        OverloadRetryPolicyState NO_OP = new NoOpOverloadRetryPolicyState();

        /**
         * @see #getCommandExecutionScoped()
         */
        void openCommandExecutionScope();

        /**
         * @see #openCommandExecutionScope()
         * @see #closeCommandExecutionScope()
         *
         * @return Non-{@code null} iff a command execution is in progress in the session and the overload retry policy is in use.
         */
        @Nullable
        CommandExecutionScoped getCommandExecutionScoped();

        /**
         * @see #getCommandExecutionScoped()
         */
        void closeCommandExecutionScope();

        /**
         * @see #closeCommitScope()
         */
        void openCommitScope();

        /**
         * @see #openCommitScope()
         * @see #closeCommitScope()
         *
         * @return Non-{@code null} iff a sequence of commit transaction operations has begun and has not ended.
         */
        @Nullable
        CommitScoped getCommitScoped();

        /**
         * @see #openCommitScope()
         */
        void closeCommitScope();

        /**
         * A part of {@link OverloadRetryPolicyState} restricted to the execution of a command
         * (a command execution may involve multiple execution attempts).
         * <p>
         * This class is not part of the public API and may be removed or changed at any time.
         *
         * @see #getCommandExecutionScoped()
         */
        interface CommandExecutionScoped {
            /**
             * @return See {@link SessionContext#notifyMessageSent()}.
             */
            boolean notifyMessageSent(boolean firstMessageInTransaction);

            /**
             * {@link CommandExecutionScoped} learns that the session is trying to start a transaction via {@link #notifyMessageSent(boolean)}.
             * Then, usually, when an execution attempt of the corresponding command succeeds, the whole command execution completes,
             * {@link OverloadRetryPolicyState#closeCommandExecutionScope() closing} the command execution scope.
             * However, in those situations where {@link RetryControl#doWhileDisabled(Supplier)} is used to execute another command within
             * an execution attempt of the transaction-starting command,
             * successful sending and receiving a response to the transaction-starting command does not imply
             * a successful completion of the corresponding transaction-starting command execution.
             * In these situations, the {@link RetryPolicy} has no way of knowing that transaction has been started successfully
             * and informing {@link CommandExecutionScoped} that the session is no longer trying to start a transaction.
             * The current method serves that purpose.
             * <p>
             * Note that when we call this method, we do not necessarily know that a transaction has been started,
             * as we may have never needed to start it.
             * We know only that we are not trying to start one from now on within the current command execution scope.
             */
            void onNotTryingToStartTransaction();

            void onAnyAttemptFailure(boolean retryableOverloadError);
        }

        /**
         * A part of {@link OverloadRetryPolicyState} restricted to all adjacent executions of the commit transaction operation
         * (each of those executions may involve the {@code commitTransaction} command execution,
         * which may involve multiple command execution attempts),
         * regardless of whether any of them is initiated directly by an application or internally by the driver.
         * <p>
         * This class is not part of the public API and may be removed or changed at any time.
         *
         * @see #getCommitScoped()
         */
        interface CommitScoped {
            void onAnyAttemptFailure(boolean retryableOverloadError);

            boolean observedErrorsAndTheyAreAllRetryableOverloadErrors();
        }
    }

    private static final class DefaultOverloadRetryPolicyState implements OverloadRetryPolicyState {
        @Nullable
        private DefaultCommandExecutionScoped commandExecutionScoped;
        @Nullable
        private DefaultCommitScoped commitScoped;

        DefaultOverloadRetryPolicyState() {
            commandExecutionScoped = null;
            commitScoped = null;
        }

        @Override
        public void openCommandExecutionScope() {
            assertNull(commandExecutionScoped);
            commandExecutionScoped = new DefaultCommandExecutionScoped();
        }

        @Override
        @Nullable
        public CommandExecutionScoped getCommandExecutionScoped() {
            return commandExecutionScoped;
        }

        @Override
        public void closeCommandExecutionScope() {
            commandExecutionScoped = null;
        }

        @Override
        public void openCommitScope() {
            assertNull(commitScoped);
            commitScoped = new DefaultCommitScoped();
        }

        @Override
        @Nullable
        public CommitScoped getCommitScoped() {
            return commitScoped;
        }

        @Override
        public void closeCommitScope() {
            commitScoped = null;
        }

        @Override
        public String toString() {
            return "DefaultOverloadRetryPolicyState{"
                    + ", commandExecutionScoped=" + commandExecutionScoped
                    + ", commitScoped=" + commitScoped
                    + '}';
        }

        private static final class DefaultCommandExecutionScoped implements CommandExecutionScoped {
            private boolean tryingToStartTransaction;
            /**
             * @see MongoException#RETRYABLE_ERROR_LABEL
             * @see MongoException#SYSTEM_OVERLOADED_ERROR_LABEL
             */
            private boolean observedNoneOrOnlyRetryableOverloadErrors;

            DefaultCommandExecutionScoped() {
                tryingToStartTransaction = false;
                observedNoneOrOnlyRetryableOverloadErrors = true;
            }

            /**
             * @return See {@link SessionContext#notifyMessageSent()}.
             */
            @Override
            public boolean notifyMessageSent(final boolean firstMessageInTransaction) {
                if (firstMessageInTransaction) {
                    assertFalse(tryingToStartTransaction);
                    tryingToStartTransaction = true;
                    return true;
                } else {
                    return tryingToStartTransaction && observedNoneOrOnlyRetryableOverloadErrors;
                }
            }

            @Override
            public void onNotTryingToStartTransaction() {
                tryingToStartTransaction = false;
            }

            @Override
            public void onAnyAttemptFailure(final boolean retryableOverloadError) {
                observedNoneOrOnlyRetryableOverloadErrors &= retryableOverloadError;
            }

            @Override
            public String toString() {
                return "DefaultCommandExecutionScoped{"
                        + "tryingToStartTransaction=" + tryingToStartTransaction
                        + ", observedNoneOrOnlyRetryableOverloadErrors=" + observedNoneOrOnlyRetryableOverloadErrors
                        + '}';
            }
        }

        private static final class DefaultCommitScoped implements CommitScoped {
            /**
             * {@code null} iff no errors have been observed.
             *
             * @see MongoException#RETRYABLE_ERROR_LABEL
             * @see MongoException#SYSTEM_OVERLOADED_ERROR_LABEL
             */
            private Boolean observedErrorsAndTheyAreAllRetryableOverloadErrors;

            DefaultCommitScoped() {
                observedErrorsAndTheyAreAllRetryableOverloadErrors = null;
            }

            @Override
            public void onAnyAttemptFailure(final boolean retryableOverloadError) {
                if (observedErrorsAndTheyAreAllRetryableOverloadErrors == null) {
                    observedErrorsAndTheyAreAllRetryableOverloadErrors = retryableOverloadError;
                } else {
                    observedErrorsAndTheyAreAllRetryableOverloadErrors &= retryableOverloadError;
                }
            }

            @Override
            public boolean observedErrorsAndTheyAreAllRetryableOverloadErrors() {
                return TRUE.equals(observedErrorsAndTheyAreAllRetryableOverloadErrors);
            }

            @Override
            public String toString() {
                return "DefaultCommitScoped{"
                        + "observedErrorsAndTheyAreAllRetryableOverloadErrors=" + observedErrorsAndTheyAreAllRetryableOverloadErrors
                        + '}';
            }
        }
    }

    private static final class NoOpOverloadRetryPolicyState implements OverloadRetryPolicyState {
        NoOpOverloadRetryPolicyState() {
        }

        @Override
        public void openCommandExecutionScope() {
        }

        @Override
        public CommandExecutionScoped getCommandExecutionScoped() {
            return NoOpCommandExecutionScoped.INSTANCE;
        }

        @Override
        public void closeCommandExecutionScope() {
        }

        @Override
        public void openCommitScope() {
        }

        @Override
        @Nullable
        public CommitScoped getCommitScoped() {
            return null;
        }

        @Override
        public void closeCommitScope() {
        }

        private static final class NoOpCommandExecutionScoped implements CommandExecutionScoped {
            static final NoOpCommandExecutionScoped INSTANCE = new NoOpCommandExecutionScoped();

            private NoOpCommandExecutionScoped() {
            }

            @Override
            public boolean notifyMessageSent(final boolean firstMessageInTransaction) {
                throw fail();
            }

            @Override
            public void onNotTryingToStartTransaction() {
            }

            @Override
            public void onAnyAttemptFailure(final boolean retryableOverloadError) {
            }
        }
    }
}
