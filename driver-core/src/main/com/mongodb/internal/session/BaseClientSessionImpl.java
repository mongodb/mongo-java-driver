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
import com.mongodb.ServerAddress;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.binding.ReferenceCounted;
import com.mongodb.lang.Nullable;
import com.mongodb.session.ClientSession;
import com.mongodb.session.ServerSession;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.internal.operation.WriteConcernHelper.cloneWithoutTimeout;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class BaseClientSessionImpl implements ClientSession {
    private static final String CLUSTER_TIME_KEY = "clusterTime";

    private final ServerSessionPool serverSessionPool;
    private ServerSession serverSession;
    private final Object originator;
    private final ClientSessionOptions options;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private BsonDocument clusterTime;
    private BsonTimestamp operationTime;
    private BsonTimestamp snapshotTimestamp;
    private ServerAddress pinnedServerAddress;
    private BsonDocument recoveryToken;
    private ReferenceCounted transactionContext;
    @Nullable
    private TimeoutContext timeoutContext;

    private TransactionOptions transactionOptions;

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

    protected void setTransactionOptions(final TimeoutContext timeoutContext, final TransactionOptions combinedTransactionOptions) {
        WriteConcern writeConcern = combinedTransactionOptions.getWriteConcern();
        if (timeoutContext.hasTimeoutMS() && writeConcern != null) {
            WriteConcern writeConcernWithoutTimeout = cloneWithoutTimeout(writeConcern);

            this.transactionOptions = TransactionOptions.merge(
                    TransactionOptions.builder().writeConcern(writeConcernWithoutTimeout).build(),
                    combinedTransactionOptions);
            return;
        }
        this.transactionOptions = combinedTransactionOptions;
    }

    protected void setTransactionOptions(@Nullable final TransactionOptions transactionOptions) {
           this.transactionOptions = transactionOptions;
    }

    protected void resetTimeout() {
        if (timeoutContext != null && timeoutContext.hasTimeoutMS()) {
            timeoutContext.resetTimeout();
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

    @Nullable
    protected TransactionOptions getTransactionOptionsInternal() {
        return transactionOptions;
    }

    protected enum TransactionState {
        NONE, IN, COMMITTED, ABORTED
    }
}
