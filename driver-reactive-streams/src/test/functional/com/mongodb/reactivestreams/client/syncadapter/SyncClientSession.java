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

package com.mongodb.reactivestreams.client.syncadapter;

import com.mongodb.ClientSessionOptions;
import com.mongodb.ServerAddress;
import com.mongodb.TransactionOptions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.TransactionBody;
import com.mongodb.session.ServerSession;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import reactor.core.publisher.Mono;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;

class SyncClientSession implements ClientSession {
    private final com.mongodb.reactivestreams.client.ClientSession wrapped;
    private final Object originator;

    SyncClientSession(final com.mongodb.reactivestreams.client.ClientSession wrapped, final Object originator) {
        this.wrapped = wrapped;
        this.originator = originator;
    }

    public com.mongodb.reactivestreams.client.ClientSession getWrapped() {
        return wrapped;
    }

    @Override
    public ServerAddress getPinnedServerAddress() {
        return wrapped.getPinnedServerAddress();
    }

    @Override
    public Object getTransactionContext() {
        return wrapped.getTransactionContext();
    }

    @Override
    public void setTransactionContext(final ServerAddress address, final Object transactionContext) {
        wrapped.setTransactionContext(address, transactionContext);
    }

    @Override
    public void clearTransactionContext() {
        wrapped.clearTransactionContext();
    }

    @Override
    public BsonDocument getRecoveryToken() {
        return wrapped.getRecoveryToken();
    }

    @Override
    public void setRecoveryToken(final BsonDocument recoveryToken) {
        wrapped.setRecoveryToken(recoveryToken);
    }

    @Override
    public ClientSessionOptions getOptions() {
        return wrapped.getOptions();
    }

    @Override
    public boolean isCausallyConsistent() {
        return wrapped.isCausallyConsistent();
    }

    @Override
    public Object getOriginator() {
        return originator;
    }

    @Override
    public ServerSession getServerSession() {
        return wrapped.getServerSession();
    }

    @Override
    public BsonTimestamp getOperationTime() {
        return wrapped.getOperationTime();
    }

    @Override
    public void advanceOperationTime(final BsonTimestamp operationTime) {
        wrapped.advanceOperationTime(operationTime);
    }

    @Override
    public void advanceClusterTime(final BsonDocument clusterTime) {
        wrapped.advanceClusterTime(clusterTime);
    }

    @Override
    public BsonDocument getClusterTime() {
        return wrapped.getClusterTime();
    }

    @Override
    public void close() {
        wrapped.close();
    }

    @Override
    public boolean hasActiveTransaction() {
        return wrapped.hasActiveTransaction();
    }

    @Override
    public boolean notifyMessageSent() {
        return wrapped.notifyMessageSent();
    }

    @Override
    public void notifyOperationInitiated(final Object operation) {
        wrapped.notifyOperationInitiated(operation);
    }

    @Override
    public TransactionOptions getTransactionOptions() {
        return wrapped.getTransactionOptions();
    }

    @Override
    public void startTransaction() {
        wrapped.startTransaction();
    }

    @Override
    public void startTransaction(final TransactionOptions transactionOptions) {
        wrapped.startTransaction(transactionOptions);
    }

    @Override
    public void commitTransaction() {
        Mono.from(wrapped.commitTransaction()).block(TIMEOUT_DURATION);
    }

    @Override
    public void abortTransaction() {
        Mono.from(wrapped.abortTransaction()).block(TIMEOUT_DURATION);
    }

    @Override
    public <T> T withTransaction(final TransactionBody<T> transactionBody) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T withTransaction(final TransactionBody<T> transactionBody, final TransactionOptions options) {
        throw new UnsupportedOperationException();
    }
}
