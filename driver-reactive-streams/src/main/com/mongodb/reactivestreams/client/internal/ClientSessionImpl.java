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
 *
 */

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.Block;
import com.mongodb.ClientSessionOptions;
import com.mongodb.ServerAddress;
import com.mongodb.TransactionOptions;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.session.ServerSession;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.reactivestreams.Publisher;


@SuppressWarnings("deprecation")
class ClientSessionImpl implements ClientSession {
    private final com.mongodb.async.client.ClientSession wrapped;
    private final Object originator;

    ClientSessionImpl(final com.mongodb.async.client.ClientSession wrapped, final Object originator) {
        this.wrapped = wrapped;
        this.originator = originator;
    }

    @Override
    public boolean hasActiveTransaction() {
        return wrapped.hasActiveTransaction();
    }

    @Override
    public TransactionOptions getTransactionOptions() {
        return wrapped.getTransactionOptions();
    }

    @Override
    public com.mongodb.async.client.ClientSession getWrapped() {
        return wrapped;
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
    public Publisher<Void> commitTransaction() {
        return new SingleResultObservableToPublisher<Void>(
                new Block<com.mongodb.async.SingleResultCallback<Void>>() {
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<Void> callback) {
                        wrapped.commitTransaction(callback);
                    }
                });
    }

    @Override
    public Publisher<Void> abortTransaction() {
        return new SingleResultObservableToPublisher<Void>(
                new Block<com.mongodb.async.SingleResultCallback<Void>>() {
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<Void> callback) {
                        wrapped.abortTransaction(callback);
                    }
                });
    }

    @Override
    public ServerAddress getPinnedServerAddress() {
        return wrapped.getPinnedServerAddress();
    }

    @Override
    public void setPinnedServerAddress(final ServerAddress serverAddress) {
        wrapped.setPinnedServerAddress(serverAddress);
    }

    @Override
    public BsonDocument getRecoveryToken() {
        return wrapped.getRecoveryToken();
    }

    @Override
    public void setRecoveryToken(final BsonDocument bsonDocument) {
        wrapped.setRecoveryToken(bsonDocument);
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
}
