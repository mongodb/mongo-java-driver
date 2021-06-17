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

import com.mongodb.session.ClientSession;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import static com.mongodb.assertions.Assertions.notNull;

public abstract class ClientSessionContext implements SessionContext {

    private ClientSession clientSession;

    public ClientSessionContext(final ClientSession clientSession) {
        this.clientSession = notNull("clientSession", clientSession);
    }

    public ClientSession getClientSession() {
        return clientSession;
    }

    @Override
    public boolean hasSession() {
        return true;
    }

    @Override
    public BsonDocument getSessionId() {
        return clientSession.getServerSession().getIdentifier();
    }

    @Override
    public boolean isCausallyConsistent() {
        return clientSession.isCausallyConsistent();
    }

    @Override
    public long getTransactionNumber() {
        return clientSession.getServerSession().getTransactionNumber();
    }

    @Override
    public long advanceTransactionNumber() {
        return clientSession.getServerSession().advanceTransactionNumber();
    }

    @Override
    public BsonTimestamp getOperationTime() {
        return clientSession.getOperationTime();
    }

    @Override
    public void advanceOperationTime(final BsonTimestamp operationTime) {
        clientSession.advanceOperationTime(operationTime);
    }

    @Override
    public BsonDocument getClusterTime() {
        return clientSession.getClusterTime();
    }

    @Override
    public void advanceClusterTime(final BsonDocument clusterTime) {
        clientSession.advanceClusterTime(clusterTime);
    }

    @Override
    public boolean isSnapshot() {
        Boolean snapshot = clientSession.getOptions().isSnapshot();
        return snapshot != null && snapshot;
    }

    @Override
    public void setSnapshotTimestamp(final BsonTimestamp snapshotTimestamp) {
        clientSession.setSnapshotTimestamp(snapshotTimestamp);
    }

    @Override
    public BsonTimestamp getSnapshotTimestamp() {
        return clientSession.getSnapshotTimestamp();
    }

    @Override
    public void setRecoveryToken(final BsonDocument recoveryToken) {
        clientSession.setRecoveryToken(recoveryToken);
    }

    @Override
    public void clearTransactionContext() {
        clientSession.clearTransactionContext();
    }

    @Override
    public void markSessionDirty() {
        clientSession.getServerSession().markDirty();
    }

    @Override
    public boolean isSessionMarkedDirty() {
        return clientSession.getServerSession().isMarkedDirty();
    }
}
