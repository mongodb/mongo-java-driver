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

package com.mongodb.internal.connection;

import com.mongodb.ReadConcern;
import com.mongodb.session.SessionContext;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

public final class ClusterClockAdvancingSessionContext implements SessionContext {

    private final SessionContext wrapped;
    private final ClusterClock clusterClock;

    public ClusterClockAdvancingSessionContext(final SessionContext wrapped, final ClusterClock clusterClock) {
        this.wrapped = wrapped;
        this.clusterClock = clusterClock;
    }

    @Override
    public boolean hasSession() {
        return wrapped.hasSession();
    }

    @Override
    public boolean isImplicitSession() {
        return wrapped.isImplicitSession();
    }

    @Override
    public BsonDocument getSessionId() {
        return wrapped.getSessionId();
    }

    @Override
    public boolean isCausallyConsistent() {
        return wrapped.isCausallyConsistent();
    }

    @Override
    public long getTransactionNumber() {
        return wrapped.getTransactionNumber();
    }

    @Override
    public long advanceTransactionNumber() {
        return wrapped.advanceTransactionNumber();
    }

    @Override
    public boolean notifyMessageSent() {
        return wrapped.notifyMessageSent();
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
    public BsonDocument getClusterTime() {
        return clusterClock.greaterOf(wrapped.getClusterTime());
    }

    @Override
    public void advanceClusterTime(final BsonDocument clusterTime) {
        wrapped.advanceClusterTime(clusterTime);
        clusterClock.advance(clusterTime);
    }

    @Override
    public boolean hasActiveTransaction() {
        return wrapped.hasActiveTransaction();
    }

    @Override
    public ReadConcern getReadConcern() {
        return wrapped.getReadConcern();
    }
}
