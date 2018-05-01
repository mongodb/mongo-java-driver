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

package com.mongodb.connection;

import com.mongodb.ReadConcern;
import com.mongodb.session.SessionContext;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

class TestSessionContext implements SessionContext {

    private BsonDocument clusterTime;
    private BsonTimestamp operationTime;

    TestSessionContext(final BsonDocument initialClusterTime) {
        this.clusterTime = initialClusterTime;
    }

    public BsonTimestamp getOperationTime() {
        return operationTime;
    }

    @Override
    public boolean hasSession() {
        return false;
    }

    @Override
    public boolean isImplicitSession() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BsonDocument getSessionId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCausallyConsistent() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getTransactionNumber() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long advanceTransactionNumber() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean notifyMessageSent() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void advanceOperationTime(final BsonTimestamp operationTime) {
        this.operationTime = operationTime;
    }

    @Override
    public BsonDocument getClusterTime() {
        return clusterTime;
    }

    @Override
    public void advanceClusterTime(final BsonDocument clusterTime) {
        this.clusterTime = clusterTime;
    }

    @Override
    public boolean hasActiveTransaction() {
        return false;
    }

    @Override
    public ReadConcern getReadConcern() {
        return ReadConcern.DEFAULT;
    }
}
