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
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

/**
 * A SessionContext implementation that does nothing and reports that it has no session.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class NoOpSessionContext implements SessionContext {

    /**
     * A singleton instance of a NoOpSessionContext
     */
    public static final NoOpSessionContext INSTANCE = new NoOpSessionContext();

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
        return false;
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
        return false;
    }

    @Nullable
    @Override
    public BsonTimestamp getOperationTime() {
        return null;
    }

    @Override
    public void advanceOperationTime(@Nullable final BsonTimestamp operationTime) {
    }

    @Nullable
    @Override
    public BsonDocument getClusterTime() {
        return null;
    }

    @Override
    public void advanceClusterTime(@Nullable final BsonDocument clusterTime) {
    }

    @Override
    public boolean isSnapshot() {
        return false;
    }

    @Override
    public void setSnapshotTimestamp(@Nullable final BsonTimestamp snapshotTimestamp) {
    }

    @Override
    @Nullable
    public BsonTimestamp getSnapshotTimestamp() {
        return null;
    }

    @Override
    public boolean hasActiveTransaction() {
        return false;
    }

    @Override
    public ReadConcern getReadConcern() {
        return ReadConcern.DEFAULT;
    }

    @Override
    public void setRecoveryToken(final BsonDocument recoveryToken) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearTransactionContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void markSessionDirty() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSessionMarkedDirty() {
        return false;
    }
}
