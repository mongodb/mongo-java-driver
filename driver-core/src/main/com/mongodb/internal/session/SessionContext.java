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

import com.mongodb.ReadConcern;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public interface SessionContext {

    /**
     * Returns true if there is a true server session associated with this context.
     */
    boolean hasSession();

    /**
     * Returns true if the session is implicit, and false if the application started the session explicitly.
     */
    boolean isImplicitSession();

    /**
     * Gets the session identifier if this context has a session backing it.
     */
    BsonDocument getSessionId();

    boolean isCausallyConsistent();

    long getTransactionNumber();

    /**
     * Advance the transaction number.
     *
     * @return the next transaction number for the session
     */
    long advanceTransactionNumber();

    /**
     *  Notify the session context that a message has been sent.
     *
     * @return true if this is the first message sent, false otherwise
     */
    boolean notifyMessageSent();

    /**
     * Gets the current operation time for this session context
     *
     * @return the current operation time, which may be null
     */
    BsonTimestamp getOperationTime();

    /**
     * Advance the operation time.  If the current operation time is greater than the given operation time, this method has no effect.
     *
     * @param operationTime the new operation time
     */
    void advanceOperationTime(BsonTimestamp operationTime);

    /**
     * Gets the current cluster time for this session context.
     *
     * @return the cluster time, which may be null
     */
    BsonDocument getClusterTime();

    /**
     * Advance the cluster time. If the current cluster time is greater than the given cluster time, this method has no effect.
     *
     * @param clusterTime the new cluster time
     */
    void advanceClusterTime(BsonDocument clusterTime);

    boolean isSnapshot();

    void setSnapshotTimestamp(BsonTimestamp snapshotTimestamp);

    @Nullable
    BsonTimestamp getSnapshotTimestamp();

    boolean hasActiveTransaction();

    ReadConcern getReadConcern();

    void setRecoveryToken(BsonDocument recoveryToken);

    /**
     * Unpin a mongos from a session.
     */
    void clearTransactionContext();

    /**
     * Mark the session as dirty. This happens when a command fails with a network
     * error. Dirty sessions are later discarded from the server session pool.
     */
    void markSessionDirty();

    boolean isSessionMarkedDirty();
}
