/*
 * Copyright 2017 MongoDB, Inc.
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

package com.mongodb.connection;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

/**
 * The session context.
 *
 * @since 3.6
 */
public interface SessionContext {

    /**
     * Returns true if there is a true server session associated with this context.
     *
     * @return true if there is a true server session associated with this context.
     */
    boolean hasSession();

    /**
     * Gets the session identifier if this context has a session backing it.
     *
     * @return the session id
     */
    BsonDocument getSessionId();

    /**
     * Gets whether this context is associated with a causally consistent session.
     *
     * @return true ift his context is associated with a causally consistent session
     */
    boolean isCausallyConsistent();

    /**
     * Advance the transaction number.
     *
     * @return the next transaction number for the session
     */
    long advanceTransactionNumber();

    /**
     * Gets the current operation time for this session context
     *
     * @return the current operation time, which may be null
     */
    BsonTimestamp getOperationTime();

    /**
     * Advance the operation time.  If the current operation time is greater than the given operation time, this method has no effect.
     *
     * @param operationTime the new operation time time
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
}
