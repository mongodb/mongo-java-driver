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
     *
     * @return the session id
     */
    BsonDocument getSessionId();

    /**
     *
     * @return the next transaction number for the session
     */
    long advanceTransactionNumber();

    /**
     * @param operationTime the new operation time time
     */
    void advanceOperationTime(BsonTimestamp operationTime);

    /**
     * @return the cluster time
     */
    BsonDocument getClusterTime();

    /**
     * @param clusterTime the new cluster time
     */
    void advanceClusterTime(BsonDocument clusterTime);
}
