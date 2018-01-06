/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.session;

import com.mongodb.ClientSessionOptions;
import com.mongodb.annotations.NotThreadSafe;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import java.io.Closeable;

/**
 * A client session.
 *
 * @mongodb.server.release 3.6
 * @since 3.6
 * @see ClientSessionOptions
 */
@NotThreadSafe
public interface ClientSession extends Closeable {

    /**
     * Get the options for this session.
     *
     * @return the options, which may not be null
     */
    ClientSessionOptions getOptions();

    /**
     * Returns true if operations in this session must be causally consistent
     *
     * @return whether operations in this session must be causally consistent.
     */
    boolean isCausallyConsistent();

    /**
     * Gets the originator for the session.
     *
     * <p>
     * Important because sessions must only be used by their own originator.
     * </p>
     *
     * @return the sessions originator
     */
    Object getOriginator();

    /**
     *
     * @return the server session
     */
    ServerSession getServerSession();

    /**
     * Gets the operation time of the last operation executed in this session.
     *
     * @return the operation time
     */
    BsonTimestamp getOperationTime();

    /**
     * Set the operation time of the last operation executed in this session.
     *
     * @param operationTime the operation time
     */
    void advanceOperationTime(BsonTimestamp operationTime);

    /**
     * @param clusterTime the cluster time to advance to
     */
    void advanceClusterTime(BsonDocument clusterTime);

    /**
     * @return the latest cluster time seen by this session
     */
    BsonDocument getClusterTime();

    @Override
    void close();
}
