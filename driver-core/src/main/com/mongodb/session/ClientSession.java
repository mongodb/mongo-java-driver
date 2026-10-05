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

package com.mongodb.session;

import com.mongodb.ClientSessionOptions;
import com.mongodb.ServerAddress;
import com.mongodb.annotations.Internal;
import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.lang.Nullable;
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
     * Get the server address of the pinned mongos on this session.
     * For internal use only.
     *
     * @return the server address of the pinned mongos
     */
    @Nullable
    @Internal
    ServerAddress getPinnedServerAddress();

    /**
     * Gets the transaction context.
     *
     * <p>For internal use only </p>
     *
     * @return the transaction context
     */
    @Nullable
    @Internal
    Object getTransactionContext();

    /**
     * Sets the transaction context.
     *
     * <p>For internal use only </p>
     * <p>Implementations may place additional restrictions on the type of the transaction context</p>
     *
     * @param address the server address
     * @param transactionContext the transaction context
     */
    @Internal
    void setTransactionContext(ServerAddress address, Object transactionContext);

    /**
     * Clears the transaction context.
     *
     * <p>For internal use only </p>
     *
     */
    @Internal
    void clearTransactionContext();

    /**
     * Get the recovery token from the latest outcome in a sharded transaction.
     * For internal use only.
     *
     * @return the recovery token
     */
    @Nullable
    @Internal
    BsonDocument getRecoveryToken();

    /**
     * Set the recovery token.
     * For internal use only.
     *
     * @param recoveryToken the recovery token
     */
    @Internal
    void setRecoveryToken(BsonDocument recoveryToken);

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
    void advanceOperationTime(@Nullable BsonTimestamp operationTime);

    /**
     * @param clusterTime the cluster time to advance to
     */
    void advanceClusterTime(@Nullable BsonDocument clusterTime);

    /**
     * For internal use only.
     *
     * @param snapshotTimestamp the snapshot timestamp
     */
    @Internal
    void setSnapshotTimestamp(@Nullable BsonTimestamp snapshotTimestamp);

    /**
     * For internal use only.
     *
     * @return the snapshot timestamp
     */
    @Nullable
    @Internal
    BsonTimestamp getSnapshotTimestamp();

    /**
     * @return the latest cluster time seen by this session
     */
    BsonDocument getClusterTime();

    @Override
    void close();

    /**
     * Gets the timeout context to use with this session:
     *
     * <ul>
     *   <li>{@code MongoClientSettings#getTimeoutMS}</li>
     *   <li>{@code ClientSessionOptions#getDefaultTimeout}</li>
     * </ul>
     * <p>For internal use only </p>
     * @return the timeout to use
     */
    @Nullable
    @Internal
    TimeoutContext getTimeoutContext();

    /**
     * For internal use only.
     *
     * @return The {@link ClientSession}-scoped state of the overload retry policy.
     */
    @Internal
    Object getOverloadRetryPolicyState();
}
