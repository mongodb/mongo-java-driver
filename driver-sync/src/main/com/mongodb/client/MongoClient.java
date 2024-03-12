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

package com.mongodb.client;

import com.mongodb.annotations.Immutable;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.event.ClusterListener;

import java.io.Closeable;

/**
 * A client-side representation of a MongoDB cluster.  Instances can represent either a standalone MongoDB instance, a replica set,
 * or a sharded cluster.  Instance of this class are responsible for maintaining an up-to-date state of the cluster,
 * and possibly cache resources related to this, including background threads for monitoring, and connection pools.
 * <p>
 * Instances of this class serve as factories for {@link MongoDatabase} instances.
 * </p>
 * <p>
 * Instances of this class can be created via the {@link MongoClients} factory.
 * </p>
 * @see MongoClients
 * @since 3.7
 */
@Immutable
public interface MongoClient extends MongoClientOperations, Closeable {

    /**
     * Close the client, which will close all underlying cached resources, including, for example,
     * sockets and background monitoring threads.
     */
    void close();

    /**
     * Gets the current cluster description.
     *
     * <p>
     * This method will not block, meaning that it may return a {@link ClusterDescription} whose {@code clusterType} is unknown
     * and whose {@link com.mongodb.connection.ServerDescription}s are all in the connecting state.  If the application requires
     * notifications after the driver has connected to a member of the cluster, it should register a {@link ClusterListener} via
     * the {@link ClusterSettings} in {@link com.mongodb.MongoClientSettings}.
     * </p>
     *
     * @return the current cluster description
     * @see ClusterSettings.Builder#addClusterListener(ClusterListener)
     * @see com.mongodb.MongoClientSettings.Builder#applyToClusterSettings(com.mongodb.Block)
     * @since 3.11
     */
    ClusterDescription getClusterDescription();
}
