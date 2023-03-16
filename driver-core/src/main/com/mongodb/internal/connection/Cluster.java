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


import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterId;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.lang.Nullable;
import com.mongodb.selector.ServerSelector;

import java.io.Closeable;

import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;

/**
 * Represents a cluster of MongoDB servers.  Implementations can define the behaviour depending upon the type of cluster.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public interface Cluster extends Closeable {

    ClusterSettings getSettings();

    /**
     * Get the description of this cluster.  This method will not return normally until the cluster type is known.
     *
     * @return a ClusterDescription representing the current state of the cluster
     * @throws com.mongodb.MongoTimeoutException if the timeout has been reached before the cluster type is known
     * @throws com.mongodb.MongoInterruptedException if interrupted when getting the cluster description
     */
    ClusterDescription getDescription();


    ClusterId getClusterId();

    @Nullable
    @VisibleForTesting(otherwise = PRIVATE)
    ClusterableServer getServer(ServerAddress serverAddress);

    /**
     * Get the current description of this cluster.
     *
     * @return the current ClusterDescription representing the current state of the cluster.
     */
    ClusterDescription getCurrentDescription();

    /**
     * Get the {@link ClusterClock} from which one may get the last seen cluster time.
     */
    ClusterClock getClock();

    ServerTuple selectServer(ServerSelector serverSelector, OperationContext operationContext);

    void selectServerAsync(ServerSelector serverSelector, OperationContext operationContext,
            SingleResultCallback<ServerTuple> callback);

    /**
     * Closes connections to the servers in the cluster.  After this is called, this cluster instance can no longer be used.
     */
    void close();

    /**
     * Whether all the servers in the cluster are closed or not.
     *
     * @return true if all the servers in this cluster have been closed
     */
    boolean isClosed();

    /**
     * Does the supplied {@code action} while holding a reentrant cluster-wide lock.
     *
     * @param action The action to {@linkplain Runnable#run() do}.
     */
    void withLock(Runnable action);

    /**
     * This method allows {@link Server}s to notify the {@link Cluster} about changes in their state as per the
     * <a href="https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst">
     * Server Discovery And Monitoring</a> specification.
     */
    void onChange(ServerDescriptionChangedEvent event);
}
