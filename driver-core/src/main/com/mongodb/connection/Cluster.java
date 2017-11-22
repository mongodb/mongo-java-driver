/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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


import com.mongodb.async.SingleResultCallback;
import com.mongodb.selector.ServerSelector;

import java.io.Closeable;

/**
 * Represents a cluster of MongoDB servers.  Implementations can define the behaviour depending upon the type of cluster.
 *
 * @since 3.0
 */
public interface Cluster extends Closeable {

    /**
     * Gets the cluster settings with which this cluster was created.
     *
     * @return the cluster settings
     * @since 3.4
     */
    ClusterSettings getSettings();

    /**
     * Get the description of this cluster.  This method will not return normally until the cluster type is known.
     *
     * @return a ClusterDescription representing the current state of the cluster
     * @throws com.mongodb.MongoTimeoutException if the timeout has been reached before the cluster type is known
     */
    ClusterDescription getDescription();

    /**
     * Get the current description of this cluster.
     *
     * @return the current ClusterDescription representing the current state of the cluster.
     */
    ClusterDescription getCurrentDescription();

    /**
     * Get a MongoDB server that matches the criteria defined by the serverSelector
     *
     * @param serverSelector a ServerSelector that defines how to select the required Server
     * @return a Server that meets the requirements
     * @throws com.mongodb.MongoTimeoutException if the timeout has been reached before a server matching the selector is available
     */
    Server selectServer(ServerSelector serverSelector);

    /**
     * Asynchronously gets a MongoDB server that matches the criteria defined by the serverSelector.
     *
     * @param serverSelector a ServerSelector that defines how to select the required Server
     * @param callback the callback to invoke when the server is found or an error occurs
     */
    void selectServerAsync(ServerSelector serverSelector, SingleResultCallback<Server> callback);

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
}
