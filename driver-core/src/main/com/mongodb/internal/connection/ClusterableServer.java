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

/**
 * A logical connection to a MongoDB server that supports clustering along with other servers.
 */
interface ClusterableServer extends Server {

    enum ConnectionState {
        BEFORE_HANDSHAKE,
        AFTER_HANDSHAKE
    }

    /**
     * Reset server description to connecting state
     */
    void resetToConnecting();

    /**
     * Invalidate the description of this server.  Implementation of this method should not block, but rather trigger an asynchronous
     * attempt to connect with the server in order to determine its current status.
     */
    void invalidate();

    /**
     * Invalidate the description of this server due to the passed in reason.
     * @param connectionState the connection state
     * @param reason the reason for invalidation.
     * @param connectionGeneration the connection pool's generation of the connection from which the error arose
     * @param maxWireVersion the maxWireVersion from the connection from which the error arose
     */
    void invalidate(ConnectionState connectionState, Throwable reason, int connectionGeneration, int maxWireVersion);

    /**
     * <p>Closes the server.  Instances that have been closed will no longer be available for use.</p>
     *
     * <p>Implementations should ensure that this method can be called multiple times with no ill effects. </p>
     */
    void close();

    /**
     * Returns true if the server is closed, false otherwise.
     *
     * @return whether the server is closed
     */
    boolean isClosed();

    /**
     * Attempt to connect to the server.
     */
    void connect();
}
