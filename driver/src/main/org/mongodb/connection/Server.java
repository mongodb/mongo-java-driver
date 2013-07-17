/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.connection;

import org.mongodb.annotations.ThreadSafe;

/**
 * A logical connection to a MongoDB server.
 *
 * @since 3.0
 */
@ThreadSafe
public interface Server {
    /**
     * Gets the description of this server.  Implementations of this method should not block if the server has not yet been successfully
     * contacted, but rather return immediately a @code{ServerDescription} in a @code{ServerDescription.Status.Connecting} state.
     *
     * @return the description of this server
     */
    ServerDescription getDescription();

    /**
     * Gets a connection to this server.  The connection should be closed after the caller is done with it.
     * <p>
     * Implementations of this method are allowed to block while waiting for a free connection from a pool of available connections.
     * </p>
     * <p>
     * Implementations of this method will likely pool the underlying connections, so the effect of closing the returned connection will
     * be to return the connection to the pool.
     * </p>
     *
     * @return a connection this server
     */
    Connection getConnection();

    /**
     * Gets an asynchronous connection to this server.  The connection should be closed after the caller is done with it.
     * <p>
     * Implementations of this method are allowed to block while waiting for a free connection from a pool of available connections.
     * </p>
     * <p>
     * Implementations of this method will likely pool the underlying connections, so the effect of closing the returned connection will
     * be to return the connection to the pool.
     * </p>
     *
     * @return an asynchronous connection to this server
     */
    AsyncServerConnection getAsyncConnection();
}
