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

import com.mongodb.annotations.ThreadSafe;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.connection.ServerDescription;

/**
 * A logical connection to a MongoDB server.
 *
 * @since 3.0
 */
@ThreadSafe
public interface Server {
    /**
     * Gets the description of this server.  Implementations of this method should not block if the server has not yet been successfully
     * contacted, but rather return immediately a @code{ServerDescription} in a @code{ServerConnectionState.CONNECTING} state.
     *
     * @return the description of this server
     */
    ServerDescription getDescription();

    /**
     * <p>Gets a connection to this server.  The connection should be released after the caller is done with it.</p>
     *
     * <p> Implementations of this method are allowed to block while waiting for a free connection from a pool of available connection.</p>
     *
     * <p> Implementations of this method will likely pool the underlying connection, so the effect of closing the returned connection will
     * be to return the connection to the pool. </p>
     *
     * @return a connection this server
     */
    Connection getConnection();

    /**
     * <p>Gets a connection to this server asynchronously.  The connection should be released after the caller is done with it.</p>
     *
     * <p> Implementations of this method will likely pool the underlying connection, so the effect of closing the returned connection will
     * be to return the connection to the pool. </p>
     *
     * @param callback the callback to execute when the connection is available or an error occurs
     */
    void getConnectionAsync(SingleResultCallback<AsyncConnection> callback);
}
