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

package org.mongodb.session;

import org.mongodb.connection.AsyncServerConnection;
import org.mongodb.connection.ServerSelector;
import org.mongodb.operation.MongoFuture;

/**
 * A provider of asynchronous connections to servers based on a server selector.
 *
 * @since 3.0
 */
public interface AsyncServerSelectingSession extends AsyncSession {

    /**
     * Gets a connection from a server that satisfies the given server selector.
     *
     *
     * @param serverSelector the server selector to use when choosing a server to get a connection from.
     * @return a future for a connection
     */
    MongoFuture<AsyncServerConnection> getConnection(ServerSelector serverSelector);

    /**
     * Gets a new session that is bound to a single server or a single connection, based on the given binding type.  In the former case,
     * the new session will cache the server instance it's bound to and reuse it for subsequent requests for a connection.  In the latter
     * case, the new session will cache the connection it's bound to and reuse it for subsequent requests for a connection.
     * <p>
     * Care should be taken to not close this session until after any bound sessions that came from it are also closed.
     * </p>
     *
     * @param serverSelector the server selector to use when binding the session
     * @param sessionBindingType the binding type
     * @return the bound session
     */
    AsyncSession getBoundSession(ServerSelector serverSelector, SessionBindingType sessionBindingType);
}
