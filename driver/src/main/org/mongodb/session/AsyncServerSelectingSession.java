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

import org.mongodb.AsyncServerSelectingOperation;
import org.mongodb.MongoFuture;

/**
 * A provider of asynchronous connections to servers based on a server selector.
 *
 * @since 3.0
 */
public interface AsyncServerSelectingSession {

    /**
     * Executes the given operation.
     *
     * @param operation the operation to execute
     * @param <T> the result type of the operation
     * @return a future representing the result of executing the operation
     */
    <T> MongoFuture<T> execute(AsyncServerSelectingOperation<T> operation);

    /**
     * Gets a new session that is bound to a single server or a single connection, based on the given binding type.  In the former case,
     * the new session will cache the server instance it's bound to and reuse it for subsequent requests for a connection.  In the latter
     * case, the new session will cache the connection it's bound to and reuse it for subsequent requests for a connection.
     * <p>
     * Care should be taken to not close this session until after any bound sessions that came from it are also closed.
     * </p>
     *
     * @param operation     the operation whose server selector is used to bind the session
     * @param sessionBindingType the binding type
     * @return a future for the bound session
     */
    <T> MongoFuture<AsyncSession> getBoundSession(AsyncServerSelectingOperation<T> operation, SessionBindingType sessionBindingType);

    void close();

    boolean isClosed();
}
