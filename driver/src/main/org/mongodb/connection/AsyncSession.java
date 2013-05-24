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

import org.mongodb.operation.MongoFuture;

/**
 * A provider of asynchronous connections.
 *
 * @since 3.0
 */
public interface AsyncSession {

    /**
     * Gets a connection, which should be closed after use.
     *
     * @return a future for an asynchronous connection
     */
    MongoFuture<AsyncConnection> getConnection();

    /**
     * Close the session.  Care should be taken to close any connections that were provided by this session before the session itself is
     * closed.
     */
    void close();

    /**
     * Returns true if this session has been closed
     *
     * @return true if the session has been closed;
     */
    boolean isClosed();
}
