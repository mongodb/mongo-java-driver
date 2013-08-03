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

import org.mongodb.AsyncOperation;
import org.mongodb.MongoFuture;

/**
 * A provider of asynchronous connections.
 *
 * @since 3.0
 */
public interface AsyncSession {

    /**
     * Executes the given operation.
     *
     * @param operation the operation to execute
     * @param <T> the return type of the operation
     * @return a future for the result of the operation
     */
    <T> MongoFuture<T> execute(AsyncOperation<T> operation);

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
