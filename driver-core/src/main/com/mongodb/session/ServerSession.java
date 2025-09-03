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

package com.mongodb.session;

import org.bson.BsonDocument;

/**
 * A MongoDB server session.
 *
 * @mongodb.server.release 3.6
 * @since 3.6
 */
public interface ServerSession {

    /**
     * @return the server session identifier
     */
    BsonDocument getIdentifier();

    /**
     * Gets the current transaction number.
     *
     * @return the current transaction number
     * @since 3.8
     */
    long getTransactionNumber();

    /**
     * Return the next available transaction number.
     *
     * @return the next transaction number
     */
    long advanceTransactionNumber();

    /**
     * Whether the server session is closed.
     *
     * @return true if the session has been closed
     */
    boolean isClosed();

    /**
     * Mark the server session as dirty.
     * <p>
     * A server session is marked dirty when a command fails with a network
     * error. Dirty sessions are later discarded from the server session pool.
     * @since 3.12
     */
    void markDirty();

    /**
     * Whether the server session is marked dirty.
     *
     * @return true if the session has been marked dirty
     * @since 3.12
     */
    boolean isMarkedDirty();
}
