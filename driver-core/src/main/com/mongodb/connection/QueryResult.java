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

package com.mongodb.connection;

import com.mongodb.MongoNamespace;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;

import java.util.List;

/**
 * A batch of query results.
 *
 * @param <T> the type of document to decode query results to
 * @since 3.0
 */
public class QueryResult<T> {
    private final MongoNamespace namespace;
    private final List<T> results;
    private final long cursorId;
    private final ServerAddress serverAddress;

    /**
     * Construct an instance.
     *
     * @param namespace    the namespace
     * @param results       the query results
     * @param cursorId      the cursor id
     * @param serverAddress the server address
     */
    public QueryResult(final MongoNamespace namespace, final List<T> results, final long cursorId, final ServerAddress serverAddress) {
        this.namespace = namespace;
        this.results = results;
        this.cursorId = cursorId;
        this.serverAddress = serverAddress;
    }

    /**
     * Gets the namespace.
     *
     * @return the namespace
     */
    public MongoNamespace getNamespace() {
        return namespace;
    }

    /**
     * Gets the cursor.
     *
     * @return the cursor, which may be null if it's been exhausted
     */
    public ServerCursor getCursor() {
        return cursorId == 0 ? null : new ServerCursor(cursorId, serverAddress);
    }

    /**
     * Gets the results.
     *
     * @return the results
     */
    public List<T> getResults() {
        return results;
    }

    /**
     * Gets the server address.
     *
     * @return the server address
     */
    public ServerAddress getAddress() {
        return serverAddress;
    }
}
