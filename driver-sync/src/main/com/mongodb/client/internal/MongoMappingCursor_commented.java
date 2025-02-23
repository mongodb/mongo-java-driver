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

package com.mongodb.client.internal;

import com.mongodb.Function;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoCursor;
import com.mongodb.lang.Nullable;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A cursor implementation that transforms documents from one type to another using a mapping function.
 * This class wraps an existing cursor and applies a transformation to each document as it is retrieved,
 * allowing for type conversion without loading the entire result set into memory.
 *
 * <p>Key features:</p>
 * <ul>
 *     <li>Lazy transformation of documents using provided mapping function</li>
 *     <li>Preserves cursor characteristics (server address, cursor ID) of wrapped cursor</li>
 *     <li>Proper null handling in tryNext() operations</li>
 *     <li>Delegates resource management to wrapped cursor</li>
 * </ul>
 *
 * <p>Error handling:</p>
 * <ul>
 *     <li>Mapper exceptions are propagated to the caller</li>
 *     <li>Null checks are performed on constructor arguments</li>
 *     <li>Resource cleanup is guaranteed through close() delegation</li>
 * </ul>
 *
 * <p>Performance considerations:</p>
 * <ul>
 *     <li>Transformation occurs on-demand as documents are accessed</li>
 *     <li>No additional memory overhead beyond the current document</li>
 *     <li>Mapping function should be efficient as it's called for each document</li>
 * </ul>
 *
 * <p>Thread safety: This class is as thread-safe as the underlying cursor and mapping function.</p>
 */
class MongoMappingCursor<T, U> implements MongoCursor<U> {
    private final MongoCursor<T> proxied;
    private final Function<T, U> mapper;

    /**
     * Creates a new mapping cursor that transforms documents from type T to type U.
     *
     * @param proxied the underlying cursor to wrap
     * @param mapper the function to transform documents from T to U
     * @throws IllegalArgumentException if either argument is null
     */
    MongoMappingCursor(final MongoCursor<T> proxied, final Function<T, U> mapper) {
        this.proxied = notNull("proxied", proxied);
        this.mapper = notNull("mapper", mapper);
    }

    /**
     * Closes the underlying cursor and releases its resources.
     */
    @Override
    public void close() {
        proxied.close();
    }

    /**
     * Checks if there are more documents available.
     *
     * @return true if there are more documents available
     */
    @Override
    public boolean hasNext() {
        return proxied.hasNext();
    }

    /**
     * Gets the next document and applies the mapping function.
     * Any exceptions from the mapping function are propagated to the caller.
     *
     * @return the mapped document
     * @throws com.mongodb.MongoException if the mapping function throws an exception
     */
    @Override
    public U next() {
        return mapper.apply(proxied.next());
    }

    /**
     * Gets the number of documents available without making a request to the server.
     *
     * @return the number of documents available locally
     */
    @Override
    public int available() {
        return proxied.available();
    }

    /**
     * Attempts to get and map the next document without throwing an exception if none is available.
     * Handles null values from the underlying cursor appropriately.
     *
     * @return the mapped document or null if no document is available
     * @throws com.mongodb.MongoException if the mapping function throws an exception
     */
    @Nullable
    @Override
    public U tryNext() {
        T proxiedNext = proxied.tryNext();
        if (proxiedNext == null) {
            return null;
        } else {
            return mapper.apply(proxiedNext);
        }
    }

    /**
     * Not supported. Delegates to the underlying cursor.
     *
     * @throws UnsupportedOperationException if the underlying cursor doesn't support removal
     */
    @Override
    public void remove() {
        proxied.remove();
    }

    /**
     * Gets the server cursor information from the underlying cursor.
     *
     * @return the server cursor or null if the cursor is exhausted or was never established
     */
    @Nullable
    @Override
    public ServerCursor getServerCursor() {
        return proxied.getServerCursor();
    }

    /**
     * Gets the address of the server that this cursor is fetching results from.
     *
     * @return the server address
     */
    @Override
    public ServerAddress getServerAddress() {
        return proxied.getServerAddress();
    }
}
