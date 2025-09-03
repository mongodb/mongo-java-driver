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

package com.mongodb.client;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.lang.Nullable;

import java.io.Closeable;
import java.util.Iterator;
import java.util.function.Consumer;

/**
 * The Mongo Cursor interface implementing the iterator protocol.
 * <p>
 * An application should ensure that a cursor is closed in all circumstances, e.g. using a try-with-resources statement:
 *
 * <pre>{@code
 * try (MongoCursor<Document> cursor = collection.find().cursor()) {
 *     while (cursor.hasNext()) {
 *         System.out.println(cursor.next());
 *     }
 * }
 * }</pre>
 *
 * @since 3.0
 * @param <TResult> The type of documents the cursor contains
 */
@NotThreadSafe
public interface MongoCursor<TResult> extends Iterator<TResult>, Closeable {
    /**
     * Despite this interface being {@linkplain NotThreadSafe non-thread-safe},
     * {@link #close()} is allowed to be called concurrently with any method of the cursor, including itself.
     * This is useful to cancel blocked {@link #hasNext()}, {@link #next()}.
     * This method is idempotent.
     */
    @Override
    void close();

    @Override
    boolean hasNext();

    @Override
    TResult next();

    /**
     * Gets the number of results available locally without blocking, which may be 0.
     *
     * <p>
     * If the cursor is known to be exhausted, returns 0.  If the cursor is closed before it's been exhausted, it may return a non-zero
     * value.
     * </p>
     *
     * @return the number of results available locally without blocking
     * @since 4.4
     */
    int available();

    /**
     * A special {@code next()} case that returns the next element in the iteration if available or null.
     *
     * <p>Tailable cursors are an example where this is useful. A call to {@code tryNext()} may return null, but in the future calling
     * {@code tryNext()} would return a new element if a document had been added to the capped collection.</p>
     *
     * @return the next element in the iteration if available or null.
     * @mongodb.driver.manual reference/glossary/#term-tailable-cursor Tailable Cursor
     */
    @Nullable
    TResult tryNext();

    /**
     * Returns the server cursor, which can be null if the no cursor was created or if the cursor has been exhausted or killed.
     *
     * @return the ServerCursor, which can be null.
     */
    @Nullable
    ServerCursor getServerCursor();

    /**
     * Returns the server address
     *
     * @return ServerAddress
     */
    ServerAddress getServerAddress();

    @Override
    default void forEachRemaining(final Consumer<? super TResult> action) {
        try {
            Iterator.super.forEachRemaining(action);
        } finally {
            close();
        }
    }
}
