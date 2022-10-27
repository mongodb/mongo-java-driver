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

package com.mongodb.internal.operation;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.lang.Nullable;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;

/**
 * MongoDB returns query results as batches, and this interface provideds an iterator over those batches.  The first call to
 * the {@code next} method will return the first batch, and subsequent calls will trigger a  request to get the next batch
 * of results.  Clients can control the batch size by setting the {@code batchSize} property between calls to {@code next}.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
@NotThreadSafe
public interface BatchCursor<T> extends Iterator<List<T>>, Closeable {
    /**
     * Despite this interface being {@linkplain NotThreadSafe non-thread-safe},
     * {@link #close()} is allowed to be called concurrently with any method of the cursor, including itself.
     * This is useful to cancel blocked {@link #hasNext()}, {@link #next()}.
     * This method is idempotent.
     * <p>
     * Another quirk is that this method is allowed to release resources "eventually",
     * i.e., not before (in the happens-before order) returning.
     */
    @Override
    void close();

    /**
     * Returns true if another batch of results exists.  A tailable cursor will block until another batch exists.
     *
     * @return true if another batch exists
     */
    boolean hasNext();

    /**
     * Returns the next batch of results.  A tailable cursor will block until another batch exists.
     *
     * @return the next batch of results
     * @throws java.util.NoSuchElementException if no next batch exists
     */
    List<T> next();

    /**
     * Gets the number of results available locally without blocking,which may be 0, or 0 when the cursor is exhausted or closed.
     *
     * @return the number of results available locally without blocking
     */
    int available();

    /**
     * Sets the batch size to use when requesting the next batch.  This is the number of documents to request in the next batch.
     *
     * @param batchSize the non-negative batch size.  0 means to use the server default.
     */
    void setBatchSize(int batchSize);

    /**
     * Gets the batch size to use when requesting the next batch.  This is the number of documents to request in the next batch.
     *
     * @return the non-negative batch size.  0 means to use the server default.
     */
    int getBatchSize();

    /**
     * A special {@code next()} case that returns the next batch if available or null.
     *
     * <p>Tailable cursors are an example where this is useful. A call to {@code tryNext()} may return null, but in the future calling
     * {@code tryNext()} would return a new batch if a document had been added to the capped collection.</p>
     *
     * @return the next batch if available or null.
     * @mongodb.driver.manual reference/glossary/#term-tailable-cursor Tailable Cursor
     */
    List<T> tryNext();

    @Nullable
    ServerCursor getServerCursor();

    ServerAddress getServerAddress();
}
