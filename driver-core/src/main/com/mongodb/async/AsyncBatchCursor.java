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

package com.mongodb.async;

import java.io.Closeable;
import java.util.List;

/**
 * MongoDB returns query results as batches, and this interface provides an asynchronous iterator over those batches.  The first call to
 * the {@code next} method will return the first batch, and subsequent calls will trigger an asynchronous request to get the next batch
 * of results.  Clients can control the batch size by setting the {@code batchSize} property between calls to {@code next}.
 *
 * @since 3.0
 *
 * @param <T> The type of documents the cursor contains
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#wire-op-get-more OP_GET_MORE
 * @deprecated Prefer the Reactive Streams-based asynchronous driver (mongodb-driver-reactivestreams artifactId)
 */
@Deprecated
public interface AsyncBatchCursor<T> extends Closeable {
    /**
     * Returns the next batch of results.  A tailable cursor will block until another batch exists.  After the last batch, the next call
     * to this method will execute the callback with a null result to indicate that there are no more batches available and the cursor
     * has been closed.
     *
     * @param callback callback to receive the next batch of results
     * @throws java.util.NoSuchElementException if no next batch exists
     */
    void next(SingleResultCallback<List<T>> callback);

    /**
     * A special {@code next()} case that returns the next batch if available or null.
     *
     * <p>Tailable cursors are an example where this is useful. A call to {@code tryNext()} may return null, but in the future calling
     * {@code tryNext()} would return a new batch if a document had been added to the capped collection.</p>
     *
     * @param callback callback to receive the next batch of results
     * @since 3.6
     * @mongodb.driver.manual reference/glossary/#term-tailable-cursor Tailable Cursor
     */
    void tryNext(SingleResultCallback<List<T>> callback);

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
     * Return true if the AsyncBatchCursor has been closed
     *
     * @return true if the AsyncBatchCursor has been closed
     */
    boolean isClosed();

    @Override
    void close();
}
