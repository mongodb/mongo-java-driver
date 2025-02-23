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

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoCursor;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.lang.Nullable;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * An adapter that converts a {@link BatchCursor} into a {@link MongoCursor} by managing the state of batches
 * and cursor positions. This implementation improves performance by fetching documents in batches from the server
 * and maintaining a client-side buffer of the current batch.
 *
 * <p>The adapter maintains three pieces of state:</p>
 * <ul>
 *     <li>batchCursor: The underlying BatchCursor that fetches batches of documents from MongoDB</li>
 *     <li>curBatch: The current batch of documents being processed (null if no current batch)</li>
 *     <li>curPos: The position within the current batch (reset to 0 when moving to a new batch)</li>
 * </ul>
 *
 * <p>Performance considerations:</p>
 * <ul>
 *     <li>Batching reduces network round trips by fetching multiple documents at once</li>
 *     <li>Client-side buffering allows for efficient iteration without server requests per document</li>
 *     <li>Memory usage scales with batch size as batches are held in memory</li>
 * </ul>
 *
 * <p>Thread safety: This class is not thread-safe and should not be used concurrently from multiple threads.</p>
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class MongoBatchCursorAdapter<T> implements MongoCursor<T> {
    private final BatchCursor<T> batchCursor;
    private List<T> curBatch;
    private int curPos;

    /**
     * Creates a new MongoCursor adapter around a BatchCursor.
     *
     * @param batchCursor the underlying batch cursor to adapt
     */
    public MongoBatchCursorAdapter(final BatchCursor<T> batchCursor) {
        this.batchCursor = batchCursor;
    }

    /**
     * Not supported by MongoDB cursors.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException("Cursors do not support removal");
    }

    /**
     * Closes the underlying batch cursor and releases server resources.
     */
    @Override
    public void close() {
        batchCursor.close();
    }

    /**
     * Checks if there are more documents available in either the current batch
     * or from the server through the batch cursor.
     *
     * @return true if there are more documents available
     */
    @Override
    public boolean hasNext() {
        return curBatch != null || batchCursor.hasNext();
    }

    /**
     * Gets the next document from either the current batch or by fetching a new batch
     * if necessary.
     *
     * @return the next document
     * @throws NoSuchElementException if there are no more documents available
     */
    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        if (curBatch == null) {
            curBatch = batchCursor.next();
        }

        return getNextInBatch();
    }

    /**
     * Returns the number of documents available without making a request to the server.
     * This includes documents in the current batch and any buffered in the batch cursor.
     *
     * @return the number of documents available without a server request
     */
    @Override
    public int available() {
        int available = batchCursor.available();
        if (curBatch != null) {
            available += (curBatch.size() - curPos);
        }
        return available;
    }

    /**
     * Attempts to get the next document without throwing an exception if none is available.
     * May fetch a new batch from the server if the current batch is exhausted.
     *
     * @return the next document or null if none is available
     */
    @Nullable
    @Override
    public T tryNext() {
        if (curBatch == null) {
            curBatch = batchCursor.tryNext();
        }

        return curBatch == null ? null : getNextInBatch();
    }

    /**
     * Gets the server cursor information if one exists.
     *
     * @return the server cursor or null if the cursor is exhausted or was never established
     */
    @Nullable
    @Override
    public ServerCursor getServerCursor() {
        return batchCursor.getServerCursor();
    }

    /**
     * Gets the address of the server that this cursor is fetching results from.
     *
     * @return the server address
     */
    @Override
    public ServerAddress getServerAddress() {
        return batchCursor.getServerAddress();
    }

    /**
     * Gets the next document from the current batch and updates the cursor position.
     * When the current batch is exhausted, the batch is cleared and position reset.
     *
     * <p>This method assumes that curBatch is not null and contains at least one document
     * at the current position.</p>
     *
     * @return the next document from the current batch
     */
    private T getNextInBatch() {
        T nextInBatch = curBatch.get(curPos);
        if (curPos < curBatch.size() - 1) {
            curPos++;
        } else {
            curBatch = null;
            curPos = 0;
        }
        return nextInBatch;
    }
}
