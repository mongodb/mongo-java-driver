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

package com.mongodb.internal.async;

import com.mongodb.internal.operation.BatchCursor;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.internal.async.AsyncRunnable.beginAsync;

/**
 * MongoDB returns query results as batches, and this interface provides an asynchronous iterator over those batches.  The first call to
 * the {@code next} method will return the first batch, and subsequent calls will trigger an asynchronous request to get the next batch
 * of results.  Clients can control the batch size by setting the {@code batchSize} property between calls to {@code next}.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public interface AsyncBatchCursor<T> extends Closeable {
    /**
     * Returns the next batch of results. A tailable cursor will block until another batch exists.
     * Unlike the {@link BatchCursor} this method will automatically mark the cursor as closed when there are no more expected results.
     * Care should be taken to check {@link #isClosed()} between calls.
     *
     * @param callback callback to receive the next batch of results
     * @throws java.util.NoSuchElementException if no next batch exists
     */
    void next(SingleResultCallback<List<T>> callback);

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
     * Implementations of {@link AsyncBatchCursor} are allowed to close themselves, see {@link #close()} for more details.
     *
     * @return {@code true} if {@code this} has been closed or has closed itself.
     */
    boolean isClosed();

    /**
     * Implementations of {@link AsyncBatchCursor} are allowed to close themselves synchronously via methods
     * {@link #next(SingleResultCallback)}.
     * Self-closing behavior is discouraged because it introduces an additional burden on code that uses {@link AsyncBatchCursor}.
     * To help making such code simpler, this method is required to be idempotent.
     * <p>
     * Another quirk is that this method is allowed to release resources "eventually",
     * i.e., not before (in the happens-before order) returning.
     * Nevertheless, {@link #isClosed()} called after (in the happens-before order) {@link #close()} must return {@code true}.
     */
    @Override
    void close();

    default void exhaust(final SingleResultCallback<List<List<T>>> finalCallback) {
        List<List<T>> results = new ArrayList<>();

        beginAsync().thenRunDoWhileLoop(iterationCallback -> {
            beginAsync().<List<T>>thenSupply(c -> {
                next(c);
            }).thenConsume((batch, c) -> {
                if (!batch.isEmpty()) {
                    results.add(batch);
                }
                c.complete(c);
            }).finish(iterationCallback);
        }, () -> !this.isClosed()
        ).<List<List<T>>>thenSupply(c -> {
            c.complete(results);
        }).finish(finalCallback);
     }
}
