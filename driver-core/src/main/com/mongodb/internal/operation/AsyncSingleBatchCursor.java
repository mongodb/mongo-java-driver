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

import com.mongodb.MongoException;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;

import java.util.List;

import static java.util.Collections.emptyList;

class AsyncSingleBatchCursor<T> implements AsyncBatchCursor<T> {

    static <R> AsyncSingleBatchCursor<R> createEmptyBatchCursor(final int batchSize) {
        return new AsyncSingleBatchCursor<>(emptyList(), batchSize);
    }

    private final List<T> batch;
    private final int batchSize;

    private volatile boolean hasNext = true;
    private volatile boolean closed = false;

    AsyncSingleBatchCursor(final List<T> batch, final int batchSize) {
        this.batch = batch;
        this.batchSize = batchSize;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public void next(final SingleResultCallback<List<T>> callback) {
        if (closed) {
            callback.onResult(null, new MongoException("next() called after the cursor was closed."));
        } else if (hasNext && !batch.isEmpty()) {
            hasNext = false;
            callback.onResult(batch, null);
        } else {
            closed = true;
            callback.onResult(emptyList(), null);
        }
    }

    @Override
    public void setBatchSize(final int batchSize) {
        // Noop
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

}
