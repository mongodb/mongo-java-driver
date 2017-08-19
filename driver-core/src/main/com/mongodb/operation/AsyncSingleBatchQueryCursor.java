/*
 * Copyright 2016 MongoDB, Inc.
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

package com.mongodb.operation;

import com.mongodb.MongoException;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.connection.QueryResult;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;

class AsyncSingleBatchQueryCursor<T> implements AsyncBatchCursor<T> {
    private volatile QueryResult<T> firstBatch;
    private volatile boolean closed;

    AsyncSingleBatchQueryCursor(final QueryResult<T> firstBatch) {
        this.firstBatch = firstBatch;
        isTrue("Empty Cursor", firstBatch.getCursor() == null);
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public void next(final SingleResultCallback<List<T>> callback) {
        if (closed) {
            callback.onResult(null, new MongoException("next() called after the cursor was closed."));
        } else if (firstBatch != null && !firstBatch.getResults().isEmpty()) {
            List<T> results = firstBatch.getResults();
            firstBatch = null;
            callback.onResult(results, null);
        } else {
            closed = true;
            callback.onResult(null, null);
        }
    }

    @Override
    public void tryNext(final SingleResultCallback<List<T>> callback) {
        next(callback);
    }

    @Override
    public void setBatchSize(final int batchSize) {
        // Noop
    }

    @Override
    public int getBatchSize() {
        return 0;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

}
