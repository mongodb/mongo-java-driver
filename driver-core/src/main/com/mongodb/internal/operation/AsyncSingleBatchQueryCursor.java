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
import com.mongodb.internal.ClientSideOperationTimeout;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.connection.QueryResult;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;

class AsyncSingleBatchQueryCursor<T> implements AsyncBatchCursor<T> {
    private final ClientSideOperationTimeout clientSideOperationTimeout;
    private volatile QueryResult<T> firstBatch;
    private volatile boolean closed;

    AsyncSingleBatchQueryCursor(final ClientSideOperationTimeout clientSideOperationTimeout, final QueryResult<T> firstBatch) {
        isTrue("Empty Cursor", firstBatch.getCursor() == null);
        this.clientSideOperationTimeout = clientSideOperationTimeout;
        this.firstBatch = firstBatch;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public ClientSideOperationTimeout getClientSideOperationTimeout() {
        return clientSideOperationTimeout;
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
