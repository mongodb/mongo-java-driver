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

import com.mongodb.internal.async.SingleResultCallback;

import java.util.List;

/**
 * Cursor representation of the results of an inline map-reduce operation.  This allows users to iterate over the results that were returned
 * from the operation, and also provides access to the statistics returned in the results.
 */
class MapReduceInlineResultsAsyncCursor<T> implements MapReduceAsyncBatchCursor<T> {

    private final AsyncSingleBatchCursor<T> delegate;
    private final MapReduceStatistics statistics;

    MapReduceInlineResultsAsyncCursor(final AsyncSingleBatchCursor<T> delegate, final MapReduceStatistics statistics) {
        this.delegate = delegate;
        this.statistics = statistics;
    }

    @Override
    public MapReduceStatistics getStatistics() {
        return statistics;
    }

    @Override
    public void next(final SingleResultCallback<List<T>> callback) {
        delegate.next(callback);
    }

    @Override
    public void setBatchSize(final int batchSize) {
        delegate.setBatchSize(batchSize);
    }

    @Override
    public int getBatchSize() {
        return delegate.getBatchSize();
    }

    @Override
    public boolean isClosed() {
        return delegate.isClosed();
    }

    @Override
    public void close() {
        delegate.close();
    }
}
