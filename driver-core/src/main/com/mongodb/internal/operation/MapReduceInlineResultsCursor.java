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

import java.util.List;

/**
 * Cursor representation of the results of an inline map-reduce operation.  This allows users to iterate over the results that were returned
 * from the operation, and also provides access to the statistics returned in the results.
 */
class MapReduceInlineResultsCursor<T> implements MapReduceBatchCursor<T> {
    private final BatchCursor<T> delegate;
    private final MapReduceStatistics statistics;

    MapReduceInlineResultsCursor(final BatchCursor<T> delegate, final MapReduceStatistics statistics) {
        this.delegate = delegate;
        this.statistics = statistics;
    }

    @Override
    public MapReduceStatistics getStatistics() {
        return statistics;
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public List<T> next() {
        return delegate.next();
    }

    @Override
    public int available() {
        return delegate.available();
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
    public List<T> tryNext() {
        return delegate.tryNext();
    }

    @Override
    public ServerCursor getServerCursor() {
        return delegate.getServerCursor();
    }

    @Override
    public ServerAddress getServerAddress() {
        return delegate.getServerAddress();
    }

    @Override
    public void close() {
        delegate.close();
    }
}
