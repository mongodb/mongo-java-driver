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
import java.util.NoSuchElementException;

import static java.util.Collections.emptyList;

class SingleBatchCursor<T> implements BatchCursor<T> {

    static <R> SingleBatchCursor<R> createEmptyBatchCursor(final ServerAddress serverAddress, final int batchSize) {
        return new SingleBatchCursor<>(emptyList(), batchSize, serverAddress);
    }

    private final List<T> batch;
    private final ServerAddress serverAddress;
    private final int batchSize;
    private boolean hasNext;

    SingleBatchCursor(final List<T> batch, final int batchSize, final ServerAddress serverAddress) {
        this.batch = batch;
        this.serverAddress = serverAddress;
        this.batchSize = batchSize;
        this.hasNext = !batch.isEmpty();
    }

    public List<T> getBatch() {
        return batch;
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public List<T> next() {
        if (hasNext) {
            hasNext = false;
            return batch;
        }
        throw new NoSuchElementException();
    }

    @Override
    public int available() {
        return hasNext ? 1 : 0;
    }

    @Override
    public void setBatchSize(final int batchSize) {
        // NOOP
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public List<T> tryNext() {
        return hasNext ? next() : null;
    }

    @Override
    public ServerCursor getServerCursor() {
        return null;
    }

    @Override
    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    @Override
    public void close() {
    }
}
