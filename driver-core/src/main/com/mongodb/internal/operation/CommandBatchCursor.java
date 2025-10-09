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
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import java.util.List;

class CommandBatchCursor<T> implements AggregateResponseBatchCursor<T> {

    private final TimeoutMode timeoutMode;
    private OperationContext operationContext;
    private Cursor<T> wrapped;

    CommandBatchCursor(
            final TimeoutMode timeoutMode,
            final long maxTimeMs,
            final OperationContext operationContext,
            final Cursor<T> wrapped) {
        this.operationContext = operationContext.withOverride(timeoutContext ->
                timeoutContext.withMaxTimeOverride(maxTimeMs));
        this.timeoutMode = timeoutMode;
        this.wrapped = wrapped;
    }

    @Override
    public boolean hasNext() {
        resetTimeout();
        return wrapped.hasNext(operationContext);
    }

    @Override
    public List<T> next() {
        resetTimeout();
        return wrapped.next(operationContext);
    }

    @Override
    public int available() {
        return wrapped.available();
    }

    @Override
    public void setBatchSize(final int batchSize) {
        wrapped.setBatchSize(batchSize);
    }

    @Override
    public int getBatchSize() {
        return wrapped.getBatchSize();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void close() {
        operationContext = operationContext.withOverride(timeoutContext -> timeoutContext
                .withNewlyStartedTimeout()
                .withDefaultMaxTime());
        wrapped.close(operationContext);
    }

    @Nullable
    @Override
    public List<T> tryNext() {
        resetTimeout();
        return wrapped.tryNext(operationContext);
    }

    @Override
    @Nullable
    public ServerCursor getServerCursor() {
        return wrapped.getServerCursor();
    }

    @Override
    public ServerAddress getServerAddress() {
        return wrapped.getServerAddress();
    }

    @Override
    @Nullable
    public BsonDocument getPostBatchResumeToken() {
        return wrapped.getPostBatchResumeToken();
    }

    @Override
    @Nullable
    public BsonTimestamp getOperationTime() {
        return wrapped.getOperationTime();
    }

    @Override
    public boolean isFirstBatchEmpty() {
        return wrapped.isFirstBatchEmpty();
    }

    @Override
    public int getMaxWireVersion() {
        return wrapped.getMaxWireVersion();
    }

    private void resetTimeout() {
        if (timeoutMode == TimeoutMode.ITERATION) {
            operationContext = operationContext.withNewlyStartedTimeout();
        }
    }

    Cursor<T> getWrapped() {
        return wrapped;
    }
}

