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

import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.internal.async.AsyncAggregateResponseBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import java.util.List;

public class AsyncCommandBatchCursor<T> implements AsyncAggregateResponseBatchCursor<T> {

    private final TimeoutMode timeoutMode;
    private OperationContext operationContext;

    private AsyncCoreCursor<T> wrapped;

    AsyncCommandBatchCursor(
            final TimeoutMode timeoutMode,
            final long maxTimeMs,
            final OperationContext operationContext,
            final AsyncCoreCursor<T> wrapped) {
        this.operationContext = operationContext.withOverride(timeoutContext ->
                timeoutContext.withMaxTimeOverride(maxTimeMs));
        this.timeoutMode = timeoutMode;
        this.wrapped = wrapped;
    }

    @Override
    public void next(final SingleResultCallback<List<T>> callback) {
        resetTimeout();
        wrapped.next(operationContext, callback);
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
    public boolean isClosed() {
        return wrapped.isClosed();
    }

    @Override
    public void close() {
        wrapped.close(operationContext
                .withOverride(timeoutContext -> timeoutContext
                        .withNewlyStartedTimeout()
                        .withDefaultMaxTime()
                ));
    }

    @Nullable
    @Override
    public BsonDocument getPostBatchResumeToken() {
        return wrapped.getPostBatchResumeToken();
    }

    @Nullable
    @Override
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

    AsyncCoreCursor<T> getWrapped() {
        return wrapped;
    }
}

