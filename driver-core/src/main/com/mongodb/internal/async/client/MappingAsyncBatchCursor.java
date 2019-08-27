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

package com.mongodb.internal.async.client;

import com.mongodb.Function;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;

import java.util.ArrayList;
import java.util.List;

class MappingAsyncBatchCursor<T, U> implements AsyncBatchCursor<U> {

    private final AsyncBatchCursor<T> batchCursor;
    private final Function<T, U> mapper;

    MappingAsyncBatchCursor(final AsyncBatchCursor<T> batchCursor, final Function<T, U> mapper) {
        this.batchCursor = batchCursor;
        this.mapper = mapper;
    }

    @Override
    public void next(final SingleResultCallback<List<U>> callback) {
        batchCursor.next(getMappingCallback(callback));
    }

    @Override
    public void tryNext(final SingleResultCallback<List<U>> callback) {
        batchCursor.tryNext(getMappingCallback(callback));
    }

    @Override
    public void setBatchSize(final int batchSize) {
        batchCursor.setBatchSize(batchSize);
    }

    @Override
    public int getBatchSize() {
        return batchCursor.getBatchSize();
    }

    @Override
    public boolean isClosed() {
        return batchCursor.isClosed();
    }

    @Override
    public void close() {
        batchCursor.close();
    }

    private SingleResultCallback<List<T>> getMappingCallback(final SingleResultCallback<List<U>> callback) {
        return new SingleResultCallback<List<T>>() {
            @Override
            public void onResult(final List<T> results, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else if (results != null) {
                    try {
                        List<U> mappedResults = new ArrayList<U>();
                        for (T result : results) {
                            mappedResults.add(mapper.apply(result));
                        }
                        callback.onResult(mappedResults, null);
                    } catch (Throwable t1) {
                        callback.onResult(null, t1);
                    }
                } else {
                    callback.onResult(null, null);
                }
            }
        };
    }
}
