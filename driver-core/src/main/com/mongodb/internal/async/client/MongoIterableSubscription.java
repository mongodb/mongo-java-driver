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

import com.mongodb.MongoException;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;

import java.util.List;


final class MongoIterableSubscription<TResult> extends AbstractSubscription<TResult> {

    private final AsyncMongoIterable<TResult> mongoIterable;

    /* protected by `this` */
    private boolean isReading;
    private boolean completed;
    /* protected by `this` */

    private volatile AsyncBatchCursor<TResult> batchCursor;

    MongoIterableSubscription(final AsyncMongoIterable<TResult> mongoIterable, final Observer<? super TResult> observer) {
        super(observer);
        this.mongoIterable = mongoIterable;
        observer.onSubscribe(this);
    }

    @Override
    void requestInitialData() {
        mongoIterable.batchSize(calculateBatchSize());
        mongoIterable.batchCursor(new SingleResultCallback<AsyncBatchCursor<TResult>>() {
            @Override
            public void onResult(final AsyncBatchCursor<TResult> result, final Throwable t) {
                if (t != null) {
                    onError(t);
                } else if (result != null) {
                    batchCursor = result;
                    requestMoreData();
                } else {
                    onError(new MongoException("Unexpected error, no AsyncBatchCursor returned from the MongoIterable."));
                }
            }
        });
    }

    @Override
    boolean checkCompleted() {
        return completed;
    }

    @Override
    void postTerminate() {
        try {
            if (batchCursor != null) {
                batchCursor.close();
            }
        } catch (Exception e) {
            // do nothing
        }
    }

    @Override
    void requestMoreData() {
        boolean mustRead = false;
        synchronized (this) {
            if (!isReading && !isTerminated() && batchCursor != null) {
                isReading = true;
                mustRead = true;
            }
        }

        if (mustRead) {
            batchCursor.setBatchSize(calculateBatchSize());
            batchCursor.next(new SingleResultCallback<List<TResult>>() {
                @Override
                public void onResult(final List<TResult> result, final Throwable t) {
                    synchronized (MongoIterableSubscription.this) {
                        isReading = false;
                    }

                    if (t != null) {
                        onError(t);
                    } else {
                        addToQueue(result);
                        synchronized (MongoIterableSubscription.this) {
                            if (result == null) {
                                completed = true;
                            }
                        }
                        tryProcessResultsQueue();
                    }
                }
            });
        }
    }

    /**
     * Returns the batchSize to be used with the cursor.
     *
     * <p>If the batch size has been set on the MongoIterable that is used, otherwise the requested demand is used. When using requested
     * demand, values less than 2 would close the cursor so that is the minimum batchSize and `Integer.MAX_VALUE` is the maximum.</p>
     *
     * @return the batchSize to use
     */
    private int calculateBatchSize() {
        Integer batchSize = mongoIterable.getBatchSize();
        if (batchSize != null) {
            return batchSize;
        }
        long requested = getRequested();
        if (requested <= 1) {
            return 2;
        } else if (requested < Integer.MAX_VALUE) {
            return (int) requested;
        } else {
            return Integer.MAX_VALUE;
        }
    }
}
