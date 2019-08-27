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

import com.mongodb.Block;
import com.mongodb.async.SingleResultCallback;

import java.util.List;

class FlatteningSingleResultCallbackSubscription<TResult> extends AbstractSubscription<TResult> {

    private final Block<SingleResultCallback<List<TResult>>> block;

    /* protected by `this` */
    private boolean completed;
    /* protected by `this` */

    FlatteningSingleResultCallbackSubscription(final Block<SingleResultCallback<List<TResult>>> block,
                                               final Observer<? super TResult> observer) {
        super(observer);
        this.block = block;
        observer.onSubscribe(this);
    }

    @Override
    void requestInitialData() {
        block.apply(new SingleResultCallback<List<TResult>>() {
            @Override
            public void onResult(final List<TResult> result, final Throwable t) {
                if (t != null) {
                    onError(t);
                } else {
                    addToQueue(result);
                    synchronized (FlatteningSingleResultCallbackSubscription.this) {
                        completed = true;
                    }
                    tryProcessResultsQueue();
                }
            }
        });
    }

    @Override
    boolean checkCompleted() {
        return completed;
    }
}
