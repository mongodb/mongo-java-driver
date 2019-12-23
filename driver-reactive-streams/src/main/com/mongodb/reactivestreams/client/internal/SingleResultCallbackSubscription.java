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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.Block;
import com.mongodb.internal.async.SingleResultCallback;
import org.reactivestreams.Subscriber;

class SingleResultCallbackSubscription<TResult> extends AbstractSubscription<TResult> {

    private final Block<SingleResultCallback<TResult>> block;

    /* protected by `this` */
    private boolean completed;
    /* protected by `this` */

    SingleResultCallbackSubscription(final Block<SingleResultCallback<TResult>> block, final Subscriber<? super TResult> subscriber) {
        super(subscriber);
        this.block = block;
        subscriber.onSubscribe(this);
    }

    @Override
    void requestInitialData() {
        block.apply((result, t) -> {
            if (t != null) {
                onError(t);
            } else {
                addToQueue(result);
                synchronized (SingleResultCallbackSubscription.this) {
                    completed = true;
                }
                tryProcessResultsQueue();
            }
        });
    }

    @Override
    boolean checkCompleted() {
        return completed;
    }

}
