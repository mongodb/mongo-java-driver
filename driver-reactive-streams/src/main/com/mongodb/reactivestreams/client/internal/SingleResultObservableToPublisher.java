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
import com.mongodb.internal.async.client.Observables;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

@SuppressWarnings("deprecation")
public class SingleResultObservableToPublisher<TResult> implements org.reactivestreams.Publisher<TResult> {

    private final ObservableToPublisher<TResult> observable;

    public SingleResultObservableToPublisher(final Block<com.mongodb.async.SingleResultCallback<TResult>> operation) {
        this.observable = new ObservableToPublisher<TResult>(Observables.observe(operation));
    }

    @Override
    public void subscribe(final Subscriber<? super TResult> subscriber) {
        observable.subscribe(new Subscriber<TResult>() {
            @Override
            public void onSubscribe(final Subscription s) {
                subscriber.onSubscribe(s);
            }

            @Override
            public void onNext(final TResult result) {
                subscriber.onNext(result);
            }

            @Override
            public void onError(final Throwable t) {
                if (t instanceof NullPointerException) {
                    onComplete();
                } else {
                    subscriber.onError(t);
                }
            }

            @Override
            public void onComplete() {
                subscriber.onComplete();
            }
        });
    }
}
