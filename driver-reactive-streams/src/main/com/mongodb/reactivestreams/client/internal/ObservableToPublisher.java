/*
 * Copyright 2015 MongoDB, Inc.
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

import org.reactivestreams.Subscriber;

import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings("deprecation")
public class ObservableToPublisher<TResult> implements org.reactivestreams.Publisher<TResult> {

    private final com.mongodb.async.client.Observable<TResult> observable;

    public ObservableToPublisher(final com.mongodb.async.client.Observable<TResult> observable) {
        this.observable = observable;
    }

    @Override
    public void subscribe(final Subscriber<? super TResult> subscriber) {
        observable.subscribe(new com.mongodb.async.client.Observer<TResult>() {
            @Override
            public void onSubscribe(final com.mongodb.async.client.Subscription subscription) {
                subscriber.onSubscribe(new org.reactivestreams.Subscription() {
                    private final AtomicBoolean cancelled = new AtomicBoolean();

                    @Override
                    public void request(final long n) {
                        if (!subscription.isUnsubscribed() && n < 1) {
                            subscriber.onError(new IllegalArgumentException("3.9 While the Subscription is not cancelled, "
                                    + "Subscription.request(long n) MUST throw a java.lang.IllegalArgumentException if the "
                                    + "argument is <= 0."));
                        } else {
                            try {
                                subscription.request(n);
                            } catch (Throwable t) {
                                subscriber.onError(t);
                            }
                        }
                    }

                    @Override
                    public void cancel() {
                        if (!cancelled.getAndSet(true)) {
                            subscription.unsubscribe();
                        }
                    }
                });
            }

            @Override
            public void onNext(final TResult result) {
                subscriber.onNext(result);
            }

            @Override
            public void onError(final Throwable e) {
                subscriber.onError(e);
            }

            @Override
            public void onComplete() {
                subscriber.onComplete();
            }
        });
    }
}
