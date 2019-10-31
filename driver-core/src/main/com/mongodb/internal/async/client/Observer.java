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

/**
 * Provides a mechanism for receiving push-based notifications.
 *
 * <p>
 * Will receive a call to {@link #onSubscribe(Subscription)} on subscription to the {@link Observable}.
 * No further notifications will be received until {@link Subscription#request(long)} is called.
 * </p>
 * <p>
 * After signaling demand:
 * <ul>
 *   <li>One or more invocations of {@link #onNext(Object)} up to the maximum number defined by {@link Subscription#request(long)}</li>
 *   <li>Single invocation of {@link #onError(Throwable)} or {@link Observer#onComplete()} which signals a terminal state after which no
 * further events will be sent.</li>
 * </ul>
 * <p>
 * Demand can be signaled via {@link Subscription#request(long)} whenever the {@link Observer} instance is capable of handling more.
 *</p>
 *
 * @param <TResult> The type of element signaled.
 */
public interface Observer<TResult> {

    /**
     * Invoked on subscription to an {@link Observable}.
     * <p>
     * No operation will happen until {@link Subscription#request(long)} is invoked.
     * </p>
     * <p>
     * It is the responsibility of this Subscriber instance to call {@link Subscription#request(long)} whenever more data is wanted.
     * </p>
     * <p>
     * The {@link AsyncMongoIterable} will send notifications only in response to {@link Subscription#request(long)}.
     * </p>
     *
     * @param subscription {@link Subscription} that allows requesting data via {@link Subscription#request(long)}
     */
    void onSubscribe(Subscription subscription);

    /**
     * Provides the Observer with a new item to observe.
     * <p>
     * The Observer may call this method 0 or more times.
     * </p>
     * <p>
     * The {@link Observable} will not call this method again after it calls either {@link #onComplete} or
     * {@link #onError}.
     *</p>
     *
     * @param result the item emitted by the {@link Observable}
     */
    void onNext(TResult result);

    /**
     * Notifies the Observer that the {@link Observable} has experienced an error condition.
     *
     * <p> If the {@link Observable} calls this method, it will not thereafter call {@link #onNext} or {@link #onComplete}.</p>
     *
     * @param e the exception encountered by the {@link Observable}
     */
    void onError(Throwable e);

    /**
     * Notifies the Subscriber that the {@link Observable} has finished sending push-based notifications.
     * <p>
     * The {@link Observable} will not call this method if it calls {@link #onError}.
     * </p>
     */
    void onComplete();
}
