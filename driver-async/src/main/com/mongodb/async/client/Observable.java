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

package com.mongodb.async.client;

/**
 * A {@code Observable} represents a MongoDB operation.
 *
 * <p>
 * As such it is a provider of a potentially unbounded number of sequenced elements, publishing them according to the demand received
 * from its {@link Observer}(s).
 * </p>
 *
 * @param <TResult> the type of element signaled.
 * @see Observables
 */
public interface Observable<TResult> {

    /**
     * Request {@code Observable} to start streaming data.
     *
     * <p>This is a "factory method" and can be called multiple times, each time starting a new {@link Subscription}.</p>
     * <p>Each {@link Subscription} will work for only a single {@link Observer}.</p>
     * <p>
     * If the {@code Observable} rejects the subscription attempt or otherwise fails it will
     * signal the error via {@link Observer#onError}.
     * </p>
     * @param observer the {@link Observer} that will consume signals from this {@code Observable}
     */
    void subscribe(Observer<? super TResult> observer);
}
