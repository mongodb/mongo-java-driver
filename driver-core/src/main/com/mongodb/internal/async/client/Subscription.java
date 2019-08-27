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
 * A {@code Subscription} represents a one-to-one lifecycle of a {@link Observer} subscribing to an {@link Observable}.
 * <p>
 * Instances can only be used once by a single {@link Observer}.
 * </p>
 * <p>
 * It is used to both signal desire for data and to allow for unsubscribing.
 * </p>
 *
 * @since 3.1
 * @deprecated Prefer the Reactive Streams-based asynchronous driver (mongodb-driver-reactivestreams artifactId)
 */
@Deprecated
public interface Subscription {

    /**
     * No operation will be sent to MongoDB from the {@link Observable} until demand is signaled via this method.
     * <p>
     * It can be called however often and whenever needed, but the outstanding cumulative demand must never exceed {@code Long.MAX_VALUE}.
     * An outstanding cumulative demand of {@code Long.MAX_VALUE} may be treated by the {@link Observable} as "effectively unbounded".
     * </p>
     * <p>
     * Whatever has been requested might be sent, so only signal demand for what can be safely handled.
     * <p>
     * An {@link Observable} can send less than is requested if the stream ends but then must emit either
     * {@link Observer#onError(Throwable)} or {@link Observer#onComplete()}.
     * </p>
     * @param n the strictly positive number of elements to requests to the upstream {@link Observable}
     */
    void request(long n);

    /**
     * Request the {@link Observable} to stop sending data and clean up resources.
     * <p>
     * As this request is asynchronous data may still be sent to meet previously signalled demand after calling cancel.
     * </p>
     */
    void unsubscribe();

    /**
     * Indicates whether this {@code Subscription} is currently unsubscribed.
     *
     * @return {@code true} if this {@code Subscription} is currently unsubscribed, {@code false} otherwise
     */
    boolean isUnsubscribed();

}
