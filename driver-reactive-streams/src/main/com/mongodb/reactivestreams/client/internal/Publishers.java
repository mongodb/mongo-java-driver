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
import com.mongodb.internal.async.client.AsyncMongoCollection;
import com.mongodb.internal.async.client.AsyncMongoIterable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import java.util.List;

/**
 * Publisher helpers.
 *
 * <p>Allows async methods to be converted into event-based {@link Publisher}s.</p>
 */
public final class Publishers {

    /**
     * Convert a {@link AsyncMongoIterable} into an {@link Publisher}.
     *
     * @param mongoIterable the MongoIterable to subscribe to
     * @param <TResult>     The type of result being observed
     * @return the observable version of the mongoIterable
     */
    public static <TResult> Publisher<TResult> publish(final AsyncMongoIterable<TResult> mongoIterable) {
        return subscriber -> new MongoIterableSubscription<>(mongoIterable, subscriber);
    }

    /**
     * Allows the conversion of {@link SingleResultCallback} based operations into an {@link Publisher}
     *
     * <p>Requires a {@link Block} that is passed the callback to be used with the operation.
     * This is required to make sure that the operation only occurs once the {@link Subscription} signals for data.</p>
     * <p>
     * A typical example would be when wrapping callback based methods to make them observable. <br>
     * For example, converting {@link AsyncMongoCollection#countDocuments(SingleResultCallback)} into an {@link Publisher}:
     * <pre>
     * {@code
     *    Publisher<Long> countPublisher = observe(new Block<SingleResultCallback<Long>>() {
     *        public void apply(final SingleResultCallback<Long> callback) {
     *            collection.countDocuments(callback);
     *        }
     *    });
     * }
     * </pre>
     *
     * @param operation the block that implements the operation.
     * @param <TResult> The type of result being observed
     * @return the observable version of the callback based operation
     */
    public static <TResult> Publisher<TResult> publish(final Block<SingleResultCallback<TResult>> operation) {
        return subscriber -> new SingleResultCallbackSubscription<>(operation, subscriber);
    }

    /**
     * Allows the conversion of {@link SingleResultCallback} based operations and flattens the results in an {@link Publisher}.
     *
     * <p>Requires a {@link Block} that is passed the callback to be used with the operation.
     * This is required to make sure that the operation only occurs once the {@link Subscription} signals for data.</p>
     *
     * @param operation the operation that is passed a callback and is used to delay execution of an operation until demanded.
     * @param <TResult> The type of result being observed
     * @return a subscription
     */
    public static <TResult> Publisher<TResult> publishAndFlatten(final Block<SingleResultCallback<List<TResult>>> operation) {
        return subscriber -> new FlatteningSingleResultCallbackSubscription<>(operation, subscriber);
    }

    private Publishers() {
    }
}
