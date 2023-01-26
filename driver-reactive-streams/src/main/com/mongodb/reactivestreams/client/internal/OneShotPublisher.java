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

import com.mongodb.annotations.NotThreadSafe;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Supplier;

/**
 * Allows creating {@link Publisher}s that do not allow calling {@link Publisher#subscribe(Subscriber)} more than once.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class OneShotPublisher {
    public static <T> Publisher<T> from(final Publisher<T> publisher) {
        return Flux.defer(new OneShotSupplier<>(publisher));
    }

    // A `Publisher` does not have to be thread-safe, therefore, `Publisher.subscribe` should not be called concurrently.
    // Hence, `OneShotSupplier.get` is not called concurrently and does not have to be thread-safe.
    @NotThreadSafe
    private static final class OneShotSupplier<T> implements Supplier<Publisher<T>> {
        private final Publisher<T> publisher;
        private boolean used;

        OneShotSupplier(final Publisher<T> publisher) {
            this.publisher = publisher;
        }

        @Override
        public Publisher<T> get() {
            if (used) {
                // we may also `throw` here, and `Flux.defer` will handle the exception and signal `onSubscribe` followed by `onError`
                return Mono.error(new IllegalStateException(
                        "This is a one-shot publisher, it does not support subscribing to it more than once."));
            }
            used = true;
            return publisher;
        }

        @Override
        public String toString() {
            return "OneShotSupplier{"
                    + "publisher=" + publisher
                    + ", used=" + used
                    + '}';
        }
    }

    private OneShotPublisher() {
    }
}
