/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class TestSubscriber<T> implements Subscriber<T> {

    private final Subscriber<T> delegate;
    private final CountDownLatch latch = new CountDownLatch(1);
    private final ArrayList<T> onNextEvents = new ArrayList<T>();
    private final ArrayList<Throwable> onErrorEvents = new ArrayList<Throwable>();
    private final ArrayList<Void> onCompleteEvents = new ArrayList<Void>();

    private Subscription subscription;

    public TestSubscriber(final Subscriber<T> delegate) {
        this.delegate = delegate;
    }

    public TestSubscriber() {
        this(new Subscriber<T>() {

            @Override
            public void onSubscribe(final Subscription subscription) {
                // do nothing
            }

            @Override
            public void onNext(final T result) {
                // do nothing
            }

            @Override
            public void onComplete() {
                // do nothing
            }

            @Override
            public void onError(final Throwable e) {
                // do nothing
            }
        });
    }

    @Override
    public void onSubscribe(final Subscription subscription) {
        this.subscription = subscription;
    }

    /**
     * Provides the Subscriber with a new item to observe.
     * <p>
     * The {@code Publisher} may call this method 0 or more times.
     * </p>
     * <p>
     * The {@code Publisher} will not call this method again after it calls either {@link #onComplete} or
     * {@link #onError}.
     * </p>
     * @param result the item emitted by the obserable
     */
    @Override
    public void onNext(final T result) {
        onNextEvents.add(result);
        delegate.onNext(result);
    }

    /**
     * Notifies the Subscriber that the obserable has experienced an error condition.
     * <p>
     * If the obserable calls this method, it will not thereafter call {@link #onNext} or
     * {@link #onComplete}.
     * </p>
     *
     * @param e the exception encountered by the obserable
     */
    @Override
    public void onError(final Throwable e) {
        try {
            onErrorEvents.add(e);
            delegate.onError(e);
        } finally {
            latch.countDown();
        }
    }

    /**
     * Notifies the Subscriber that the obserable has finished sending push-based notifications.
     * <p>
     * The obserable will not call this method if it calls {@link #onError}.
     * </p>
     */
    @Override
    public void onComplete() {
        try {
            onCompleteEvents.add(null);
            delegate.onComplete();
        } finally {
            latch.countDown();
        }
    }

    /**
     * Allow calling the protected {@link Subscription#request(long)} from unit tests.
     *
     * @param n the maximum number of items you want the obserable to emit to the Subscriber at this time, or
     *          {@code Long.MAX_VALUE} if you want the obserable to emit items at its own pace
     */
    public void requestMore(final long n) {
        subscription.request(n);
    }

    /**
     * Get the {@link Throwable}s this {@code Subscriber} was notified of via {@link #onError} as a
     * {@link List}.
     *
     * @return a list of the Throwables that were passed to this Subscriber's {@link #onError} method
     */
    public List<Throwable> getOnErrorEvents() {
        return onErrorEvents;
    }

    /**
     * Get the sequence of items observed by this {@link Subscriber}, as an ordered {@link List}.
     *
     * @return a list of items observed by this Subscriber, in the order in which they were observed
     */
    public List<T> getOnNextEvents() {
        return onNextEvents;
    }

    /**
     * Returns the subscription to the this {@link Subscriber}.
     *
     * @return the subscription or null if not subscribed to
     */
    public Subscription getSubscription() {
        return subscription;
    }

    /**
     * Assert that a particular sequence of items was received by this {@link Subscriber} in order.
     *
     * @param items the sequence of items expected to have been observed
     * @throws AssertionError if the sequence of items observed does not exactly match {@code items}
     */
    public void assertReceivedOnNext(final List<T> items) {
        if (getOnNextEvents().size() != items.size()) {
            throw new AssertionError("Number of items does not match. Provided: " + items.size() + "  Actual: " + getOnNextEvents().size());
        }

        for (int i = 0; i < items.size(); i++) {
            if (items.get(i) == null) {
                // check for null equality
                if (onNextEvents.get(i) != null) {
                    throw new AssertionError("Value at index: " + i + " expected to be [null] but was: [" + getOnNextEvents().get(i) + "]");
                }
            } else if (!items.get(i).equals(getOnNextEvents().get(i))) {
                throw new AssertionError("Value at index: " + i + " expected to be ["
                        + items.get(i) + "] (" + items.get(i).getClass().getSimpleName() + ") but was: [" + getOnNextEvents().get(i)
                        + "] (" + getOnNextEvents().get(i).getClass().getSimpleName() + ")");

            }
        }
    }

    /**
     * Assert that a single terminal event occurred, either {@link #onComplete} or {@link #onError}.
     *
     * @throws AssertionError if not exactly one terminal event notification was received
     */
    public void assertTerminalEvent() {
        if (onErrorEvents.size() > 1) {
            throw new AssertionError("Too many onError events: " + onErrorEvents.size());
        }

        if (onCompleteEvents.size() > 1) {
            throw new AssertionError("Too many onCompleted events: " + onCompleteEvents.size());
        }

        if (onCompleteEvents.size() == 1 && onErrorEvents.size() == 1) {
            throw new AssertionError("Received both an onError and onCompleted. Should be one or the other.");
        }

        if (onCompleteEvents.size() == 0 && onErrorEvents.size() == 0) {
            throw new AssertionError("No terminal events received.");
        }
    }

    /**
     * Assert that no terminal event occurred, either {@link #onComplete} or {@link #onError}.
     *
     * @throws AssertionError if a terminal event notification was received
     */
    public void assertNoTerminalEvent() {
        if (onCompleteEvents.size() != 0 && onErrorEvents.size() != 0) {
            throw new AssertionError("Terminal events received.");
        }
    }

    /**
     * Assert that this {@link Subscriber} has received no {@code onError} notifications.
     *
     * @throws AssertionError if this {@link Subscriber} has received one or more {@link #onError} notifications
     */
    public void assertNoErrors() {
        if (onErrorEvents.size() > 0) {
            // can't use AssertionError because (message, cause) doesn't exist until Java 7
            throw new RuntimeException("Unexpected onError events: " + getOnErrorEvents().size(), getOnErrorEvents().get(0));
        }
    }

    /**
     * Assert that this {@link Subscriber} has received an {@code onError} notification.
     *
     * @throws AssertionError if this {@link Subscriber} did not received an {@link #onError} notifications
     */
    public void assertErrored() {
        if (onErrorEvents.size() == 0) {
            // can't use AssertionError because (message, cause) doesn't exist until Java 7
            throw new RuntimeException("No onError events");
        }
    }
}
