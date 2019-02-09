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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class TestObserver<T> implements Observer<T> {

    private final Observer<T> delegate;
    private final CountDownLatch latch = new CountDownLatch(1);
    private final ArrayList<T> onNextEvents = new ArrayList<T>();
    private final ArrayList<Throwable> onErrorEvents = new ArrayList<Throwable>();
    private final ArrayList<Void> onCompleteEvents = new ArrayList<Void>();

    private Subscription subscription;

    public TestObserver(final Observer<T> delegate) {
        this.delegate = delegate;
    }

    public TestObserver() {
        this(new Observer<T>() {

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
    public synchronized void onSubscribe(final Subscription subscription) {
        this.subscription = subscription;
        delegate.onSubscribe(subscription);
    }

    /**
     * Provides the Subscriber with a new item to observe.
     * <p>
     * The {@link Observable} may call this method 0 or more times.
     * </p>
     * <p>
     * The {@link Observable} will not call this method again after it calls either {@link #onComplete} or
     * {@link #onError}.
     * </p>
     * @param result the item emitted by the obserable
     */
    @Override
    public synchronized void onNext(final T result) {
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
    public synchronized void onError(final Throwable e) {
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
    public synchronized void onComplete() {
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
    public synchronized void requestMore(final long n) {
        subscription.request(n);
    }

    /**
     * Get the {@link Throwable}s this {@link Observer} was notified of via {@link #onError} as a
     * {@link List}.
     *
     * @return a list of the Throwables that were passed to this Subscriber's {@link #onError} method
     */
    public synchronized List<Throwable> getOnErrorEvents() {
        return onErrorEvents;
    }

    /**
     * Get the sequence of items observed by this {@link Observer}, as an ordered {@link List}.
     *
     * @return a list of items observed by this Subscriber, in the order in which they were observed
     */
    public synchronized List<T> getOnNextEvents() {
        return onNextEvents;
    }

    /**
     * Returns the subscription to the this {@link Observer}.
     *
     * @return the subscription or null if not subscribed to
     */
    public synchronized Subscription getSubscription() {
        return subscription;
    }

    /**
     * Assert that a particular sequence of items was received by this {@link Observer} in order.
     *
     * @param items the sequence of items expected to have been observed
     * @throws AssertionError if the sequence of items observed does not exactly match {@code items}
     */
    public synchronized void assertReceivedOnNext(final List<T> items) {
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
    public synchronized void assertTerminalEvent() {
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
    public synchronized void assertNoTerminalEvent() {
        if (onCompleteEvents.size() > 0 || onErrorEvents.size() > 0) {
            throw new AssertionError("Terminal events received.");
        }
    }

    /**
     * Assert that the {@link Subscription} is unsubscribed.
     *
     * @throws AssertionError if this {@link Subscription} is not unsubscribed
     */
    public synchronized void assertUnsubscribed() {
        if (subscription == null || !subscription.isUnsubscribed()) {
            throw new AssertionError("Not unsubscribed.");
        }
    }

    /**
     * Assert that the {@link Subscription} is subscribed.
     *
     * @throws AssertionError if this {@link Subscription} is not subscribed
     */
    public void assertSubscribed() {
        if (subscription == null || subscription.isUnsubscribed()) {
            throw new AssertionError("Not subscribed.");
        }
    }

    /**
     * Assert that this {@link Observer} has received no {@code onError} notifications.
     *
     * @throws AssertionError if this {@link Observer} has received one or more {@link #onError} notifications
     */
    public synchronized void assertNoErrors() {
        if (onErrorEvents.size() > 0) {
            throw new AssertionError("Unexpected onError events: " + getOnErrorEvents().size(), getOnErrorEvents().get(0));
        }
    }

    /**
     * Assert that this {@link Observer} has received an {@code onError} notification.
     *
     * @throws AssertionError if this {@link Observer} did not received an {@link #onError} notifications
     */
    public synchronized void assertErrored() {
        if (onErrorEvents.size() == 0) {
            throw new AssertionError("No onError events");
        }
    }
}
