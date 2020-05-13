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

import com.mongodb.MongoException;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.lang.Nullable;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;


abstract class AbstractSubscription<TResult> implements Subscription {
    private static final Logger LOGGER = Loggers.getLogger("client");
    private final Subscriber<? super TResult> subscriber;

    /* protected by `this` */
    private boolean requestedData;
    private boolean isProcessing;
    private long requested = 0;
    private boolean isUnsubscribed = false;
    private boolean isTerminated = false;
    /* protected by `this` */

    private final ConcurrentLinkedQueue<TResult> resultsQueue = new ConcurrentLinkedQueue<TResult>();

    AbstractSubscription(final Subscriber<? super TResult> subscriber) {
        this.subscriber = subscriber;
    }

    public void unsubscribe() {
        boolean unsubscribe = false;

        synchronized (this) {
            if (!isUnsubscribed) {
                unsubscribe = true;
                isUnsubscribed = true;
                isTerminated = true;
            }
        }

        if (unsubscribe) {
            postTerminate();
        }
    }

    public synchronized boolean isUnsubscribed() {
        return isUnsubscribed;
    }

    @Override
    public void cancel() {
        unsubscribe();
    }

    @Override
    public void request(final long n) {

        if (isTerminated()) {
            return;
        }
        if (!isUnsubscribed() && n < 1) {
            onError(new IllegalArgumentException("3.9 While the Subscription is not cancelled, "
                    + "Subscription.request(long n) MUST throw a java.lang.IllegalArgumentException if the "
                    + "argument is <= 0."));
            return;
        }

        boolean requestData = false;
        synchronized (this) {
            if (requested + n < 1) {
                requested = Long.MAX_VALUE;
            } else {
                requested += n;
            }
            if (!requestedData) {
                requestedData = true;
                requestData = true;
            }
        }

        if (requestData) {
            tryRequestInitialData();
        } else {
            tryProcessResultsQueue();
        }
    }

    abstract void requestInitialData();

    void requestMoreData() {
    }

    void postTerminate() {
    }

    abstract boolean checkCompleted();

    synchronized boolean isTerminated() {
        return isTerminated;
    }

    synchronized long getRequested() {
        return requested;
    }

    void addToQueue(@Nullable final TResult result) {
        if (result != null) {
            resultsQueue.add(result);
        }
    }

    void addToQueue(@Nullable final List<TResult> results) {
        if (results != null) {
            for (TResult cur : results) {
                addToQueue(cur);
            }
        }
    }

    void onError(final Throwable t) {
        if (terminalAction()) {
            postTerminate();
            try {
                subscriber.onError(t);
            } catch (Throwable t1) {
                LOGGER.error("Calling onError threw an exception", t1);
                throw MongoException.fromThrowableNonNull(t1);
            }
        } else {
            throw new MongoException("Subscription has already been terminated", t);
        }
    }

    private void onNext(final TResult next) {
        if (!isTerminated()) {
            try {
                subscriber.onNext(next);
            } catch (Throwable t) {
                LOGGER.error("Calling onNext threw an exception", t);
                onError(t);
            }
        }
    }

    private void onComplete() {
        if (terminalAction()) {
            postTerminate();
            try {
                subscriber.onComplete();
            } catch (Throwable t) {
                LOGGER.error("Calling onComplete threw an exception", t);
                throw MongoException.fromThrowableNonNull(t);
            }
        }
    }

    private void tryRequestInitialData() {
        try {
            requestInitialData();
        } catch (Throwable t) {
            onError(t);
        }
    }

    void tryProcessResultsQueue() {
        try {
            processResultsQueue();
        } catch (Throwable t) {
            onError(t);
        }
    }

    @SuppressWarnings("unchecked")
    private void processResultsQueue() {
        boolean mustProcess = false;

        synchronized (this) {
            if (!isProcessing && !isTerminated) {
                isProcessing = true;
                mustProcess = true;
            }
        }

        if (mustProcess) {
            boolean requestMore = false;

            long processedCount = 0;
            boolean completed = false;
            while (true) {
                long localWanted;

                synchronized (this) {
                    requested -= processedCount;
                    if (resultsQueue.isEmpty()) {
                        completed = checkCompleted();
                        requestMore = requested > 0;
                        isProcessing = false;
                        break;
                    } else if (requested == 0) {
                        isProcessing = false;
                        break;
                    }
                    localWanted = requested;
                }
                processedCount = 0;

                while (localWanted > 0) {
                    TResult item = resultsQueue.poll();
                    if (item == null) {
                        break;
                    } else {
                        onNext(item);
                        localWanted -= 1;
                        processedCount += 1;
                    }
                }
            }

            if (completed) {
                onComplete();
            } else if (requestMore) {
                requestMoreData();
            }
        }
    }

    private boolean terminalAction() {
        boolean isTerminal = false;
        synchronized (this) {
            if (!isTerminated) {
                isTerminated = true;
                isTerminal = true;
            }
        }
        return isTerminal;
    }

}
