/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.reactivestreams.client.syncadapter;

import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoCursor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.NoSuchElementException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.TIMEOUT;

class SyncMongoCursor<T> implements MongoCursor<T> {
    private static final Object COMPLETED = new Object();
    private final BlockingDeque<Object> results = new LinkedBlockingDeque<>();
    private volatile Subscription subscription;
    private volatile T next;

    SyncMongoCursor(final Publisher<T> publisher) {
        CountDownLatch latch = new CountDownLatch(1);
        publisher.subscribe(new Subscriber<T>() {
            @Override
            public void onSubscribe(final Subscription s) {
                subscription = s;
                subscription.request(Long.MAX_VALUE);
                latch.countDown();
            }

            @Override
            public void onNext(final T t) {
                results.addLast(t);
            }

            @Override
            public void onError(final Throwable t) {
                results.addLast(t);
            }

            @Override
            public void onComplete() {
                results.addLast(COMPLETED);
            }
        });
        try {
            if (!latch.await(TIMEOUT, TimeUnit.SECONDS)) {
                throw new MongoTimeoutException("Timeout waiting for subscription");
            }
        } catch (InterruptedException e) {
            throw new MongoInterruptedException("Interrupted awaiting latch", e);
        }
    }

    @Override
    public void close() {
        subscription.cancel();
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean hasNext() {
        if (next != null) {
            return true;
        }
        try {
            Object first = results.pollFirst(TIMEOUT, TimeUnit.SECONDS);
            if (first == null) {
                throw new MongoTimeoutException("Time out waiting for result from cursor");
            } else if (first instanceof Throwable) {
                throw translateError((Throwable) first);
            } else if (first == COMPLETED) {
                return false;
            } else {
                next = (T) first;
                return true;
            }
        } catch (InterruptedException e) {
            throw new MongoInterruptedException("Interrupted waiting for next result", e);
        }
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        T retVal = next;
        next = null;
        return retVal;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public T tryNext() {
        throw new UnsupportedOperationException();  // No good way to fulfill this contract with a Publisher<T>
    }

    @Override
    public ServerCursor getServerCursor() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ServerAddress getServerAddress() {
        throw new UnsupportedOperationException();
    }

    private RuntimeException translateError(final Throwable throwable) {
        if (throwable instanceof RuntimeException) {
            return (RuntimeException) throwable;
        }
        return new RuntimeException(throwable);
    }
}
