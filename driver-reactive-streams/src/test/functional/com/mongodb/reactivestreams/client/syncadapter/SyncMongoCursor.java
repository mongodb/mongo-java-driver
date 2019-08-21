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
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoCursor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SyncMongoCursor<T> implements MongoCursor<T> {
    private volatile Subscription subscription;
    private volatile CountDownLatch latch;
    private volatile T next;
    private volatile Throwable error;
    private volatile boolean complete;

    SyncMongoCursor(final Publisher<T> publisher) {
        initLatch();
        publisher.subscribe(new Subscriber<T>() {
            @Override
            public void onSubscribe(final Subscription s) {
                subscription = s;
                latch.countDown();
            }

            @Override
            public void onNext(final T t) {
                next = t;
                latch.countDown();
            }

            @Override
            public void onError(final Throwable t) {
                error = t;
                latch.countDown();
            }

            @Override
            public void onComplete() {
                complete = true;
                latch.countDown();
            }
        });
        awaitLatch();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException();  // TODO
    }

    @Override
    public boolean hasNext() {
        if (next != null) {
            return true;
        }
        initLatch();
        subscription.request(1);
        awaitLatch();
        return !complete;
    }

    private RuntimeException translateError() {
        if (error instanceof RuntimeException) {
            return (RuntimeException) error;
        }
        return new RuntimeException(error);
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

    private void initLatch() {
        latch = new CountDownLatch(1);
    }

    private void awaitLatch() {
        try {
            latch.await(10, TimeUnit.SECONDS);
            latch = null;
            if (error != null) {
                throw translateError();
            }
        } catch (InterruptedException e) {
            throw new MongoInterruptedException("Interrupted awaiting latch", e);
        }
    }
}
