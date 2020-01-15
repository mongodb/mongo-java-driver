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
import com.mongodb.lang.Nullable;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.TIMEOUT;

class SingleResultSubscriber<T> implements Subscriber<T> {
    private volatile T result;
    private volatile Throwable exception;
    private final CountDownLatch latch = new CountDownLatch(1);

    @Nullable
    T get() {
        try {
            if (!latch.await(TIMEOUT, TimeUnit.SECONDS)) {
                throw new MongoTimeoutException("Timeout waiting for single result");
            }
            if (exception != null) {
                throw exception;
            }
            return result;
        } catch (InterruptedException e) {
            throw new MongoInterruptedException("Test interrupted", e);
        } catch (RuntimeException runtimeException) {
            throw runtimeException;
        } catch (Throwable throwable) {
            throw new RuntimeException("Wrapped exception", throwable);
        }
    }

    @Override
    public void onSubscribe(final Subscription s) {
        s.request(2);
    }

    @Override
    public void onNext(final T t) {
        result = t;
    }

    @Override
    public void onError(final Throwable t) {
        exception = t;
        latch.countDown();
    }

    @Override
    public void onComplete() {
        latch.countDown();
    }
}
