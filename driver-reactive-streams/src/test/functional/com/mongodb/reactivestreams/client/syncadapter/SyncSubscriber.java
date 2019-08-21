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
import com.mongodb.lang.Nullable;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class SyncSubscriber<T> implements Subscriber<T> {
    private final List<T> results = new ArrayList<>();
    private volatile Throwable exception;
    private final CountDownLatch latch = new CountDownLatch(1);

    @Nullable
    T first() {
        List<T> all = all();
        if (all.isEmpty()) {
            return null;
        }
        return all.get(0);
    }

    List<T> all() {
        try {
            latch.await(10, TimeUnit.SECONDS);
            if (exception != null) {
                throw exception;
            }
            return results;
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
        s.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(final T t) {
        results.add(t);
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
