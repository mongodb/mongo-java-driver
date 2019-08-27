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

package com.mongodb.async;

import com.mongodb.MongoException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.internal.async.SingleResultCallback;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * A SingleResultCallback Future implementation.
 *
 * <p>The result of the callback is stored internally and is accessible via {@link #get}, which will either return the successful result
 * of the callback or if the callback returned an error the error will be throw.</p>
 *
 * @param <T> the result type
 * @since 3.0
 */
public class FutureResultCallback<T> implements SingleResultCallback<T>, Future<T> {
    private final CountDownLatch latch;
    private final CallbackResultHolder<T> result;

    public FutureResultCallback() {
        latch = new CountDownLatch(1);
        result = new CallbackResultHolder<T>();
    }

    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return result.isDone();
    }

    @Override
    public T get() {
        return get(5, TimeUnit.MINUTES);
    }

    @Override
    public T get(final long timeout, final TimeUnit unit) {
        try {
            if (!latch.await(timeout, unit)) {
                throw new MongoTimeoutException("Callback timed out");
            }
        } catch (InterruptedException e) {
            throw new MongoException("Latch interrupted");
        }

        if (result.hasError()) {
            if (result.getError() instanceof RuntimeException) {
                throw (RuntimeException) result.getError();
            } else if (result.getError() instanceof Error) {
                throw (Error) result.getError();
            } else {
                throw new RuntimeException("Wrapping unexpected Throwable", result.getError());
            }
        } else {
            return result.getResult();
        }
    }

    @Override
    public void onResult(final T result, final Throwable t) {
        this.result.onResult(result, t);
        latch.countDown();
    }
}
