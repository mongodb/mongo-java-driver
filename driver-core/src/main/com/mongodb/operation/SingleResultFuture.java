/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.operation;

import com.mongodb.MongoException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.async.MongoFuture;
import com.mongodb.connection.SingleResultCallback;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * This class is not part of the public API.  Changes that affect binary compatibility may be made without notice.
 *
 * @param <T> the future type
 * @since 3.0
 */
@ThreadSafe
public class SingleResultFuture<T> implements MongoFuture<T> {
    private T result;
    private MongoException exception;
    private boolean isDone;
    private boolean isCancelled;
    private final List<SingleResultCallback<T>> callbacks = new ArrayList<SingleResultCallback<T>>();

    public SingleResultFuture() {
    }

    public SingleResultFuture(final T result, final MongoException newException) {
        init(result, newException);
    }

    public SingleResultFuture(final T result) {
        init(result, null);
    }

    public synchronized void init(final T newResult, final MongoException newException) {
        if (isCancelled()) {
            return;
        }

        if (isDone()) {
            if (newException != null) {
                throw new IllegalStateException("Illegal re-initialization of future with exception.  Already initialized with " + result,
                                                newException);
            } else {
                throw new IllegalStateException("Illegal re-initialization of future with result: " + newResult);
            }
        }

        if (newResult != null && newException != null) {
            throw new IllegalArgumentException("result and exception can't both not be null");
        }

        this.isDone = true;
        this.result = newResult;
        this.exception = newException;

        notifyAll();

        for (final SingleResultCallback<T> callback : callbacks) {
            callback.onResult(result, exception);
        }
    }

    @Override
    public synchronized boolean cancel(final boolean mayInterruptIfRunning) {
        if (isDone()) {
            return false;
        }
        if (!isCancelled) {
            isCancelled = true;
            isDone = true;
            notifyAll();
        }
        return true;
    }

    @Override
    public boolean isCancelled() {
        return isCancelled;
    }

    @Override
    public synchronized boolean isDone() {
        return isDone;
    }

    @Override
    public synchronized T get() {
        try {
            return get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new IllegalStateException("This really shouldn't happen with such a long timeout");
        }
    }

    @Override
    public synchronized T get(final long timeout, final TimeUnit unit) throws TimeoutException {
        notNull("timeUnit", unit);
        if (!isDone()) {
            try {
                wait(unit.toMillis(timeout));
            } catch (InterruptedException e) {
                throw new MongoInterruptedException("Interrupted", e);
            }
        }

        if (isCancelled()) {
            throw new CancellationException();
        }

        if (!isDone()) {
            throw new TimeoutException(String.format("Timed out waiting for %d %s", timeout, unit));
        }

        if (exception != null) {
            throw exception;
        }
        return result;
    }

    @Override
    public synchronized void register(final SingleResultCallback<T> callback) {
        if (callback == null) {
            throw new IllegalArgumentException("Callback can not be null");
        }

        if (isCancelled()) {
            throw new CancellationException();
        }

        if (isDone()) {
            callback.onResult(result, exception);
        } else {
            callbacks.add(callback);
        }
    }
}
