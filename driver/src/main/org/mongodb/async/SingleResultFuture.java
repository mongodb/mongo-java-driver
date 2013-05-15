/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.async;

import org.mongodb.MongoException;
import org.mongodb.MongoFuture;
import org.mongodb.annotations.ThreadSafe;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

// TODO: Should this be public?
@ThreadSafe
public class SingleResultFuture<T> implements MongoFuture<T> {
    private T result;
    private MongoException exception;
    private boolean isDone;
    private boolean isCancelled;
    private SingleResultCallback<T> callback;

    public SingleResultFuture() {
    }

    public SingleResultFuture(final T result, final MongoException newException) {
        init(result, newException);
    }

    public synchronized void init(final T newResult, final MongoException newException) {
        if (isCancelled()) {
            return;
        }

        if (isDone()) {
            throw new IllegalArgumentException("already initialized");
        }

        if (newResult != null && newException != null) {
            throw new IllegalArgumentException("result and exception can't both not be null");
        }

        this.isDone = true;
        this.result = newResult;
        this.exception = newException;

        notifyAll();

        if (callback != null) {
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
    public synchronized T get() throws InterruptedException, ExecutionException {
        try {
            return get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new IllegalStateException("This really shouldn't happen with such a long timeout");
        }
    }

    @Override
    public synchronized T get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (unit == null) {
            throw new IllegalArgumentException("Time unit can not be null");

        }
        if (!isDone()) {
            wait(unit.toMillis(timeout));
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
    public synchronized void register(final SingleResultCallback<T> newCallback) {
        if (callback != null) {
            throw new IllegalStateException("Can not register more than one callback");
        }

        if (newCallback == null) {
            throw new IllegalArgumentException("Callback can not be null");
        }

        callback = newCallback;

        if (!isDone()) {
            return;
        }

        if (isCancelled()) {
            throw new CancellationException();
        }

        newCallback.onResult(result, exception);
    }
}
