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
 *
 */

package org.mongodb.impl;

import org.mongodb.MongoException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class SingleResultFuture<T> implements Future<T> {
    private T result;
    private MongoException exception;

    synchronized void init(final T newResult, final MongoException newException) {
        this.result = newResult;
        this.exception = newException;
        notifyAll();
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
    public synchronized boolean isDone() {
        return result != null || exception != null;
    }

    @Override
    public synchronized T get() throws InterruptedException, ExecutionException {
        while (!isDone()) {
            wait();
        }
        if (exception != null) {
            throw new ExecutionException(exception);
        }
        return result;
    }

    @Override
    public T get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (!isDone()) {
            wait(unit.toMillis(timeout));
        }
        if (!isDone()) {
            throw new TimeoutException(String.format("Timed out waiting for %d %s", timeout, unit));
        }
        if (exception != null) {
            throw new ExecutionException(exception);
        }
        return result;
    }
}
