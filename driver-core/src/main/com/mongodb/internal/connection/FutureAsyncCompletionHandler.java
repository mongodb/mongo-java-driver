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

package com.mongodb.internal.connection;

import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.lang.Nullable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.mongodb.internal.thread.InterruptionUtil.interruptAndCreateMongoInterruptedException;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class FutureAsyncCompletionHandler<T> implements AsyncCompletionHandler<T> {
    private final CompletableFuture<T> completableFuture = new CompletableFuture<>();

    @Override
    public void completed(@Nullable final T t) {
        completableFuture.complete(t);
    }

    @Override
    public void failed(final Throwable t) {
        completableFuture.completeExceptionally(t);
    }

    public T get() throws IOException {
        try {
            return completableFuture.get();
        } catch (InterruptedException e) {
            throw interruptAndCreateMongoInterruptedException("Interrupted", e);
        } catch (ExecutionException executionException) {
            Throwable t = executionException.getCause();
            if (t instanceof IOException) {
                throw (IOException) t;
            } else if (t instanceof MongoException) {
                throw (MongoException) t;
            } else {
                throw new MongoInternalException("Exception thrown from Stream", t);
            }
        }
    }
}
