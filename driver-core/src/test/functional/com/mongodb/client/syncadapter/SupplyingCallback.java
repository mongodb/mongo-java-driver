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
package com.mongodb.client.syncadapter;

import com.mongodb.annotations.ThreadSafe;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.lang.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

@ThreadSafe
public final class SupplyingCallback<R> implements SingleResultCallback<R>, Supplier<R> {
    public static final long TIMEOUT_MINUTES = 1;

    private final CompletableFuture<R> future;

    public SupplyingCallback() {
        future = new CompletableFuture<>();
    }

    @Override
    public void onResult(@Nullable final R result, @Nullable final Throwable t) {
        if (t != null) {
            future.completeExceptionally(t);
        } else {
            future.complete(result);
        }
    }

    @Override
    public R get() {
        try {
            return future.get(TIMEOUT_MINUTES, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause == null) {
                throw new RuntimeException(e);
            } else if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else if (cause instanceof Error) {
                throw (Error) cause;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public boolean completed() {
        return future.isDone();
    }
}
