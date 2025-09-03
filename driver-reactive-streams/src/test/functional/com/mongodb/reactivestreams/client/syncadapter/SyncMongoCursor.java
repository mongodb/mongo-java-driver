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

import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoCursor;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.internal.BatchCursor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

import java.util.NoSuchElementException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.mongodb.ClusterFixture.TIMEOUT;
import static com.mongodb.internal.thread.InterruptionUtil.interruptAndCreateMongoInterruptedException;
import static com.mongodb.reactivestreams.client.syncadapter.ContextHelper.CONTEXT;
import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.getSleepAfterCursorClose;
import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.getSleepAfterCursorError;
import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.getSleepAfterCursorOpen;
import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.isWaitForBatchCursorCreationEnabled;

class SyncMongoCursor<T> implements MongoCursor<T> {
    private static final Object COMPLETED = new Object();
    private final BlockingDeque<Object> results = new LinkedBlockingDeque<>();
    private final CompletableFuture<Object> batchCursorCompletableFuture = new CompletableFuture<>();
    private final Integer batchSize;
    private int countToBatchSize;
    private Subscription subscription;
    private T current;
    private boolean completed;
    private RuntimeException error;

    SyncMongoCursor(final Publisher<T> publisher, @Nullable final Integer batchSize) {
        this.batchSize = batchSize;
        CountDownLatch latch = new CountDownLatch(1);

        if (isWaitForBatchCursorCreationEnabled()) {
            // This hook allows us to intercept the `onNext` and `onError` signals for any operation to determine
            // whether the {@link BatchCursor} was created successfully or if an error occurred during its creation process.
            // The result is propagated to a {@link CompletableFuture}, which we use to block until it is completed.
            Hooks.onEachOperator(Operators.lift((sc, sub) ->
                    new BatchCursorInterceptSubscriber(sub, batchCursorCompletableFuture)));
        }

        //noinspection ReactiveStreamsSubscriberImplementation
        Flux.from(publisher).contextWrite(CONTEXT).subscribe(new Subscriber<T>() {
            @Override
            public void onSubscribe(final Subscription s) {
                subscription = s;
                if (batchSize == null || batchSize == 0) {
                    subscription.request(Long.MAX_VALUE);
                } else {
                    subscription.request(batchSize);
                }
                latch.countDown();
            }

            @Override
            public void onNext(final T t) {
                results.addLast(t);
            }

            @Override
            public void onError(final Throwable t) {
                results.addLast(t);
                sleep(getSleepAfterCursorError());
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
            if (isWaitForBatchCursorCreationEnabled()) {
                batchCursorCompletableFuture.get(TIMEOUT, TimeUnit.SECONDS);
                Hooks.resetOnEachOperator();
            }
            sleep(getSleepAfterCursorOpen());
        } catch (InterruptedException e) {
            throw interruptAndCreateMongoInterruptedException("Interrupted waiting for asynchronous cursor establishment", e);
        } catch (ExecutionException | TimeoutException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        subscription.cancel();
        sleep(getSleepAfterCursorClose());
    }

    private static void sleep(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw interruptAndCreateMongoInterruptedException("Interrupted from nap", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean hasNext() {
        if (error != null) {
            throw error;
        }
        if (completed) {
            return false;
        }
        if (current != null) {
            return true;
        }
        try {
            Object next;
            if (batchSize != null && batchSize != 0 && countToBatchSize == batchSize) {
                subscription.request(batchSize);
                countToBatchSize = 0;
            }
            next = results.pollFirst(TIMEOUT, TimeUnit.SECONDS);
            if (next == null) {
                throw new MongoTimeoutException("Time out waiting for result from cursor");
            } else if (next instanceof Throwable) {
                error = translateError((Throwable) next);
                sleep(getSleepAfterCursorError());
                throw error;
            } else if (next == COMPLETED) {
                completed = true;
                return false;
            } else {
                current = (T) next;
                countToBatchSize++;
                return true;
            }
        } catch (InterruptedException e) {
            throw interruptAndCreateMongoInterruptedException("Interrupted waiting for next result", e);
        }
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        T retVal = current;
        current = null;
        return retVal;
    }

    @Override
    public int available() {
        throw new UnsupportedOperationException();
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


    private static final class BatchCursorInterceptSubscriber implements CoreSubscriber<Object> {

        private final CoreSubscriber<Object> sub;
        private final CompletableFuture<Object> batchCursorCompletableFuture;

        BatchCursorInterceptSubscriber(final CoreSubscriber<Object> sub,
                                       final CompletableFuture<Object> batchCursorCompletableFuture) {
            this.sub = sub;
            this.batchCursorCompletableFuture = batchCursorCompletableFuture;
        }

        @Override
        public Context currentContext() {
            return sub.currentContext();
        }

        @Override
        public void onSubscribe(final Subscription s) {
            sub.onSubscribe(s);
        }

        @Override
        public void onNext(final Object o) {
            if (o instanceof BatchCursor) {
                // Interception of a cursor means that it has been created at this point.
                batchCursorCompletableFuture.complete(o);
            }
            sub.onNext(o);
        }

        @Override
        public void onError(final Throwable t) {
            if (!batchCursorCompletableFuture.isDone()) { // Cursor has not been created yet but an error occurred.
                batchCursorCompletableFuture.completeExceptionally(t);
            }
            sub.onError(t);
        }

        @Override
        public void onComplete() {
            sub.onComplete();
        }
    }
}
