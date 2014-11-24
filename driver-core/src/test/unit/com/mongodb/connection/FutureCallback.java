package com.mongodb.connection;

import com.mongodb.MongoException;
import com.mongodb.async.SingleResultCallback;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class FutureCallback<T> implements SingleResultCallback<T>, Future<T> {
    private final CountDownLatch latch = new CountDownLatch(1);
    private Throwable throwable;
    private T result;

    @Override
    public void onResult(final T result, final MongoException throwable) {
        this.throwable = throwable;
        this.result = result;
        latch.countDown();
    }

    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return latch.getCount() == 0;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        latch.await();
        if (throwable != null) {
            throw new ExecutionException(throwable);
        }
        return result;
    }

    @Override
    public T get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return get();
    }
}
