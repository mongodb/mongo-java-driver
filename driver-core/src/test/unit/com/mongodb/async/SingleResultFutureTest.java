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

package com.mongodb.async;

import category.Slow;
import com.mongodb.MongoException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SingleResultFutureTest {

    @Test
    public void testInitFailureWhenMongoExceptionThrown() {
        SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();

        try {
            future.init(1, new MongoException("bad"));
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertFalse(future.isDone());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testInitFailure() {
        SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();
        future.init(1, null);
        future.init(2, null);
    }

    @Test
    public void testInitSuccessWithResult() throws ExecutionException, InterruptedException {
        SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();
        future.init(1, null);
        assertTrue(future.isDone());
        assertEquals(1, (int) future.get());
    }

    @Test
    public void testConstructorSuccessWithResult() throws ExecutionException, InterruptedException {
        SingleResultFuture<Integer> future = new SingleResultFuture<Integer>(1, null);
        assertTrue(future.isDone());
        assertEquals(1, (int) future.get());
    }

    @Test
    public void testInitSuccessWithException() throws ExecutionException, InterruptedException {
        SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();
        MongoException mongoException = new MongoException("bad");
        future.init(null, mongoException);
        try {
            future.get();
            fail();
        } catch (MongoException e) {
            assertTrue(future.isDone());
            assertEquals(mongoException, e);
        }
    }

    @Test
    public void testSuccessfulCancel() throws ExecutionException, InterruptedException {
        SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();
        assertFalse(future.isDone());
        assertFalse(future.isCancelled());
        boolean wasCancelled = future.cancel(true);
        assertTrue(wasCancelled);
        assertTrue(future.isCancelled());
        assertTrue(future.isDone());
        try {
            future.get();
        } catch (CancellationException e) { // NOPMD
        }
    }

    @Test
    public void testUnsuccessfulCancel() throws ExecutionException, InterruptedException {
        SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();
        future.init(1, null);
        boolean wasCancelled = future.cancel(true);
        assertFalse(wasCancelled);
        assertFalse(future.isCancelled());
        assertEquals(1, (int) future.get());
    }

    @Test
    @Category(Slow.class)
    public void testCancellationNotification() throws InterruptedException {
        final SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    future.get();
                } catch (CancellationException e) {
                    latch.countDown();
                }
            }
        }).start();
        Thread.sleep(500);
        future.cancel(true);
        latch.await();
    }

    @Test
    public void testInitAfterCancel() throws ExecutionException, InterruptedException {
        SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();
        future.cancel(true);
        future.init(1, null);
        try {
            future.get();
        } catch (CancellationException e) { // NOPMD
            // all good
        }
    }

    @Test(expected = TimeoutException.class)
    public void testGetTimeout() throws InterruptedException, ExecutionException, TimeoutException {
        SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();
        future.get(1, TimeUnit.MILLISECONDS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullTimeUnit() throws InterruptedException, ExecutionException, TimeoutException {
        SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();
        future.get(1, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullCallback() throws InterruptedException {
        SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();
        future.register(null);
    }

    @Test
    public void testCallbackRegisteredAfterInit() throws InterruptedException {
        SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();
        final CountDownLatch latch = new CountDownLatch(1);

        future.init(1, null);

        future.register(new SingleResultCallback<Integer>() {
            @Override
            public void onResult(final Integer result, final MongoException e) {
                latch.countDown();
            }
        });

        latch.await();
    }

    @Test
    public void testCallbackRegisteredBeforeInit() throws InterruptedException {
        SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();
        final CountDownLatch latch = new CountDownLatch(1);

        future.register(new SingleResultCallback<Integer>() {
            @Override
            public void onResult(final Integer result, final MongoException e) {
                latch.countDown();
            }
        });

        future.init(1, null);

        latch.await();
    }

    @Test
    public void testMultipleCallbacksRegisteredBeforeInit() throws InterruptedException {
        SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();
        final CountDownLatch latch = new CountDownLatch(2);

        future.register(new SingleResultCallback<Integer>() {
            @Override
            public void onResult(final Integer result, final MongoException e) {
                latch.countDown();
            }
        });
        future.register(new SingleResultCallback<Integer>() {
            @Override
            public void onResult(final Integer result, final MongoException e) {
                latch.countDown();
            }
        });

        future.init(1, null);

        latch.await();
    }

    @Test(expected = CancellationException.class)
    public void testCallbackRegisteredAfterCancel() throws InterruptedException {
        SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();
        future.cancel(true);

        future.register(new SingleResultCallback<Integer>() {
            @Override
            public void onResult(final Integer result, final MongoException e) {
            }
        });
    }
}
