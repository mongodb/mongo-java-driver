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

import org.junit.Test;
import org.mongodb.MongoException;
import org.mongodb.async.SingleResultCallback;
import org.mongodb.async.SingleResultFuture;

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
        final SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();

        try {
            future.init(1, new MongoException("bad"));
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertFalse(future.isDone());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitFailure() {
        final SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();
        future.init(1, null);
        future.init(2, null);
    }

    @Test
    public void testInitSuccessWithResult() throws ExecutionException, InterruptedException {
        final SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();
        future.init(1, null);
        assertTrue(future.isDone());
        assertEquals(1, (int) future.get());
    }

    @Test
    public void testInitSuccessWithException() throws ExecutionException, InterruptedException {
        final SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();
        final MongoException mongoException = new MongoException("bad");
        future.init(null, mongoException);
        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(future.isDone());
            assertEquals(mongoException, e.getCause());
        }
    }

    @Test
    public void testSuccessfulCancel() throws ExecutionException, InterruptedException {
        final SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();
        assertFalse(future.isDone());
        assertFalse(future.isCancelled());
        final boolean wasCancelled = future.cancel(true);
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
        final SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();
        future.init(1, null);
        final boolean wasCancelled = future.cancel(true);
        assertFalse(wasCancelled);
        assertFalse(future.isCancelled());
        assertEquals(1, (int) future.get());
    }

    @Test
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
                } catch (InterruptedException e) { // NOPMD
                    // all good
                } catch (ExecutionException e) { // NOPMD
                    // all good
                }
            }
        }).start();
        Thread.sleep(500);
        future.cancel(true);
        latch.await();
    }

    @Test
    public void testInitAfterCancel() throws ExecutionException, InterruptedException {
        final SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();
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
        final SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();
        future.get(1, TimeUnit.MILLISECONDS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullTimeUnit() throws InterruptedException, ExecutionException, TimeoutException {
        final SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();
        future.get(1, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullCallback() throws InterruptedException {
        final SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();
        future.register(null);
    }

    @Test
    public void testMultipleCallbacks() throws InterruptedException {
        final SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();

        future.register(new SingleResultCallback<Integer>() {
            @Override
            public void onResult(final Integer result, final MongoException e) {
            }
        });

        try {
            future.register(new SingleResultCallback<Integer>() {
                @Override
                public void onResult(final Integer result, final MongoException e) {
                }
            });
            fail();
        } catch (IllegalStateException e) { // NOPMD
            // all good
        }
    }


    @Test
    public void testCallbackRegisteredAfterInit() throws InterruptedException {
        final SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();
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
        final SingleResultFuture<Integer> future = new SingleResultFuture<Integer>();
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
}
