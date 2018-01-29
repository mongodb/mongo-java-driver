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

package com.mongodb.connection;

import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoInterruptedException;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

class FutureAsyncCompletionHandler<T> implements AsyncCompletionHandler<T> {
    private final CountDownLatch latch = new CountDownLatch(1);;
    private volatile T result;
    private volatile Throwable error;

    @Override
    public void completed(final T result) {
        this.result = result;
        latch.countDown();
    }

    @Override
    public void failed(final Throwable t) {
        this.error = t;
        latch.countDown();
    }

    public void getOpen() throws IOException {
        get("Opening");
    }

    public void getWrite() throws IOException {
        get("Writing to");
    }

    public T getRead() throws IOException {
        return get("Reading from");
    }

    private T get(final String prefix) throws IOException {
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new MongoInterruptedException(prefix + " the AsynchronousSocketChannelStream failed", e);

        }
        if (error != null) {
            if (error instanceof IOException) {
                throw (IOException) error;
            } else if (error instanceof MongoException) {
                throw (MongoException) error;
            } else {
                throw new MongoInternalException(prefix + " the AsynchronousSocketChannelStream failed", error);
            }
        }
        return result;
    }

}
