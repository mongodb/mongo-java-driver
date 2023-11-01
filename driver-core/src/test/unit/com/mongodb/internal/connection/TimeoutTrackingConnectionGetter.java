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

import com.mongodb.MongoTimeoutException;
import com.mongodb.internal.TimeoutSettings;

import java.util.concurrent.CountDownLatch;

import static com.mongodb.ClusterFixture.createOperationContext;

class TimeoutTrackingConnectionGetter implements Runnable {
    private final ConnectionPool connectionPool;
    private final TimeoutSettings timeoutSettings;
    private final CountDownLatch latch = new CountDownLatch(1);

    private volatile boolean gotTimeout;

    TimeoutTrackingConnectionGetter(final ConnectionPool connectionPool, final TimeoutSettings timeoutSettings) {
        this.connectionPool = connectionPool;
        this.timeoutSettings = timeoutSettings;
    }

    boolean isGotTimeout() {
        return gotTimeout;
    }

    @Override
    public void run() {
        try {
            InternalConnection connection = connectionPool.get(createOperationContext(timeoutSettings));
            connection.close();
        } catch (MongoTimeoutException e) {
            gotTimeout = true;
        } finally {
            latch.countDown();
        }
    }

    CountDownLatch getLatch() {
        return latch;
    }
}
