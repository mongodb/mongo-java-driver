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

import com.mongodb.ClusterFixture;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.selector.ServerAddressSelector;

import static com.mongodb.ClusterFixture.getAsyncCluster;
import static com.mongodb.ClusterFixture.getCluster;
import static java.lang.Thread.sleep;

public final class ServerHelper {
    public static void checkPool(final ServerAddress address) {
        checkPool(address, getCluster());
        checkPool(address, getAsyncCluster());
    }

    public static void waitForLastRelease(final Cluster cluster) {
        for (ServerDescription cur : cluster.getCurrentDescription().getServerDescriptions()) {
            if (cur.isOk()) {
                waitForLastRelease(cur.getAddress(), cluster);
            }
        }
    }

    public static void waitForLastRelease(final ServerAddress address, final Cluster cluster) {
        DefaultServer server = (DefaultServer) cluster.selectServer(new ServerAddressSelector(address))
                .getServer();
        DefaultConnectionPool connectionProvider = (DefaultConnectionPool) server.getConnectionPool();
        ConcurrentPool<UsageTrackingInternalConnection> pool = connectionProvider.getPool();
        long startTime = System.currentTimeMillis();
        while (pool.getInUseCount() > 0) {
            try {
                sleep(10);
                if (System.currentTimeMillis() > startTime + ClusterFixture.TIMEOUT * 1000) {
                    throw new MongoTimeoutException("Timed out waiting for pool in use count to drop to 0.  Now at: "
                                                            + pool.getInUseCount());
                }
            } catch (InterruptedException e) {
                throw new MongoInterruptedException("Interrupted", e);
            }
        }
    }

    private static void checkPool(final ServerAddress address, final Cluster cluster) {
        DefaultServer server = (DefaultServer) cluster.selectServer(new ServerAddressSelector(address))
                .getServer();
        DefaultConnectionPool connectionProvider = (DefaultConnectionPool) server.getConnectionPool();
        ConcurrentPool<UsageTrackingInternalConnection> pool = connectionProvider.getPool();
        if (pool.getInUseCount() > 0) {
            throw new IllegalStateException("Connection pool in use count is " + pool.getInUseCount());
        }
    }

    public static void waitForRelease(final AsyncConnectionSource connectionSource, final int expectedCount) {
        long startTime = System.currentTimeMillis();
        while (connectionSource.getCount() > expectedCount) {
            try {
                sleep(10);
                if (System.currentTimeMillis() > startTime + ClusterFixture.TIMEOUT * 1000) {
                    throw new MongoTimeoutException("Timed out waiting for ConnectionSource count to drop to " + expectedCount);
                }
            } catch (InterruptedException e) {
                throw new MongoInterruptedException("Interrupted", e);
            }
        }
    }

    private ServerHelper() {
    }
}
