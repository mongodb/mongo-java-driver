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

package com.mongodb.connection;

import com.mongodb.MongoInterruptedException;
import com.mongodb.ServerAddress;
import com.mongodb.internal.connection.ConcurrentPool;
import com.mongodb.selector.ServerAddressSelector;

import static com.mongodb.ClusterFixture.getAsyncCluster;
import static com.mongodb.ClusterFixture.getCluster;
import static java.lang.Thread.sleep;

public final class ServerHelper {
    public static void checkPool(final ServerAddress address) {
        checkPool(address, getCluster());
        checkPool(address, getAsyncCluster());
    }

    public static void waitForLastCheckin(final ServerAddress address, final Cluster cluster) {
        DefaultServer server = (DefaultServer) cluster.selectServer(new ServerAddressSelector(address));
        DefaultConnectionPool connectionProvider = (DefaultConnectionPool) server.getConnectionPool();
        ConcurrentPool<UsageTrackingInternalConnection> pool = connectionProvider.getPool();
        while (pool.getInUseCount() > 0) {
            try {
                sleep(10);
            } catch (InterruptedException e) {
                throw new MongoInterruptedException("Interrupted", e);
            }
        }
    }

    private static void checkPool(final ServerAddress address, final Cluster cluster) {
        DefaultServer server = (DefaultServer) cluster.selectServer(new ServerAddressSelector(address));
        DefaultConnectionPool connectionProvider = (DefaultConnectionPool) server.getConnectionPool();
        ConcurrentPool<UsageTrackingInternalConnection> pool = connectionProvider.getPool();
        if (pool.getInUseCount() > 0) {
            throw new IllegalStateException("Connection pool in use count is " + pool.getInUseCount());
        }
    }


    private ServerHelper() {
    }
}
