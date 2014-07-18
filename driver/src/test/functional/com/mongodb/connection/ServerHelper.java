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

import com.mongodb.ServerAddress;
import com.mongodb.selector.ServerAddressSelector;

import java.util.concurrent.TimeUnit;

import static org.mongodb.Fixture.getCluster;

public final class ServerHelper {
    public static void checkPool(final ServerAddress address) {
        DefaultServer server = (DefaultServer) getCluster().selectServer(new ServerAddressSelector(address), 1, TimeUnit.SECONDS);
        DefaultConnectionPool connectionProvider = (DefaultConnectionPool) server.getConnectionPool();
        ConcurrentPool<UsageTrackingInternalConnection> pool = connectionProvider.getPool();
        if (pool.getInUseCount() > 0) {
            throw new IllegalStateException("Connection pool in use count is " + pool.getInUseCount());
        }
    }

    private ServerHelper() {
    }
}
