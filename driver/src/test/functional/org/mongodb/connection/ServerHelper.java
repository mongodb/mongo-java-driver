package org.mongodb.connection;

import org.mongodb.selector.ServerAddressSelector;

import java.util.concurrent.TimeUnit;

import static org.mongodb.Fixture.getCluster;

public final class ServerHelper {
    public static void checkPool(final ServerAddress address) throws InterruptedException {
        DefaultServer server = (DefaultServer) getCluster().selectServer(new ServerAddressSelector(address), 1, TimeUnit.SECONDS);
        PooledConnectionProvider connectionProvider = (PooledConnectionProvider) server.getConnectionProvider();
        ConcurrentPool<UsageTrackingInternalConnection> pool = connectionProvider.getPool();
        if (pool.getInUseCount() > 0) {
            throw new IllegalStateException("Connection pool in use count is " + pool.getInUseCount());
        }
    }

    private ServerHelper() {
    }
}
