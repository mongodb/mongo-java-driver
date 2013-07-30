package org.mongodb.connection.impl;

import org.mongodb.connection.ConnectionProvider;
import org.mongodb.connection.MongoTimeoutException;

import java.util.concurrent.CountDownLatch;

class TimeoutTrackingConnectionGetter implements Runnable {
    private final ConnectionProvider connectionProvider;
    private final CountDownLatch latch = new CountDownLatch(1);

    private volatile boolean gotTimeout;

    TimeoutTrackingConnectionGetter(final ConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
    }

    boolean isGotTimeout() {
        return gotTimeout;
    }

    @Override
    public void run() {
        try {
            connectionProvider.get();
        } catch (MongoTimeoutException e) {
            gotTimeout = true;
        } finally {
            latch.countDown();
        }
    }

}
