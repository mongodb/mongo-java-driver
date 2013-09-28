package com.mongodb;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

class DefaultClusterableServerFactory implements ClusterableServerFactory {
    private ServerSettings settings;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Mongo mongo;

    public DefaultClusterableServerFactory(final ServerSettings settings,
                                           final ScheduledExecutorService scheduledExecutorService,
                                           final Mongo mongo) {
        this.settings = settings;
        this.scheduledExecutorService = scheduledExecutorService;
        this.mongo = mongo;
    }

    @Override
    public ClusterableServer create(final ServerAddress serverAddress) {
        return new DefaultServer(serverAddress, settings, scheduledExecutorService, mongo);
    }

    @Override
    public void close() {
        scheduledExecutorService.shutdownNow();
    }
}
