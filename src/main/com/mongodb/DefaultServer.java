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

package com.mongodb;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ServerConnectionState.Connecting;
import static com.mongodb.ServerConnectionState.Unconnected;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.bson.util.Assertions.isTrue;
import static org.bson.util.Assertions.notNull;

class DefaultServer implements ClusterableServer {
    private final ScheduledExecutorService scheduledExecutorService;
    private final ServerAddress serverAddress;
    private final ServerStateNotifier stateNotifier;
    private final ScheduledFuture<?> scheduledFuture;
    private final PooledConnectionProvider connectionProvider;
    private final Map<ChangeListener<ServerDescription>, Boolean> changeListeners =
    new ConcurrentHashMap<ChangeListener<ServerDescription>, Boolean>();
    private final ServerSettings settings;
    private final ChangeListener<ServerDescription> serverStateListener;
    private volatile ServerDescription description;
    private volatile boolean isClosed;

    public DefaultServer(final ServerAddress serverAddress,
                         final ServerSettings settings,
                         final PooledConnectionProvider connectionProvider,
                         final ScheduledExecutorService scheduledExecutorService,
                         Mongo mongo) {
        this.settings = notNull("settings", settings);

        this.scheduledExecutorService = notNull("scheduledExecutorService", scheduledExecutorService);
        this.serverAddress = notNull("serverAddress", serverAddress);
        this.description = ServerDescription.builder().state(Connecting).address(serverAddress).build();
        serverStateListener = new DefaultServerStateListener();
        this.stateNotifier = new ServerStateNotifier(serverAddress, serverStateListener,
                                                     settings.getHeartbeatSocketSettings(), mongo);
        this.scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(stateNotifier, 0,
                                                                            settings.getHeartbeatFrequency(MILLISECONDS),
                                                                            MILLISECONDS);
        this.connectionProvider = connectionProvider;
    }


    @Override
    public ServerDescription getDescription() {
        isTrue("open", !isClosed());

        return description;
    }

    @Override
    public Connection getConnection(final long maxWaitTime, final TimeUnit timeUnit) {
        return connectionProvider.get(maxWaitTime, timeUnit);
    }

    @Override
    public void addChangeListener(final ChangeListener<ServerDescription> changeListener) {
        isTrue("open", !isClosed());

        changeListeners.put(changeListener, true);
    }

    @Override
    public void invalidate() {
        isTrue("open", !isClosed());

        serverStateListener.stateChanged(new ChangeEvent<ServerDescription>(description, ServerDescription.builder()
                                                                                                          .state(Connecting)
                                                                                                          .address(serverAddress).build()));
        scheduledExecutorService.submit(stateNotifier);
        connectionProvider.invalidate();
    }

    @Override
    public void close() {
        if (!isClosed()) {
            scheduledFuture.cancel(true);
            stateNotifier.close();
            connectionProvider.close();
            isClosed = true;
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    private final class DefaultServerStateListener implements ChangeListener<ServerDescription> {
        @Override
        public void stateChanged(final ChangeEvent<ServerDescription> event) {
            description = event.getNewValue();
            for (ChangeListener<ServerDescription> listener : changeListeners.keySet()) {
                listener.stateChanged(event);
            }
            if (event.getNewValue().getState() == Unconnected) {
                scheduledExecutorService.schedule(stateNotifier, settings.getHeartbeatConnectRetryFrequency(MILLISECONDS),
                                                  MILLISECONDS);
            }
        }

    }
}
