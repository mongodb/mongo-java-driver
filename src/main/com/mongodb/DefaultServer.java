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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ServerConnectionState.Connecting;
import static com.mongodb.ServerConnectionState.Unconnected;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.bson.util.Assertions.isTrue;
import static org.bson.util.Assertions.notNull;

class DefaultServer implements ClusterableServer {

    private final String clusterId;

    private enum HeartbeatFrequency {
        NORMAL {
            @Override
            long getFrequencyMS(final ServerSettings settings) {
                return settings.getHeartbeatFrequency(MILLISECONDS);
            }
        },

        RETRY {
            @Override
            long getFrequencyMS(final ServerSettings settings) {
                return settings.getHeartbeatConnectRetryFrequency(MILLISECONDS);
            }
        };

        abstract long getFrequencyMS(final ServerSettings settings);
    }

    private final ScheduledExecutorService scheduledExecutorService;
    private final ServerAddress serverAddress;
    private final ServerStateNotifier stateNotifier;
    private final PooledConnectionProvider connectionProvider;
    private final Map<ChangeListener<ServerDescription>, Boolean> changeListeners =
    new ConcurrentHashMap<ChangeListener<ServerDescription>, Boolean>();
    private final ServerSettings settings;
    private final ChangeListener<ServerDescription> serverStateListener;
    private volatile ServerDescription description;
    private volatile boolean isClosed;

    private ScheduledFuture<?> scheduledFuture;
    private HeartbeatFrequency currentFrequency;

    public DefaultServer(final ServerAddress serverAddress,
                         final ServerSettings settings,
                         final String clusterId, final PooledConnectionProvider connectionProvider,
                         final Mongo mongo) {
        this.clusterId = notNull("clusterId", clusterId);
        this.settings = notNull("settings", settings);
        this.serverAddress = notNull("serverAddress", serverAddress);
        this.description = ServerDescription.builder().state(Connecting).address(serverAddress).build();
        serverStateListener = new DefaultServerStateListener();
        this.stateNotifier = new ServerStateNotifier(serverAddress, serverStateListener,
                                                     settings.getHeartbeatSocketSettings(), mongo);
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory());
        setHeartbeat(0, HeartbeatFrequency.NORMAL);
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
        setHeartbeat(0, HeartbeatFrequency.RETRY);
        connectionProvider.invalidate();
    }

    @Override
    public void close() {
        if (!isClosed()) {
            scheduledFuture.cancel(true);
            scheduledExecutorService.shutdownNow();
            stateNotifier.close();
            connectionProvider.close();
            isClosed = true;
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    private void setHeartbeat(final ChangeEvent<ServerDescription> event) {
        HeartbeatFrequency heartbeatFrequency = event.getNewValue().getState() == Unconnected
                                                ? HeartbeatFrequency.RETRY
                                                : HeartbeatFrequency.NORMAL;
        long initialDelay = heartbeatFrequency.getFrequencyMS(settings);
        setHeartbeat(initialDelay, heartbeatFrequency);
    }

    private synchronized void setHeartbeat(final long initialDelay, final HeartbeatFrequency newFrequency) {
        if (currentFrequency != newFrequency) {
            currentFrequency = newFrequency;
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
            }
            scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(stateNotifier, initialDelay,
                                                                           newFrequency.getFrequencyMS(settings),
                                                                           MILLISECONDS);
        }
    }

    // Custom thread factory for scheduled executor service that creates daemon threads.  Otherwise,
    // applications that neglect to close the MongoClient will not exit.
    class DefaultThreadFactory implements ThreadFactory {
        public Thread newThread(final Runnable runnable) {
            Thread t = new Thread(runnable, "cluster-" + clusterId + "-" + serverAddress);
            t.setDaemon(true);
            return t;
        }
    }

    private final class DefaultServerStateListener implements ChangeListener<ServerDescription> {
        @Override
        public void stateChanged(final ChangeEvent<ServerDescription> event) {
            description = event.getNewValue();
            for (ChangeListener<ServerDescription> listener : changeListeners.keySet()) {
                listener.stateChanged(event);
            }
            setHeartbeat(event);
        }
    }
}
