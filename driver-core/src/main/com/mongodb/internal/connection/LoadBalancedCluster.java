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

import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterOpeningEvent;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.selector.ServerSelector;
import org.bson.BsonTimestamp;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static com.mongodb.internal.event.EventListenerHelper.createServerListener;
import static com.mongodb.internal.event.EventListenerHelper.getClusterListener;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class LoadBalancedCluster implements Cluster {
    private static final Logger LOGGER = Loggers.getLogger("cluster");

    private final ClusterId clusterId;
    private final ClusterSettings settings;
    private final ClusterClock clusterClock = new ClusterClock();
    private final ClusterListener clusterListener;
    private volatile ClusterDescription description;
    private volatile ClusterableServer server;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final DnsSrvRecordMonitor dnsSrvRecordMonitor;
    private volatile MongoException srvResolutionException;
    private volatile boolean srvRecordResolvedToMultipleHosts;
    private final CountDownLatch latch = new CountDownLatch(1);

    public LoadBalancedCluster(final ClusterId clusterId, final ClusterSettings settings, final ClusterableServerFactory serverFactory,
                               final DnsSrvRecordMonitorFactory dnsSrvRecordMonitorFactory) {
        LOGGER.info(format("Cluster created with settings %s", settings.getShortDescription()));

        this.clusterId = clusterId;
        this.settings = settings;
        this.clusterListener = getClusterListener(settings);
        this.description = new ClusterDescription(ClusterConnectionMode.LOAD_BALANCED, ClusterType.LOAD_BALANCED, emptyList(), settings,
                serverFactory.getSettings());

        if (settings.getSrvHost() == null) {
            dnsSrvRecordMonitor = null;
            assertTrue(settings.getHosts().size() == 1);
            init(clusterId, serverFactory, settings.getHosts().get(0));
            latch.countDown();
        } else {
            notNull("dnsSrvRecordMonitorFactory", dnsSrvRecordMonitorFactory);
            dnsSrvRecordMonitor = dnsSrvRecordMonitorFactory.create(settings.getSrvHost(), new DnsSrvRecordInitializer() {
                private volatile boolean initialized;

                @Override
                public void initialize(final Collection<ServerAddress> hosts) {
                    if (hosts.size() != 1) {
                        srvRecordResolvedToMultipleHosts = true;
                    } else {
                        init(clusterId, serverFactory, hosts.iterator().next());
                    }
                    srvResolutionException = null;
                    initialized = true;
                    latch.countDown();
                }

                @Override
                public void initialize(final MongoException initializationException) {
                    srvResolutionException = initializationException;
                }

                @Override
                public ClusterType getClusterType() {
                    return initialized ? ClusterType.LOAD_BALANCED : ClusterType.UNKNOWN;
                }
            });
            dnsSrvRecordMonitor.start();
        }
    }

    private void init(final ClusterId clusterId, final ClusterableServerFactory serverFactory, final ServerAddress host) {
        description = new ClusterDescription(ClusterConnectionMode.LOAD_BALANCED, ClusterType.LOAD_BALANCED,
                singletonList(ServerDescription.builder()
                        .ok(true)
                        .state(ServerConnectionState.CONNECTED)
                        .type(ServerType.LOAD_BALANCER)
                        .address(host)
                        .build()),
                settings, serverFactory.getSettings());
        server = serverFactory.create(settings.getHosts().get(0), event -> { }, createServerListener(serverFactory.getSettings()),
                clusterClock);

        clusterListener.clusterOpening(new ClusterOpeningEvent(clusterId));

        ClusterDescription startingDescription = new ClusterDescription(settings.getMode(), ClusterType.UNKNOWN, Collections.emptyList(),
                settings, serverFactory.getSettings());
        ClusterDescription initialDescription = new ClusterDescription(settings.getMode(), ClusterType.LOAD_BALANCED,
                singletonList(ServerDescription.builder().address(settings.getHosts().get(0)).state(CONNECTING).build()),
                settings, serverFactory.getSettings());
        clusterListener.clusterDescriptionChanged(new ClusterDescriptionChangedEvent(clusterId, initialDescription, startingDescription));
        clusterListener.clusterDescriptionChanged(new ClusterDescriptionChangedEvent(clusterId, description, initialDescription));
    }

    @Override
    public ClusterSettings getSettings() {
        isTrue("open", !isClosed());
        return settings;
    }

    @Override
    public ClusterDescription getDescription() {
        isTrue("open", !isClosed());
        waitForSrv();
        return description;
    }

    @Override
    public ClusterId getClusterId() {
        return clusterId;
    }

    @Override
    public ClusterableServer getServer(final ServerAddress serverAddress) {
        waitForSrv();
        return server;
    }

    @Override
    public ClusterDescription getCurrentDescription() {
        isTrue("open", !isClosed());
        return description;
    }

    @Override
    public BsonTimestamp getClusterTime() {
        isTrue("open", !isClosed());
        return clusterClock.getClusterTime();
    }

    @Override
    public ServerTuple selectServer(final ServerSelector serverSelector) {
        isTrue("open", !isClosed());
        waitForSrv();
        if (srvRecordResolvedToMultipleHosts) {
            throw new MongoClientException("In load balancing mode, the host must resolve to a single SRV record, but instead it resolved "
                    + "to multiple hosts");
        }
        return new ServerTuple(server, description.getServerDescriptions().get(0));
    }

    private void waitForSrv() {
        try {
            if (!latch.await(settings.getServerSelectionTimeout(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)) {
                MongoException localSrvResolutionException = srvResolutionException;
                if (localSrvResolutionException == null) {
                    throw new MongoTimeoutException(format("Timed out after %d ms while waiting to resolve SRV records for %s.",
                            settings.getServerSelectionTimeout(MILLISECONDS), settings.getSrvHost()));
                } else {
                    throw new MongoTimeoutException(format("Timed out after %d ms while waiting to resolve SRV records for %s. "
                                    + "Resolution exception was '%s'",
                            settings.getServerSelectionTimeout(MILLISECONDS), settings.getSrvHost(), localSrvResolutionException));
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MongoInterruptedException(format("Interrupted while resolving SRV records for %s", settings.getSrvHost()), e);
        }
    }

    @Override
    public void selectServerAsync(final ServerSelector serverSelector, final SingleResultCallback<ServerTuple> callback) {
        isTrue("open", !isClosed());
        // TODO: wait on SRV
        callback.onResult(selectServer(serverSelector), null);
    }

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
            clusterListener.clusterClosed(new ClusterClosedEvent(clusterId));
            if (dnsSrvRecordMonitor != null) {
                dnsSrvRecordMonitor.close();
            }
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }
}
