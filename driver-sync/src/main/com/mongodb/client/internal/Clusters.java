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
package com.mongodb.client.internal;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoDriverInformation;
import com.mongodb.connection.SocketSettings;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.DefaultClusterFactory;
import com.mongodb.internal.connection.InternalMongoClientSettings;
import com.mongodb.internal.connection.StreamFactory;
import com.mongodb.internal.connection.StreamFactoryFactory;
import com.mongodb.lang.Nullable;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.internal.event.EventListenerHelper.getCommandListener;

public final class Clusters {

    private Clusters() {
        //NOP
    }

    /**
     * Creates a cluster with the given settings and default internal settings.
     *
     * @param settings the client settings
     * @param mongoDriverInformation driver information
     * @param streamFactoryFactory the stream factory
     * @return the cluster
     */
    public static Cluster createCluster(final MongoClientSettings settings,
                                        @Nullable final MongoDriverInformation mongoDriverInformation,
                                        final StreamFactoryFactory streamFactoryFactory) {
        return createCluster(settings, mongoDriverInformation, streamFactoryFactory, InternalMongoClientSettings.getDefaults());
    }

    /**
     * Creates a cluster with the given settings and internal settings.
     *
     * @param settings the client settings
     * @param mongoDriverInformation driver information
     * @param streamFactoryFactory the stream factory
     * @param internalSettings the internal settings
     * @return the cluster
     */
    public static Cluster createCluster(final MongoClientSettings settings,
                                        @Nullable final MongoDriverInformation mongoDriverInformation,
                                        final StreamFactoryFactory streamFactoryFactory,
                                        final InternalMongoClientSettings internalSettings) {
        assertNotNull(streamFactoryFactory);
        assertNotNull(settings);
        assertNotNull(internalSettings);

        StreamFactory streamFactory = getStreamFactory(streamFactoryFactory, settings, false);
        StreamFactory heartbeatStreamFactory = getStreamFactory(streamFactoryFactory, settings, true);

        return new DefaultClusterFactory().createCluster(settings.getClusterSettings(), settings.getServerSettings(),
                settings.getConnectionPoolSettings(), internalSettings,
                TimeoutSettings.create(settings), streamFactory,
                TimeoutSettings.createHeartbeatSettings(settings), heartbeatStreamFactory,
                settings.getCredential(), settings.getLoggerSettings(), getCommandListener(settings.getCommandListeners()),
                settings.getApplicationName(), mongoDriverInformation, settings.getCompressorList(), settings.getServerApi(),
                settings.getDnsClient());
    }

    private static StreamFactory getStreamFactory(
            final StreamFactoryFactory streamFactoryFactory,
            final MongoClientSettings settings,
            final boolean isHeartbeat) {
        SocketSettings socketSettings = isHeartbeat ? settings.getHeartbeatSocketSettings() : settings.getSocketSettings();
        return streamFactoryFactory.create(socketSettings, settings.getSslSettings());
    }
}
