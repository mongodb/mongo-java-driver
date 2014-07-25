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

package com.mongodb.client;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.DefaultClusterFactory;
import com.mongodb.connection.SocketStreamFactory;
import com.mongodb.connection.StreamFactory;
import com.mongodb.management.JMXConnectionPoolListener;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@ThreadSafe
public final class MongoClients {

    public static MongoClient create(final MongoClientSettings settings) {
        return new MongoClientImpl(settings, createCluster(settings));
    }

    public static MongoClient create(final ServerAddress serverAddress) {
        return create(serverAddress, Collections.<MongoCredential>emptyList());

    }

    public static MongoClient create(final ServerAddress serverAddress, final List<MongoCredential> credentialList) {
        return create(MongoClientSettings.builder()
                                         .credentialList(credentialList)
                                         .clusterSettings(ClusterSettings.builder()
                                                                         .hosts(Arrays.asList(serverAddress)).build())
                                         .build());
    }

    private static Cluster createCluster(final MongoClientSettings settings) {
        return new DefaultClusterFactory().create(settings.getClusterSettings(), settings.getServerSettings(),
                                                  settings.getConnectionPoolSettings(), getStreamFactory(settings),
                                                  new SocketStreamFactory(settings.getHeartbeatSocketSettings(), settings.getSslSettings()),
                                                  settings.getCredentialList(), null, new JMXConnectionPoolListener(), null);
    }

    private static StreamFactory getStreamFactory(final MongoClientSettings settings) {
        return new SocketStreamFactory(settings.getSocketSettings(), settings.getSslSettings());
    }

    private MongoClients() {
    }
}