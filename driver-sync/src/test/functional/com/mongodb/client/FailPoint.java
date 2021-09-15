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
package com.mongodb.client;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.ServerApi;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ConnectionPoolSettings;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.conversions.Bson;

import java.util.Collections;

public final class FailPoint implements AutoCloseable {
    private final BsonDocument failPointDocument;
    private final MongoClient client;
    private final boolean close;

    private FailPoint(final BsonDocument failPointDocument, final MongoClient client, final boolean close) {
        this.failPointDocument = failPointDocument.toBsonDocument();
        this.client = client;
        this.close = close;
    }

    /**
     * @param configureFailPointDoc A document representing {@code configureFailPoint} command to be issued as is via
     * {@link com.mongodb.client.MongoDatabase#runCommand(Bson)}.
     */
    public static FailPoint enable(final BsonDocument configureFailPointDoc, final MongoClientSettings clientSettingsTemplate,
            final ServerAddress serverAddress) {
        MongoClientSettings.Builder clientSettingsBuilder = MongoClientSettings.builder()
                .applyToClusterSettings(builder -> builder
                        .applySettings(ClusterSettings
                                .builder()
                                .mode(ClusterConnectionMode.SINGLE)
                                .hosts(Collections.singletonList(serverAddress))
                                .build()))
                .applyToConnectionPoolSettings(builder -> builder
                        .applySettings(ConnectionPoolSettings.builder()
                                .maxSize(1)
                                .build()))
                .applyToSslSettings(builder -> builder
                        .enabled(clientSettingsTemplate.getSslSettings().isEnabled())
                        .invalidHostNameAllowed(clientSettingsTemplate.getSslSettings().isInvalidHostNameAllowed())
                        .context(clientSettingsTemplate.getSslSettings().getContext()));
        MongoCredential cred = clientSettingsTemplate.getCredential();
        if (cred != null) {
            clientSettingsBuilder.credential(cred);
        }
        ServerApi srvApi = clientSettingsTemplate.getServerApi();
        if (srvApi != null) {
            clientSettingsBuilder.serverApi(srvApi);
        }
        ClusterType clusterType = clientSettingsTemplate.getClusterSettings().getRequiredClusterType();
        if (clusterType != null) {
            clientSettingsBuilder.applyToClusterSettings(builder -> builder.requiredClusterType(clusterType));
        }
        String rsName = clientSettingsTemplate.getClusterSettings().getRequiredReplicaSetName();
        if (rsName != null) {
            clientSettingsBuilder.applyToClusterSettings(builder -> builder.requiredReplicaSetName(rsName));
        }
        MongoClient client = MongoClients.create(clientSettingsBuilder.build());
        return enable(configureFailPointDoc, client, true);
    }

    /**
     * @see #enable(BsonDocument, MongoClientSettings, ServerAddress)
     */
    public static FailPoint enable(final BsonDocument configureFailPointDoc, final MongoClient client) {
        return enable(configureFailPointDoc, client, false);
    }

    private static FailPoint enable(final BsonDocument configureFailPointDoc, final MongoClient client, final boolean close) {
        FailPoint result = new FailPoint(configureFailPointDoc, client, close);
        client.getDatabase("admin").runCommand(configureFailPointDoc);
        return result;
    }

    @Override
    public void close() {
        try {
            client.getDatabase("admin").runCommand(new BsonDocument()
                    .append("configureFailPoint", failPointDocument.getString("configureFailPoint"))
                    .append("mode", new BsonString("off")));
        } finally {
            if (close) {
                client.close();
            }
        }
    }
}
