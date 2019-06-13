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

import com.mongodb.Block;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.connection.ClusterSettings;
import org.bson.RawBsonDocument;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("UseOfProcessBuilder")
class CommandMarker implements Closeable {
    private MongoClient client;
    private final ProcessBuilder processBuilder;
    private boolean active;

    CommandMarker(final Map<String, Object> options) {
        String connectionString;

        if (options.containsKey("mongocryptdURI")) {
            connectionString = (String) options.get("mongocryptdURI");
        } else {
            connectionString = "mongodb://localhost:27020";
        }

        this.client = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .applyToClusterSettings(new Block<ClusterSettings.Builder>() {
                    @Override
                    public void apply(final ClusterSettings.Builder builder) {
                        builder.serverSelectionTimeout(1, TimeUnit.SECONDS);
                    }
                })
                .build());
        this.active = false;

        if (!options.containsKey("mongocryptdBypassSpawn") || !((Boolean) options.get("mongocryptdBypassSpawn"))) {
            List<String> spawnArgs = createSpawnArgs(options);
            processBuilder = new ProcessBuilder(spawnArgs);

        } else {
            processBuilder = null;
        }
    }

    RawBsonDocument mark(final String databaseName, final RawBsonDocument command) {
        spawnIfNecesary();

        try {
            try {
                return executeCommand(databaseName, command);
            } catch (MongoTimeoutException e) {
                spawnIfNecesary();
                return executeCommand(databaseName, command);
            }
        } catch (MongoException e) {
            throw wrapInClientException(e);
        }
    }

    @Override
    public void close() {
        client.close();
    }

    @SuppressWarnings("unchecked")
    private List<String> createSpawnArgs(final Map<String, Object> options) {
        List<String> spawnArgs = new ArrayList<String>();

        String path = options.containsKey("mongocryptdPath")
                ? (String) options.get("mongocryptdPath")
                : "mongocryptd";

        spawnArgs.add(path);
        if (options.containsKey("mongocryptdSpawnArgs")) {
            spawnArgs.addAll((List<String>) options.get("mongocryptdSpawnArgs"));
        }

        if (!spawnArgs.contains("--idleShutdownTimeoutSecs")) {
            spawnArgs.add("--idleShutdownTimeoutSecs");
            spawnArgs.add("60");
        }
        return spawnArgs;
    }

    private RawBsonDocument executeCommand(final String databaseName, final RawBsonDocument markablecommand) {
        return client.getDatabase(databaseName)
                .withReadConcern(ReadConcern.DEFAULT)
                .withReadPreference(ReadPreference.primary())
                .runCommand(markablecommand, RawBsonDocument.class);
    }

    private synchronized void spawnIfNecesary() {
        try {
            if (processBuilder != null) {
                synchronized (this) {
                    if (!active) {
                        processBuilder.start();
                        active = true;
                    }
                }
            }
        } catch (IOException e) {
            throw new MongoClientException("Exception starting mongocryptd process", e);
        }
    }

    private MongoClientException wrapInClientException(final MongoException e) {
        return new MongoClientException("Exception in encryption library: " + e.getMessage(), e);
    }

}
