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

package com.mongodb.internal.async.client;

import com.mongodb.Block;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.internal.async.SingleResultCallback;
import org.bson.RawBsonDocument;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.mongodb.internal.capi.MongoCryptOptionsHelper.createMongocryptdSpawnArgs;

@SuppressWarnings("UseOfProcessBuilder")
class CommandMarker implements Closeable {
    private AsyncMongoClient client;
    private final ProcessBuilder processBuilder;

    CommandMarker(final Map<String, Object> options) {
        String connectionString;

        if (options.containsKey("mongocryptdURI")) {
            connectionString = (String) options.get("mongocryptdURI");
        } else {
            connectionString = "mongodb://localhost:27020";
        }

        if (!options.containsKey("mongocryptdBypassSpawn") || !((Boolean) options.get("mongocryptdBypassSpawn"))) {
            processBuilder = new ProcessBuilder(createMongocryptdSpawnArgs(options));
            startProcess();
        } else {
            processBuilder = null;
        }

        client = AsyncMongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .applyToClusterSettings(new Block<ClusterSettings.Builder>() {
                    @Override
                    public void apply(final ClusterSettings.Builder builder) {
                        builder.serverSelectionTimeout(1, TimeUnit.SECONDS);
                    }
                })
                .build());
    }

    void mark(final String databaseName, final RawBsonDocument command, final SingleResultCallback<RawBsonDocument> callback) {
        final SingleResultCallback<RawBsonDocument> wrappedCallback = new SingleResultCallback<RawBsonDocument>() {
            @Override
            public void onResult(final RawBsonDocument result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, new MongoClientException("Exception in encryption library: " + t.getMessage(), t));
                } else {
                    callback.onResult(result, null);
                }
            }
        };
        runCommand(databaseName, command, new SingleResultCallback<RawBsonDocument>() {
            @Override
            public void onResult(final RawBsonDocument result, final Throwable t) {
                if (t == null) {
                    wrappedCallback.onResult(result, null);
                } else if (t instanceof MongoTimeoutException && processBuilder != null) {
                    startProcessAndContinue(new SingleResultCallback<Void>() {
                        @Override
                        public void onResult(final Void result, final Throwable t) {
                            if (t != null) {
                                callback.onResult(null, t);
                            } else {
                                runCommand(databaseName, command, wrappedCallback);
                            }
                        }
                    });
                } else {
                    wrappedCallback.onResult(null, t);
                }
            }
        });
    }

    @Override
    public void close() {
        client.close();
    }

    private void runCommand(final String databaseName, final RawBsonDocument command,
                            final SingleResultCallback<RawBsonDocument> callback) {
        client.getDatabase(databaseName)
                .withReadConcern(ReadConcern.DEFAULT)
                .withReadPreference(ReadPreference.primary())
                .runCommand(command, RawBsonDocument.class, callback);
    }

    private void startProcessAndContinue(final SingleResultCallback<Void> callback) {
        try {
            startProcess();
            callback.onResult(null, null);
        } catch (Throwable t) {
            callback.onResult(null, t);
        }
    }

    private void startProcess() {
        try {
            processBuilder.start();
        } catch (Throwable t) {
            throw new MongoClientException("Exception starting mongocryptd process. Is `mongocryptd` on the system path?", t);
        }
    }
}
