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

import com.mongodb.MongoClientException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.internal.async.SingleResultCallback;
import org.bson.RawBsonDocument;

import java.io.Closeable;
import java.util.Map;

import static com.mongodb.internal.capi.MongoCryptHelper.createMongocryptdClientSettings;
import static com.mongodb.internal.capi.MongoCryptHelper.createProcessBuilder;
import static com.mongodb.internal.capi.MongoCryptHelper.startProcess;

@SuppressWarnings("UseOfProcessBuilder")
class CommandMarker implements Closeable {
    private AsyncMongoClient client;
    private final ProcessBuilder processBuilder;

    CommandMarker(final boolean isBypassAutoEncryption, final Map<String, Object> options) {

        if (isBypassAutoEncryption) {
            processBuilder = null;
            client = null;
        } else {
            if (!options.containsKey("mongocryptdBypassSpawn") || !((Boolean) options.get("mongocryptdBypassSpawn"))) {
                processBuilder = createProcessBuilder(options);
                startProcess(processBuilder);
            } else {
                processBuilder = null;
            }
            client = AsyncMongoClients.create(createMongocryptdClientSettings((String) options.get("mongocryptdURI")));
        }
    }

    void mark(final String databaseName, final RawBsonDocument command, final SingleResultCallback<RawBsonDocument> callback) {
        if (client != null) {
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
        } else {
            callback.onResult(command, null);
        }
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
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
            startProcess(processBuilder);
            callback.onResult(null, null);
        } catch (Throwable t) {
            callback.onResult(null, t);
        }
    }
}
