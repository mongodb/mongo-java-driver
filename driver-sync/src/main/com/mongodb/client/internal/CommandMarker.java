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

import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.RawBsonDocument;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import static com.mongodb.internal.capi.MongoCryptHelper.createMongocryptdSpawnArgs;
import static com.mongodb.internal.capi.MongoCryptHelper.createMongocryptdClientSettings;

@SuppressWarnings("UseOfProcessBuilder")
class CommandMarker implements Closeable {
    private MongoClient client;
    private final ProcessBuilder processBuilder;

    CommandMarker(final boolean isBypassAutoEncryption, final Map<String, Object> options) {
        if (isBypassAutoEncryption) {
            processBuilder = null;
            client = null;
        } else {
            if (!options.containsKey("mongocryptdBypassSpawn") || !((Boolean) options.get("mongocryptdBypassSpawn"))) {
                processBuilder = new ProcessBuilder(createMongocryptdSpawnArgs(options));
                startProcess();
            } else {
                processBuilder = null;
            }
            client = MongoClients.create(createMongocryptdClientSettings((String) options.get("mongocryptdURI")));
        }
    }

    RawBsonDocument mark(final String databaseName, final RawBsonDocument command) {
        if (client != null) {
            try {
                try {
                    return executeCommand(databaseName, command);
                } catch (MongoTimeoutException e) {
                    if (processBuilder == null) {  // mongocryptdBypassSpawn=true
                        throw e;
                    }
                    startProcess();
                    return executeCommand(databaseName, command);
                }
            } catch (MongoException e) {
                throw wrapInClientException(e);
            }
        } else {
            return command;
        }
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }

    private RawBsonDocument executeCommand(final String databaseName, final RawBsonDocument markableCommand) {
        return client.getDatabase(databaseName)
                .withReadConcern(ReadConcern.DEFAULT)
                .withReadPreference(ReadPreference.primary())
                .runCommand(markableCommand, RawBsonDocument.class);
    }

    private void startProcess() {
        try {
            processBuilder.start();
        } catch (IOException e) {
            throw new MongoClientException("Exception starting mongocryptd process. Is `mongocryptd` on the system path?", e);
        }
    }

    private MongoClientException wrapInClientException(final MongoException e) {
        return new MongoClientException("Exception in encryption library: " + e.getMessage(), e);
    }
}
