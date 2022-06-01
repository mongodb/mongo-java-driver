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

package com.mongodb.reactivestreams.client.internal.crypt;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.crypt.capi.MongoCrypt;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import org.bson.RawBsonDocument;
import reactor.core.publisher.Mono;

import java.io.Closeable;
import java.util.Map;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.internal.capi.MongoCryptHelper.createMongocryptdClientSettings;
import static com.mongodb.internal.capi.MongoCryptHelper.createProcessBuilder;
import static com.mongodb.internal.capi.MongoCryptHelper.startProcess;

@SuppressWarnings("UseOfProcessBuilder")
class CommandMarker implements Closeable {
    @Nullable
    private final MongoClient client;
    @Nullable
    private final ProcessBuilder processBuilder;

    /**
     * The command marker
     *
     * <p>
     * If the extraOptions.cryptSharedLibRequired option is true then the driver MUST NOT attempt to spawn or connect to mongocryptd.
     *
     * If the following conditions are met:
     * <ul>
     *  <li>The user's MongoClient is configured for client-side encryption (i.e. bypassAutoEncryption is not false)</li>
     *  <li>The user has not disabled mongocryptd spawning (i.e. by setting extraOptions.mongocryptdBypassSpawn to true)</li>
     *  <li>The crypt shared library is unavailable.</li>
     *  <li>The extraOptions.cryptSharedLibRequired option is false.</li>
     * </ul>
     *  Then mongocryptd MUST be spawned by the driver.
     * </p>
     */
    CommandMarker(
            final MongoCrypt mongoCrypt,
            final AutoEncryptionSettings settings) {
        Map<String, Object> extraOptions = settings.getExtraOptions();
        String cryptSharedLibVersionString = mongoCrypt.getCryptSharedLibVersionString();

        boolean bypassAutoEncryption = settings.isBypassAutoEncryption();
        boolean isBypassQueryAnalysis = settings.isBypassQueryAnalysis();
        boolean cryptSharedIsAvailable = cryptSharedLibVersionString != null && cryptSharedLibVersionString.isEmpty();
        boolean cryptSharedLibRequired = (boolean) extraOptions.getOrDefault("cryptSharedLibRequired", false);

        if (bypassAutoEncryption || isBypassQueryAnalysis || cryptSharedLibRequired || cryptSharedIsAvailable) {
            processBuilder = null;
            client = null;
        } else {
            boolean mongocryptdBypassSpawn = (boolean) extraOptions.getOrDefault("mongocryptdBypassSpawn", false);
            if (!mongocryptdBypassSpawn) {
                processBuilder = createProcessBuilder(extraOptions);
                startProcess(processBuilder);
            } else {
                processBuilder = null;
            }
            client = MongoClients.create(createMongocryptdClientSettings((String) extraOptions.get("mongocryptdURI")));
        }
    }

    Mono<RawBsonDocument> mark(final String databaseName, final RawBsonDocument command) {
        if (client != null) {
            return runCommand(databaseName, command)
                    .onErrorResume(Throwable.class, e -> {
                        if (processBuilder == null) {
                            throw MongoException.fromThrowable(e);
                        }
                        return Mono.fromRunnable(() -> startProcess(processBuilder)).then(runCommand(databaseName, command));
                    })
                    .onErrorMap(t -> new MongoClientException("Exception in encryption library: " + t.getMessage(), t));
        } else {
            return Mono.fromCallable(() -> command);
        }
    }

    private Mono<RawBsonDocument> runCommand(final String databaseName, final RawBsonDocument command) {
        assertNotNull(client);
        return Mono.from(client.getDatabase(databaseName)
                                 .withReadConcern(ReadConcern.DEFAULT)
                                 .withReadPreference(ReadPreference.primary())
                                 .runCommand(command, RawBsonDocument.class));
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }

}
