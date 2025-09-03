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
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.internal.crypt.capi.MongoCrypt;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.bson.RawBsonDocument;
import reactor.core.publisher.Mono;

import java.io.Closeable;
import java.util.Map;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.internal.capi.MongoCryptHelper.createMongocryptdClientSettings;
import static com.mongodb.internal.capi.MongoCryptHelper.createProcessBuilder;
import static com.mongodb.internal.capi.MongoCryptHelper.isMongocryptdSpawningDisabled;
import static com.mongodb.internal.capi.MongoCryptHelper.startProcess;
import static com.mongodb.reactivestreams.client.internal.TimeoutHelper.databaseWithTimeoutDeferred;

@SuppressWarnings("UseOfProcessBuilder")
class CommandMarker implements Closeable {
    private static final String TIMEOUT_ERROR_MESSAGE = "Command marker exceeded the timeout limit.";
    @Nullable
    private final MongoClient client;
    @Nullable
    private final ProcessBuilder processBuilder;

    /**
     * The command marker
     *
     * <p>
     * If the extraOptions.cryptSharedLibRequired option is true then the driver MUST NOT attempt to spawn or connect to mongocryptd.
     * <p>
     * If the following conditions are met:
     * <ul>
     *  <li>The user's MongoClient is configured for client-side encryption (i.e. bypassAutoEncryption is not false)</li>
     *  <li>The user has not disabled mongocryptd spawning (i.e. by setting extraOptions.mongocryptdBypassSpawn to true)</li>
     *  <li>The crypt shared library is unavailable.</li>
     *  <li>The extraOptions.cryptSharedLibRequired option is false.</li>
     * </ul>
     *  Then mongocryptd MUST be spawned by the driver.
     */
    CommandMarker(
            final MongoCrypt mongoCrypt,
            final AutoEncryptionSettings settings) {

        if (isMongocryptdSpawningDisabled(mongoCrypt.getCryptSharedLibVersionString(), settings)) {
            processBuilder = null;
            client = null;
        } else {
            Map<String, Object> extraOptions = settings.getExtraOptions();
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

    Mono<RawBsonDocument> mark(final String databaseName, final RawBsonDocument command, @Nullable final Timeout operationTimeout) {
        if (client != null) {
            return runCommand(databaseName, command, operationTimeout)
                    .onErrorResume(Throwable.class, e -> {
                        if (processBuilder == null || e instanceof MongoOperationTimeoutException) {
                            throw MongoException.fromThrowable(e);
                        }
                        return Mono.fromRunnable(() -> startProcess(processBuilder)).then(runCommand(databaseName, command, operationTimeout));
                    })
                    .onErrorMap(t -> new MongoClientException("Exception in encryption library: " + t.getMessage(), t));
        } else {
            return Mono.fromCallable(() -> command);
        }
    }

    private Mono<RawBsonDocument> runCommand(final String databaseName, final RawBsonDocument command, @Nullable final Timeout operationTimeout) {
        assertNotNull(client);
        MongoDatabase mongoDatabase = client.getDatabase(databaseName)
                .withReadConcern(ReadConcern.DEFAULT)
                .withReadPreference(ReadPreference.primary());

        return databaseWithTimeoutDeferred(mongoDatabase, TIMEOUT_ERROR_MESSAGE, operationTimeout)
                .flatMap(database -> Mono.from(database.runCommand(command, RawBsonDocument.class)));
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }

}
