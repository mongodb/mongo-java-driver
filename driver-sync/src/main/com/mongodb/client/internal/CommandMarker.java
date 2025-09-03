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

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.internal.crypt.capi.MongoCrypt;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;
import org.bson.RawBsonDocument;

import java.io.Closeable;
import java.util.Map;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.client.internal.TimeoutHelper.databaseWithTimeout;
import static com.mongodb.internal.capi.MongoCryptHelper.createMongocryptdClientSettings;
import static com.mongodb.internal.capi.MongoCryptHelper.createProcessBuilder;
import static com.mongodb.internal.capi.MongoCryptHelper.isMongocryptdSpawningDisabled;
import static com.mongodb.internal.capi.MongoCryptHelper.startProcess;

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

    RawBsonDocument mark(final String databaseName, final RawBsonDocument command, @Nullable final Timeout operationTimeout) {
        if (client != null) {
            try {
                try {
                    return executeCommand(databaseName, command, operationTimeout);
                } catch (MongoOperationTimeoutException e){
                    throw e;
                } catch (MongoTimeoutException e) {
                    if (processBuilder == null) {  // mongocryptdBypassSpawn=true
                        throw e;
                    }
                    startProcess(processBuilder);
                    return executeCommand(databaseName, command, operationTimeout);
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

    private RawBsonDocument executeCommand(final String databaseName, final RawBsonDocument markableCommand, @Nullable final Timeout operationTimeout) {
        assertNotNull(client);

        MongoDatabase mongoDatabase = client.getDatabase(databaseName)
                .withReadConcern(ReadConcern.DEFAULT)
                .withReadPreference(ReadPreference.primary());

        return databaseWithTimeout(mongoDatabase, TIMEOUT_ERROR_MESSAGE, operationTimeout)
                .runCommand(markableCommand, RawBsonDocument.class);
    }

    private MongoClientException wrapInClientException(final MongoException e) {
        return new MongoClientException("Exception in encryption library: " + e.getMessage(), e);
    }
}
