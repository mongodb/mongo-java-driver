/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.unified;

import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.internal.ClientEncryptionImpl;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.lang.NonNull;
import org.junit.jupiter.params.provider.Arguments;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;

public abstract class UnifiedSyncTest extends UnifiedTest {
    protected UnifiedSyncTest() {
    }

    @Override
    protected MongoClient createMongoClient(final MongoClientSettings settings) {
        return MongoClients.create(settings);
    }

    @Override
    protected GridFSBucket createGridFSBucket(final MongoDatabase database) {
        return GridFSBuckets.create(database);
    }

    @Override
    protected ClientEncryption createClientEncryption(final MongoClient keyVaultClient, final ClientEncryptionSettings clientEncryptionSettings) {
        return new ClientEncryptionImpl(keyVaultClient, clientEncryptionSettings);
    }

    @NonNull
    protected static Collection<Arguments> getTestData(final String directory) throws URISyntaxException, IOException {
        return getTestData(directory, false);
    }
}
