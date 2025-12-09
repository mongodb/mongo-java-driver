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

package com.mongodb.reactivestreams.client.unified;

import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.unified.UnifiedTest;
import com.mongodb.client.unified.UnifiedTestModifications;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.connection.TransportSettings;
import com.mongodb.lang.NonNull;
import com.mongodb.reactivestreams.client.gridfs.GridFSBuckets;
import com.mongodb.reactivestreams.client.internal.vault.ClientEncryptionImpl;
import com.mongodb.reactivestreams.client.syncadapter.SyncClientEncryption;
import com.mongodb.reactivestreams.client.syncadapter.SyncGridFSBucket;
import com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient;
import com.mongodb.reactivestreams.client.syncadapter.SyncMongoDatabase;
import org.junit.jupiter.params.provider.Arguments;

import java.util.Collection;

import static com.mongodb.ClusterFixture.getOverriddenTransportSettings;
import static com.mongodb.client.unified.UnifiedTestModifications.Modifier;
import static com.mongodb.client.unified.UnifiedTestModifications.TestDef;
import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.disableSleep;
import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.disableWaitForBatchCursorCreation;
import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.enableSleepAfterCursorClose;
import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.enableSleepAfterCursorOpen;
import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.enableWaitForBatchCursorCreation;

public abstract class UnifiedReactiveStreamsTest extends UnifiedTest {
    protected UnifiedReactiveStreamsTest() {
    }

    @Override
    protected MongoClient createMongoClient(final MongoClientSettings settings) {
        TransportSettings overriddenTransportSettings = getOverriddenTransportSettings();
        MongoClientSettings clientSettings = overriddenTransportSettings == null ? settings
                : MongoClientSettings.builder(settings).transportSettings(overriddenTransportSettings).build();
        return new SyncMongoClient(clientSettings);
    }

    @Override
    protected GridFSBucket createGridFSBucket(final MongoDatabase database) {
        return new SyncGridFSBucket(GridFSBuckets.create(((SyncMongoDatabase) database).getWrapped()));
    }

    @Override
    protected ClientEncryption createClientEncryption(final MongoClient keyVaultClient, final ClientEncryptionSettings clientEncryptionSettings) {
        return new SyncClientEncryption(new ClientEncryptionImpl(((SyncMongoClient) keyVaultClient).getWrapped(), clientEncryptionSettings));
    }

    @Override
    protected boolean isReactive() {
        return true;
    }

    @Override
    protected void postSetUp(final TestDef testDef) {
        super.postSetUp(testDef);
        if (testDef.wasAssignedModifier(UnifiedTestModifications.Modifier.IGNORE_EXTRA_EVENTS)) {
            ignoreExtraEvents(); // no disable needed
        }
        if (testDef.wasAssignedModifier(Modifier.SLEEP_AFTER_CURSOR_OPEN)) {
            enableSleepAfterCursorOpen(256);
        }
        if (testDef.wasAssignedModifier(Modifier.SLEEP_AFTER_CURSOR_CLOSE)) {
            enableSleepAfterCursorClose(256);
        }
        if (testDef.wasAssignedModifier(Modifier.WAIT_FOR_BATCH_CURSOR_CREATION)) {
            enableWaitForBatchCursorCreation();
        }
    }

    @Override
    protected void postCleanUp(final TestDef testDef) {
        super.postCleanUp(testDef);
        if (testDef.wasAssignedModifier(Modifier.WAIT_FOR_BATCH_CURSOR_CREATION)) {
            disableWaitForBatchCursorCreation();
        }
        if (testDef.wasAssignedModifier(Modifier.SLEEP_AFTER_CURSOR_CLOSE)) {
            disableSleep();
        }
        if (testDef.wasAssignedModifier(Modifier.SLEEP_AFTER_CURSOR_OPEN)) {
            disableSleep();
        }
    }

    @NonNull
    protected static Collection<Arguments> getTestData(final String directory) {
        return getTestData(directory, true, Language.JAVA);
    }
}
