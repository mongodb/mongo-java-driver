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

package com.mongodb.reactivestreams.client.csot;


import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.csot.AbstractClientSideOperationsEncryptionTimeoutProseTest;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.reactivestreams.client.syncadapter.SyncClientEncryption;
import com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient;
import com.mongodb.reactivestreams.client.vault.ClientEncryptions;

public class ClientSideOperationsEncryptionTimeoutProseTest extends AbstractClientSideOperationsEncryptionTimeoutProseTest {
    public ClientEncryption createClientEncryption(final ClientEncryptionSettings.Builder builder) {
        return new SyncClientEncryption(ClientEncryptions.create(builder.build()));
    }

    @Override
    protected MongoClient createMongoClient(final MongoClientSettings.Builder builder) {
        return new SyncMongoClient(builder);
    }
}
