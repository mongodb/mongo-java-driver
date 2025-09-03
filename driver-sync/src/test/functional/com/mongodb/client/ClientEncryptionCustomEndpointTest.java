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

package com.mongodb.client;

import com.mongodb.ClientEncryptionSettings;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.client.vault.ClientEncryptions;
import org.bson.BsonDocument;

public class ClientEncryptionCustomEndpointTest extends AbstractClientEncryptionCustomEndpointTest {
    public ClientEncryptionCustomEndpointTest(final String name, final String provider, final BsonDocument masterKey,
            final boolean testInvalidClientEncryption, final Class<? extends RuntimeException> exceptionClass,
            final Class<? extends RuntimeException> wrappedExceptionClass, final String messageContainedInException) {
        super(name, provider, masterKey, testInvalidClientEncryption, exceptionClass, wrappedExceptionClass, messageContainedInException);
    }

    @Override
    public ClientEncryption getClientEncryption(final ClientEncryptionSettings settings) {
        return ClientEncryptions.create(settings);
    }
}
