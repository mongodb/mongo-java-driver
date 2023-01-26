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

package com.mongodb.client.model;

import com.mongodb.annotations.Beta;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Auxiliary parameters for creating an encrypted collection.
 *
 * @since 4.9
 */
@Beta(Beta.Reason.SERVER)
public final class CreateEncryptedCollectionParams {
    private final String kmsProvider;
    @Nullable
    private BsonDocument masterKey;

    /**
     * A constructor.
     *
     * @param kmsProvider The name of the KMS provider.
     */
    public CreateEncryptedCollectionParams(final String kmsProvider) {
        this.kmsProvider = notNull("kmsProvider", kmsProvider);
        masterKey = null;
    }

    /**
     * The name of the KMS provider.
     *
     * @return The name of the KMS provider.
     */
    public String getKmsProvider() {
        return kmsProvider;
    }

    /**
     * Sets the {@linkplain DataKeyOptions#getMasterKey() master key} for creating a data key.
     *
     * @param masterKey The master key for creating a data key.
     * @return {@code this}.
     */
    public CreateEncryptedCollectionParams masterKey(@Nullable final BsonDocument masterKey) {
        this.masterKey = masterKey;
        return this;
    }

    /**
     * The {@linkplain DataKeyOptions#getMasterKey() master key} for creating a data key.
     * The default is {@code null}.
     *
     * @return The master key for creating a data key.
     */
    @Nullable
    public BsonDocument getMasterKey() {
        return masterKey;
    }

    @Override
    public String toString() {
        return "CreateEncryptedCollectionParams{"
                + ", kmsProvider=" + kmsProvider
                + ", masterKey=" + masterKey
                + '}';
    }
}
