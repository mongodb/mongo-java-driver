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

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

/**
 * Options for dropping a collection
 *
 * @mongodb.driver.manual reference/command/drop/ Drop Collection
 * @mongodb.driver.manual core/security-client-side-encryption/ Client side encryption
 * @since 4.7
 */
public class DropCollectionOptions {
    private Bson encryptedFields;

    /**
     * Gets any explicitly set encrypted fields.
     *
     * <p>Note: If not set the driver will lookup the namespace in {@link AutoEncryptionSettings#getEncryptedFieldsMap()}</p>
     * @return the encrypted fields document
     * @since 4.7
     */
    @Nullable
    public Bson getEncryptedFields() {
        return encryptedFields;
    }

    /**
     * Sets the encrypted fields document
     *
     * <p>Explicitly set encrypted fields.</p>
     * <p>Note: If not set the driver will lookup the namespace in {@link AutoEncryptionSettings#getEncryptedFieldsMap()}</p>
     * @param encryptedFields the encrypted fields document
     * @return this
     * @since 4.7
     * @mongodb.driver.manual core/security-client-side-encryption/ Client side encryption
     */
    public DropCollectionOptions encryptedFields(@Nullable final Bson encryptedFields) {
        this.encryptedFields = encryptedFields;
        return this;
    }

    @Override
    public String toString() {
        return "DropCollectionOptions{"
                + ", encryptedFields=" + encryptedFields
                + '}';
    }
}
