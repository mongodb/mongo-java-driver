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

package com.mongodb.client.model.vault;

import org.bson.BsonDocument;

import java.util.List;

/**
 * The options for creating a data key.
 *
 * @since 3.11
 */
public class DataKeyOptions {
    private List<String> keyAltNames;
    private BsonDocument masterKey;

    /**
     * Set the alternate key names.
     *
     * @param keyAltNames a list of alternate key names
     * @return this
     * @see #getKeyAltNames()
     */
    public DataKeyOptions keyAltNames(final List<String> keyAltNames) {
        this.keyAltNames = keyAltNames;
        return this;
    }

    /**
     * Sets the master key document.
     *
     * @param masterKey the master key document
     * @return this
     * @see #getMasterKey()
     */
    public DataKeyOptions masterKey(final BsonDocument masterKey) {
        this.masterKey = masterKey;
        return this;
    }

    /**
     * Gets the alternate key names.
     *
     * <p>
     * An optional list of alternate names used to reference a key. If a key is created with alternate names, then encryption may refer
     * to the key by the unique alternate name instead of by _id.
     * </p>
     *
     * @return the list of alternate key names
     */
    public List<String> getKeyAltNames() {
        return keyAltNames;
    }

    /**
     * Gets the master key document
     *
     * <p>
     * The masterKey identifies a KMS-specific key used to encrypt the new data key.
     * </p>
     * <p>
     *     If the kmsProvider is "aws" the master key is required and must contain the following fields:
     * </p>
     * <ul>
     *   <li>region: a String containing the AWS region in which to locate the master key</li>
     *   <li>key: a String containing the Amazon Resource Name (ARN) to the AWS customer master key</li>
     * </ul>
     * <p>
     *     If the kmsProvider is "azure" the master key is required and must contain the following fields:
     * </p>
     * <ul>
     *   <li>keyVaultEndpoint: a String with the host name and an optional port. Example: "example.vault.azure.net".</li>
     *   <li>keyName: a String</li>
     *   <li>keyVersion: an optional String, the specific version of the named key, defaults to using the key's primary version.</li>
     * </ul>
     * <p>
     *     If the kmsProvider is "gcp" the master key is required and must contain the following fields:
     * </p>
     * <ul>
     *   <li>projectId: a String</li>
     *   <li>location: String</li>
     *   <li>keyRing: String</li>
     *   <li>keyName: String</li>
     *   <li>keyVersion: an optional String, the specific version of the named key, defaults to using the key's primary version.</li>
     *   <li>endpoint: an optional String, with the host with optional port. Defaults to "cloudkms.googleapis.com".</li>
     * </ul>
     * <p>
     * If the kmsProvider is "local" the masterKey is not applicable.
     * </p>
     * @return the master key document
     */
    public BsonDocument getMasterKey() {
        return masterKey;
    }

    @Override
    public String toString() {
        return "DataKeyOptions{"
                + "keyAltNames=" + keyAltNames
                + ", masterKey=" + masterKey
                + '}';
    }
}
