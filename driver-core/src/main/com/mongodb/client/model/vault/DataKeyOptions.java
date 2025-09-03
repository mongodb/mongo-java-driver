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

import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;

import java.util.Arrays;
import java.util.List;

/**
 * The options for creating a data key.
 *
 * @since 3.11
 */
public class DataKeyOptions {
    private List<String> keyAltNames;
    private BsonDocument masterKey;
    private byte[] keyMaterial;

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
     * Sets the key material
     *
     * <p>An optional BinData of 96 bytes to use as custom key material for the data key being created.
     * If set the custom key material is used for encrypting and decrypting data. Otherwise, the key material for the new data key is
     * generated from a cryptographically secure random device.</p>
     *
     * @param keyMaterial the optional custom key material for the data key
     * @return this
     * @since 4.7
     * @see #getKeyMaterial()
     */
    public DataKeyOptions keyMaterial(final byte[] keyMaterial) {
        this.keyMaterial = keyMaterial;
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
    @Nullable
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
     *     If the kmsProvider type is "aws" the master key is required and must contain the following fields:
     * </p>
     * <ul>
     *   <li>region: a String containing the AWS region in which to locate the master key</li>
     *   <li>key: a String containing the Amazon Resource Name (ARN) to the AWS customer master key</li>
     * </ul>
     * <p>
     *     If the kmsProvider type is "azure" the master key is required and must contain the following fields:
     * </p>
     * <ul>
     *   <li>keyVaultEndpoint: a String with the host name and an optional port. Example: "example.vault.azure.net".</li>
     *   <li>keyName: a String</li>
     *   <li>keyVersion: an optional String, the specific version of the named key, defaults to using the key's primary version.</li>
     * </ul>
     * <p>
     *     If the kmsProvidertype type is "gcp" the master key is required and must contain the following fields:
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
     *     If the kmsProvider type is "kmip" the master key is required and must contain the following fields:
     * </p>
     * <ul>
     *   <li>keyId: optional String, keyId is the KMIP Unique Identifier to a 96 byte KMIP Secret Data managed object. If keyId is
     *   omitted, the driver creates a random 96 byte KMIP Secret Data managed object.</li>
     *   <li>endpoint: a String, the endpoint as a host with required port. e.g. "example.com:443". If endpoint is not provided, it
     *   defaults to the required endpoint from the KMS providers map.</li>
     *   <li>delegated: If true (recommended), the KMIP server will perform
     *   encryption and decryption. If delegated is not provided, defaults
     *   to false.</li>
     * </ul>
     * <p>
     * If the kmsProvider type is "local" the masterKey is not applicable.
     * </p>
     * @return the master key document
     */
    @Nullable
    public BsonDocument getMasterKey() {
        return masterKey;
    }

    /**
     * Gets the custom key material if set.
     *
     * <p>The optional BinData of 96 bytes to use as custom key material for the data key being created.
     * If set the custom key material is used for encrypting and decrypting data. Otherwise, the key material for the new data key is
     * generated from a cryptographically secure random device.</p>

     * @return the custom key material for the data key or null
     * @since 4.7
     */
    @Nullable
    public byte[] getKeyMaterial() {
        return keyMaterial;
    }

    @Override
    public String toString() {
        return "DataKeyOptions{"
                + "keyAltNames=" + keyAltNames
                + ", masterKey=" + masterKey
                + ", keyMaterial=" + Arrays.toString(keyMaterial)
                + '}';
    }
}
