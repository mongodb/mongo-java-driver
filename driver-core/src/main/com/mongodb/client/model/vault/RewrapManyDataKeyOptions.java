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

/**
 * The rewrap many data key options
 *
 * <p>
 *     The masterKey document MUST have the fields corresponding to the given provider as specified in masterKey.
 * </p>
 *
 * @since 4.7
 */
public class RewrapManyDataKeyOptions {

    private String provider;
    private BsonDocument masterKey;


    /**
     * Sets the provider name
     *
     * @param provider the provider name
     * @return this
     * @see #getProvider()
     */
    public RewrapManyDataKeyOptions provider(final String provider) {
        this.provider = provider;
        return this;
    }

    /**
      * @return the provider name
     */
    public String getProvider() {
        return provider;
    }

    /**
     * Sets the optional master key document.
     *
     * @param masterKey the master key document
     * @return this
     * @see #getMasterKey()
     */
    public RewrapManyDataKeyOptions masterKey(final BsonDocument masterKey) {
        this.masterKey = masterKey;
        return this;
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
     *     If the kmsProvider is "kmip" the master key is required and must contain the following fields:
     * </p>
     * <ul>
     *   <li>keyId: optional String, keyId is the KMIP Unique Identifier to a 96 byte KMIP Secret Data managed object. If keyId is
     *   omitted, the driver creates a random 96 byte KMIP Secret Data managed object.</li>
     *   <li>endpoint: a String, the endpoint as a host with required port. e.g. "example.com:443". If endpoint is not provided, it
     *   defaults to the required endpoint from the KMS providers map.</li>
     * </ul>
     * <p>
     * If the kmsProvider is "local" the masterKey is not applicable.
     * </p>
     * @return the master key document
     */
    public BsonDocument getMasterKey() {
        return masterKey;
    }

}
