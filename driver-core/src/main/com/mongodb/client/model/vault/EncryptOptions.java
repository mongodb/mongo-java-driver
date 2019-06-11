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

import org.bson.BsonBinary;
import org.bson.BsonValue;

/**
 * The options for explicit encryption.
 *
 * @since 3.11
 */
public class EncryptOptions {
    private BsonBinary keyId;
    private BsonValue keyAltName;
    private final String algorithm;

    /**
     * Construct an instance with the given algorithm.
     *
     * @param algorithm the encryption algorithm
     * @see #getAlgorithm()
     */
    public EncryptOptions(final String algorithm) {
        this.algorithm = algorithm;
    }

    /**
     * Gets the encryption algorithm, which must be either "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic" or
     * "AEAD_AES_256_CBC_HMAC_SHA_512-Random".
     *
     * @return the encryption algorithm
     */
    public String getAlgorithm() {
        return algorithm;
    }

    /**
     * Gets the key identifier.
     *
     * <p>
     * Identifies the data key by its _id value. The value is a UUID (binary subtype 4).
     * </p>
     *
     * @return the key identifier
     */
    public BsonBinary getKeyId() {
        return keyId;
    }

    /**
     * Gets the alternate name with which to look up the key.
     *
     * <p>
     * Identifies the alternate key name to look up the key by.
     * </p>
     *
     * @return the alternate name
     */
    public BsonValue getKeyAltName() {
        return keyAltName;
    }

    /**
     * Sets the key identifier
     *
     * @param keyId the key identifier
     * @return this
     * @see #getKeyId()
     */
    public EncryptOptions keyId(final BsonBinary keyId) {
        this.keyId = keyId;
        return this;
    }

    /**
     * Sets the alternate key name
     *
     * @param keyAltName the alternate key name
     * @return this
     * @see #getKeyAltName()
     */
    public EncryptOptions keyAltName(final BsonValue keyAltName) {
        this.keyAltName = keyAltName;
        return this;
    }

    @Override
    public String toString() {
        return "EncryptOptions{"
                + "keyId=" + keyId
                + ", keyAltName=" + keyAltName
                + ", algorithm='" + algorithm + "'}";
    }
}
