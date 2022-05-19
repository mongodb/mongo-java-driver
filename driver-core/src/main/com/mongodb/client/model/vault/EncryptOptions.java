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
import org.bson.BsonBinary;

/**
 * The options for explicit encryption.
 *
 * @since 3.11
 */
public class EncryptOptions {
    private BsonBinary keyId;
    private String keyAltName;
    private final String algorithm;
    private Long contentionFactor;
    private QueryType queryType;

    /**
     * The QueryType to use for "Indexed" queries
     *
     * @since 4.6
     */
    public enum QueryType {
        /**
         * Equality query type
         */
        EQUALITY
    }

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
     * Gets the encryption algorithm, which must be either:
     *
     * <ul>
     *     <li>AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic</li>
     *     <li>AEAD_AES_256_CBC_HMAC_SHA_512-Random</li>
     *     <li>Indexed</li>
     *     <li>Unindexed</li>
     * </ul>
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
    @Nullable
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
    @Nullable
    public String getKeyAltName() {
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
    public EncryptOptions keyAltName(final String keyAltName) {
        this.keyAltName = keyAltName;
        return this;
    }

    /**
     * The contention factor.
     *
     * <p>It is an error to set contentionFactor when algorithm is not "Indexed".
     * @param contentionFactor the contention factor, which must be {@code >= 0} or null.
     * @return this
     * @since 4.6
     */
    public EncryptOptions contentionFactor(@Nullable final Long contentionFactor) {
        this.contentionFactor = contentionFactor;
        return this;
    }

    /**
     * Gets the contention factor.
     *
     * @see #contentionFactor(Long)
     * @return the contention factor
     * @since 4.6
     */
    @Nullable
    public Long getContentionFactor() {
        return contentionFactor;
    }

    /**
     * The QueryType.
     *
     * <p>It is an error to set queryType when algorithm is not "Indexed".</p>
     *
     * @param queryType the query type
     * @return this
     * @since 4.6
     */
    public EncryptOptions queryType(@Nullable final QueryType queryType) {
        this.queryType = queryType;
        return this;
    }

    /**
     * Gets the QueryType.
     *
     * @see #queryType(QueryType)
     * @return the queryType or null
     * @since 4.6
     */
    @Nullable
    public QueryType getQueryType() {
        return queryType;
    }

    @Override
    public String toString() {
        return "EncryptOptions{"
                + "keyId=" + keyId
                + ", keyAltName='" + keyAltName + '\''
                + ", algorithm='" + algorithm + '\''
                + ", contentionFactor=" + contentionFactor
                + ", queryType=" + queryType
                + '}';
    }
}
