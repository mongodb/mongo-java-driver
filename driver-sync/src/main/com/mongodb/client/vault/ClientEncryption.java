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

package com.mongodb.client.vault;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.model.vault.RewrapManyDataKeyOptions;
import com.mongodb.client.model.vault.RewrapManyDataKeyResult;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.lang.Nullable;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import java.io.Closeable;

/**
 * The Key vault.
 * <p>
 * Used to create data encryption keys, and to explicitly encrypt and decrypt values when auto-encryption is not an option.
 * </p>
 *
 * @since 3.11
 */
public interface ClientEncryption extends Closeable {

    /**
     * Create a data key with the given KMS provider.
     *
     * <p>
     * Creates a new key document and inserts into the key vault collection.
     * </p>
     *
     * @param kmsProvider the KMS provider
     * @return the identifier for the created data key
     */
    BsonBinary createDataKey(String kmsProvider);

    /**
     * Create a data key with the given KMS provider and options.
     *
     * <p>
     * Creates a new key document and inserts into the key vault collection.
     * </p>
     *
     * @param kmsProvider    the KMS provider
     * @param dataKeyOptions the options for data key creation
     * @return the identifier for the created data key
     */
    BsonBinary createDataKey(String kmsProvider, DataKeyOptions dataKeyOptions);

    /**
     * Encrypt the given value with the given options.
     * <p>
     *  The driver may throw an exception for prohibited BSON value types
     * </p>
     *
     * @param value   the value to encrypt
     * @param options the options for data encryption
     * @return the encrypted value, a BSON binary of subtype 6
     */
    BsonBinary encrypt(BsonValue value, EncryptOptions options);

    /**
     * Decrypt the given value.
     *
     * @param value the value to decrypt, which must be of subtype 6
     * @return the decrypted value
     */
    BsonValue decrypt(BsonBinary value);

    /**
     * Removes the key document with the given data key from the key vault collection.
     * @param id the data key UUID (BSON binary subtype 0x04)
     * @return the result
     * @since 4.7
     */
    DeleteResult deleteKey(BsonBinary id);

    /**
     * Finds a single key document with the given UUID (BSON binary subtype 0x04).
     *
     * @param id the data key UUID (BSON binary subtype 0x04)
     * @return the single key document or null if there is no match
     * @since 4.7
     */
    @Nullable
    BsonDocument getKey(BsonBinary id);

    /**
     * Finds all documents in the key vault collection.
     * @return a find iterable for the documents in the key vault collection
     * @since 4.7
     */
    FindIterable<BsonDocument> getKeys();

    /**
     * Adds a keyAltName to the keyAltNames array of the key document in the key vault collection with the given UUID.
     *
     * @param id the data key UUID (BSON binary subtype 0x04)
     * @param keyAltName the alternative key name to add to the keyAltNames array
     * @return the previous version of the key document or null if no match
     * @since 4.7
     */
    @Nullable
    BsonDocument addKeyAltName(BsonBinary id, String keyAltName);

    /**
     * Removes a keyAltName from the keyAltNames array of the key document in the key vault collection with the given id.
     *
     * @param id the data key UUID (BSON binary subtype 0x04)
     * @param keyAltName the alternative key name
     * @return the previous version of the key document or null if no match
     * @since 4.7
     */
    @Nullable
    BsonDocument removeKeyAltName(BsonBinary id, String keyAltName);

    /**
     * Returns a key document in the key vault collection with the given keyAltName.
     *
     * @param keyAltName the alternative key name
     * @return a matching key document or null
     * @since 4.7
     */
    @Nullable
    BsonDocument getKeyByAltName(String keyAltName);

    /**
     * Decrypts multiple data keys and (re-)encrypts them with the current masterKey.
     *
     * @param filter the filter
     * @return the result
     * @since 4.7
     */
    RewrapManyDataKeyResult rewrapManyDataKey(Bson filter);

    /**
     * Decrypts multiple data keys and (re-)encrypts them with a new masterKey, or with their current masterKey if a new one is not given.
     *
     * @param filter the filter
     * @param options the options
     * @return the result
     * @since 4.7
     */
    RewrapManyDataKeyResult rewrapManyDataKey(Bson filter, RewrapManyDataKeyOptions options);

    @Override
    void close();
}
