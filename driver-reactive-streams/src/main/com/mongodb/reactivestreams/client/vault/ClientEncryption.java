/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client.vault;

import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.model.vault.RewrapManyDataKeyOptions;
import com.mongodb.client.model.vault.RewrapManyDataKeyResult;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.FindPublisher;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.reactivestreams.Publisher;

import java.io.Closeable;

/**
 * The Key vault.
 * <p>
 * Used to create data encryption keys, and to explicitly encrypt and decrypt values when auto-encryption is not an option.
 * </p>
 * @since 1.12
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
     * @return a Publisher containing the identifier for the created data key
     */
    Publisher<BsonBinary> createDataKey(String kmsProvider);

    /**
     * Create a data key with the given KMS provider and options.
     *
     * <p>
     * Creates a new key document and inserts into the key vault collection.
     * </p>
     *
     * @param kmsProvider    the KMS provider
     * @param dataKeyOptions the options for data key creation
     * @return a Publisher containing the identifier for the created data key
     */
    Publisher<BsonBinary> createDataKey(String kmsProvider, DataKeyOptions dataKeyOptions);

    /**
     * Encrypt the given value with the given options.
     * <p>
     *  The driver may throw an exception for prohibited BSON value types
     * </p>
     *
     * @param value   the value to encrypt
     * @param options the options for data encryption
     * @return a Publisher containing the encrypted value, a BSON binary of subtype 6
     */
    Publisher<BsonBinary> encrypt(BsonValue value, EncryptOptions options);

    /**
     * Decrypt the given value.
     *
     * @param value the value to decrypt, which must be of subtype 6
     * @return a Publisher containing the decrypted value
     */
    Publisher<BsonValue> decrypt(BsonBinary value);

    /**
     * Removes the key document with the given data key from the key vault collection.
     * @param id the data key UUID (BSON binary subtype 0x04)
     * @return a Publisher containing the delete result
     * @since 4.7
     */
    Publisher<DeleteResult> deleteKey(BsonBinary id);

    /**
     * Finds a single key document with the given UUID (BSON binary subtype 0x04).
     *
     * @param id the data key UUID (BSON binary subtype 0x04)
     * @return a Publisher containing the single key document or an empty publisher if there is no match
     * @since 4.7
     */
    @Nullable
    Publisher<BsonDocument> getKey(BsonBinary id);

    /**
     * Finds all documents in the key vault collection.
     * @return a find publisher for the documents in the key vault collection
     * @since 4.7
     */
    FindPublisher<BsonDocument> getKeys();

    /**
     * Adds a keyAltName to the keyAltNames array of the key document in the key vault collection with the given UUID.
     *
     * @param id the data key UUID (BSON binary subtype 0x04)
     * @param keyAltName the alternative key name to add to the keyAltNames array
     * @return a Publisher containing the previous version of the key document or an empty publisher if no match
     * @since 4.7
     */
    Publisher<BsonDocument> addKeyAltName(BsonBinary id, String keyAltName);

    /**
     * Removes a keyAltName from the keyAltNames array of the key document in the key vault collection with the given id.
     *
     * @param id the data key UUID (BSON binary subtype 0x04)
     * @param keyAltName the alternative key name
     * @return a Publisher containing the previous version of the key document or an empty publisher if there is no match
     * @since 4.7
     */
    Publisher<BsonDocument> removeKeyAltName(BsonBinary id, String keyAltName);

    /**
     * Returns a key document in the key vault collection with the given keyAltName.
     *
     * @param keyAltName the alternative key name
     * @return a Publisher containing the matching key document or an empty publisher if there is no match
     * @since 4.7
     */
    Publisher<BsonDocument> getKeyByAltName(String keyAltName);

    /**
     * Decrypts multiple data keys and (re-)encrypts them with the current masterKey.
     *
     * @param filter the filter
     * @return a Publisher containing the result
     * @since 4.7
     */
    Publisher<RewrapManyDataKeyResult> rewrapManyDataKey(BsonDocument filter);

    /**
     * Decrypts multiple data keys and (re-)encrypts them with a new masterKey, or with their current masterKey if a new one is not given.
     *
     * @param filter the filter
     * @param options the options
     * @return a Publisher containing the result
     * @since 4.7
     */
    Publisher<RewrapManyDataKeyResult> rewrapManyDataKey(BsonDocument filter, RewrapManyDataKeyOptions options);

    @Override
    void close();
}
