/*
 * Copyright 2015 MongoDB, Inc.
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

import com.mongodb.annotations.Beta;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import org.bson.BsonBinary;
import org.bson.BsonValue;
import org.reactivestreams.Publisher;

import java.io.Closeable;

/**
 * The Key vault.
 * <p>
 * Used to create data encryption keys, and to explicitly encrypt and decrypt values when auto-encryption is not an option.
 * </p>
 * <p>
 * Note: support for client-side encryption should be considered as beta.  Backwards-breaking changes may be made before the final
 * release.
 * </p>
 * @since 1.12
 */
@Beta
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

    @Override
    void close();
}
