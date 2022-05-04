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
package org.mongodb.scala

import com.mongodb.{ AutoEncryptionSettings => JAutoEncryptionSettings }

/**
 * The client-side automatic encryption settings. Client side encryption enables an application to specify what fields in a collection
 * must be encrypted, and the driver automatically encrypts commands sent to MongoDB and decrypts responses.
 *
 * Automatic encryption is an enterprise only feature that only applies to operations on a collection. Automatic encryption is not
 * supported for operations on a database or view and will result in error. To bypass automatic encryption,
 * set bypassAutoEncryption=true in `AutoEncryptionSettings`.
 *
 * Explicit encryption/decryption and automatic decryption is a community feature, enabled with the new
 * `com.mongodb.client.vault.ClientEncryption` type.
 *
 * A MongoClient configured with bypassAutoEncryption=true will still automatically decrypt.
 *
 * If automatic encryption fails on an operation, use a MongoClient configured with bypassAutoEncryption=true and use
 * ClientEncryption#encrypt to manually encrypt values.
 *
 * Enabling client side encryption reduces the maximum document and message size (using a maxBsonObjectSize of 2MiB and
 * maxMessageSizeBytes of 6MB) and may have a negative performance impact.
 *
 * Automatic encryption requires the authenticated user to have the listCollections privilege action.
 *
 * Supplying an `encryptedFieldsMap` provides more security than relying on an encryptedFields obtained from the server.
 * It protects against a malicious server advertising false encryptedFields.
 *
 * @since 2.7
 */
object AutoEncryptionSettings {

  /**
   * Gets a Builder for creating a new AutoEncryptionSettings instance.
   *
   * @return a new Builder for creating AutoEncryptionSettings.
   */
  def builder(): Builder = JAutoEncryptionSettings.builder()

  /**
   * AutoEncryptionSettings builder type
   */
  type Builder = JAutoEncryptionSettings.Builder
}
