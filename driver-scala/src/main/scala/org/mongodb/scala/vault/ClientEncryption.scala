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

package org.mongodb.scala.vault

import java.io.Closeable

import com.mongodb.annotations.Beta
import com.mongodb.reactivestreams.client.vault.{ ClientEncryption => JClientEncryption }
import org.bson.{ BsonBinary, BsonValue }
import org.mongodb.scala.SingleObservable
import org.mongodb.scala.model.vault.{ DataKeyOptions, EncryptOptions }

/**
 * The Key vault.
 *
 * Used to create data encryption keys, and to explicitly encrypt and decrypt values when auto-encryption is not an option.
 *
 * @note support for client-side encryption should be considered as beta.  Backwards-breaking changes may be made before the final
 * release.
 * @since 2.7
 */
@Beta
case class ClientEncryption(private val wrapped: JClientEncryption) extends Closeable {

  /**
   * Create a data key with the given KMS provider.
   *
   * Creates a new key document and inserts into the key vault collection.
   *
   * @param kmsProvider the KMS provider
   * @return a Publisher containing the identifier for the created data key
   */
  def createDataKey(kmsProvider: String): SingleObservable[BsonBinary] = createDataKey(kmsProvider, DataKeyOptions())

  /**
   * Create a data key with the given KMS provider and options.
   *
   * Creates a new key document and inserts into the key vault collection.
   *
   * @param kmsProvider    the KMS provider
   * @param dataKeyOptions the options for data key creation
   * @return a Publisher containing the identifier for the created data key
   */
  def createDataKey(kmsProvider: String, dataKeyOptions: DataKeyOptions): SingleObservable[BsonBinary] =
    wrapped.createDataKey(kmsProvider, dataKeyOptions)

  /**
   * Encrypt the given value with the given options.
   * The driver may throw an exception for prohibited BSON value types
   *
   * @param value   the value to encrypt
   * @param options the options for data encryption
   * @return a Publisher containing the encrypted value, a BSON binary of subtype 6
   */
  def encrypt(value: BsonValue, options: EncryptOptions): SingleObservable[BsonBinary] =
    wrapped.encrypt(value, options)

  /**
   * Decrypt the given value.
   *
   * @param value the value to decrypt, which must be of subtype 6
   * @return a Publisher containing the decrypted value
   */
  def decrypt(value: BsonBinary): SingleObservable[BsonValue] = wrapped.decrypt(value)

  override def close(): Unit = wrapped.close()

}
