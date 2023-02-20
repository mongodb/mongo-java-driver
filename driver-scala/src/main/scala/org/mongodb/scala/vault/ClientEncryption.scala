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

import com.mongodb.annotations.Beta
import com.mongodb.client.model.{ CreateCollectionOptions, CreateEncryptedCollectionParams }

import java.io.Closeable
import com.mongodb.reactivestreams.client.vault.{ ClientEncryption => JClientEncryption }
import org.bson.{ BsonBinary, BsonDocument, BsonValue }
import org.mongodb.scala.{ Document, MongoDatabase, SingleObservable, ToSingleObservablePublisher }
import org.mongodb.scala.model.vault.{ DataKeyOptions, EncryptOptions }

/**
 * The Key vault.
 *
 * Used to create data encryption keys, and to explicitly encrypt and decrypt values when auto-encryption is not an option.
 *
 * @since 2.7
 */
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
   * Encrypts a Match Expression or Aggregate Expression to query a range index.
   *
   * The expression is expected to be in one of the following forms:
   *
   * - A Match Expression of this form:
   *   {{{ {\$and: [{<field>: {\$gt: <value1>}}, {<field>: {\$lt: <value2> }}]}} }}}
   * - An Aggregate Expression of this form:
   *   {{{ {\$and: [{\$gt: [<fieldpath>, <value1>]}, {\$lt: [<fieldpath>, <value2>]}] }} }}}
   *
   * `\$gt` may also be `\$gte`. `\$lt` may also be `\$lte`.
   *
   * Only supported when queryType is "rangePreview" and algorithm is "RangePreview".
   *
   * '''Note:''' The Range algorithm is experimental only. It is not intended for public use. It is subject to breaking changes.
   *
   * [[https://www.mongodb.com/docs/manual/core/queryable-encryption/ queryable encryption]]
   *
   * @note Requires MongoDB 6.2 or greater
   * @param expression the Match Expression or Aggregate Expression
   * @param options    the options
   * @return a Publisher containing the queryable encrypted range expression
   * @since 4.9
   */
  @Beta(Array(Beta.Reason.SERVER)) def encryptExpression(
      expression: Document,
      options: EncryptOptions
  ): SingleObservable[Document] =
    wrapped.encryptExpression(expression.toBsonDocument, options).map(d => Document(d))

  /**
   * Decrypt the given value.
   *
   * @param value the value to decrypt, which must be of subtype 6
   * @return a Publisher containing the decrypted value
   */
  def decrypt(value: BsonBinary): SingleObservable[BsonValue] = wrapped.decrypt(value)

  /**
   * Create a new collection with encrypted fields,
   * automatically creating
   * new data encryption keys when needed based on the configured
   * `encryptedFields`, which must be specified.
   * This method does not modify the configured `encryptedFields` when creating new data keys,
   * instead it creates a new configuration if needed.
   *
   * @param database The database to use for creating the collection.
   * @param collectionName The name for the collection to create.
   * @param createCollectionOptions Options for creating the collection.
   * @param createEncryptedCollectionParams Auxiliary parameters for creating an encrypted collection.
   * @return A publisher of the (potentially updated) `encryptedFields` configuration that was used to create the collection.
   * A user may use this document to configure `com.mongodb.AutoEncryptionSettings.getEncryptedFieldsMap`.
   *
   * Produces MongoUpdatedEncryptedFieldsException` if an exception happens after creating at least one data key.
   * This exception makes the updated `encryptedFields` available to the caller.
   * @since 4.9
   * @note Requires MongoDB 6.0 or greater.
   * @see [[https://www.mongodb.com/docs/manual/reference/command/create/ Create Command]]
   */
  @Beta(Array(Beta.Reason.SERVER))
  def createEncryptedCollection(
      database: MongoDatabase,
      collectionName: String,
      createCollectionOptions: CreateCollectionOptions,
      createEncryptedCollectionParams: CreateEncryptedCollectionParams
  ): SingleObservable[BsonDocument] =
    wrapped.createEncryptedCollection(
      database.wrapped,
      collectionName,
      createCollectionOptions,
      createEncryptedCollectionParams
    )

  override def close(): Unit = wrapped.close()

}
