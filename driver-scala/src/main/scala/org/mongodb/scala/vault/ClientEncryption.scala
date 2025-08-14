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

import com.mongodb.annotations.{ Beta, Reason }
import com.mongodb.client.model.{ CreateCollectionOptions, CreateEncryptedCollectionParams }

import java.io.Closeable
import com.mongodb.reactivestreams.client.vault.{ ClientEncryption => JClientEncryption }
import org.bson.{ BsonBinary, BsonDocument, BsonValue }
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.{ Document, FindObservable, MongoDatabase, SingleObservable, ToSingleObservablePublisher }
import org.mongodb.scala.model.vault.{
  DataKeyOptions,
  EncryptOptions,
  RewrapManyDataKeyOptions,
  RewrapManyDataKeyResult
}
import org.mongodb.scala.result.DeleteResult

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
   * @return an Observable containing the identifier for the created data key
   */
  def createDataKey(kmsProvider: String): SingleObservable[BsonBinary] = createDataKey(kmsProvider, DataKeyOptions())

  /**
   * Create a data key with the given KMS provider and options.
   *
   * Creates a new key document and inserts into the key vault collection.
   *
   * @param kmsProvider    the KMS provider
   * @param dataKeyOptions the options for data key creation
   * @return an Observable containing the identifier for the created data key
   */
  def createDataKey(kmsProvider: String, dataKeyOptions: DataKeyOptions): SingleObservable[BsonBinary] =
    wrapped.createDataKey(kmsProvider, dataKeyOptions)

  /**
   * Encrypt the given value with the given options.
   * The driver may throw an exception for prohibited BSON value types
   *
   * @param value   the value to encrypt
   * @param options the options for data encryption
   * @return an Observable containing the encrypted value, a BSON binary of subtype 6
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
   * Only supported when queryType is "range" and algorithm is "Range".
   *
   * [[https://www.mongodb.com/docs/manual/core/queryable-encryption/ queryable encryption]]
   *
   * @note Requires MongoDB 8.0 or greater
   * @param expression the Match Expression or Aggregate Expression
   * @param options    the options
   * @return an Observable containing the queryable encrypted range expression
   * @since 4.9
   */
  def encryptExpression(
      expression: Document,
      options: EncryptOptions
  ): SingleObservable[Document] =
    wrapped.encryptExpression(expression.toBsonDocument, options).map(d => Document(d))

  /**
   * Decrypt the given value.
   *
   * @param value the value to decrypt, which must be of subtype 6
   * @return an Observable containing the decrypted value
   */
  def decrypt(value: BsonBinary): SingleObservable[BsonValue] = wrapped.decrypt(value)

  /**
   * Finds a single key document with the given UUID (BSON binary subtype 0x04).
   *
   * @param id the data key UUID (BSON binary subtype 0x04)
   * @return an Observable containing the single key document or an empty publisher if there is no match
   * @since 5.6
   */
  def getKey(id: BsonBinary): SingleObservable[BsonDocument] = wrapped.getKey(id)

  /**
   * Returns a key document in the key vault collection with the given keyAltName.
   *
   * @param keyAltName the alternative key name
   * @return an Observable containing the matching key document or an empty publisher if there is no match
   * @since 5.6
   */
  def getKeyByAltName(keyAltName: String): SingleObservable[BsonDocument] = wrapped.getKeyByAltName(keyAltName)

  /**
   * Finds all documents in the key vault collection.
   *
   * @return a find Observable for the documents in the key vault collection
   * @since 5.6
   */
  def keys: FindObservable[BsonDocument] = FindObservable(wrapped.getKeys)

  /**
   * Adds a keyAltName to the keyAltNames array of the key document in the key vault collection with the given UUID.
   *
   * @param id         the data key UUID (BSON binary subtype 0x04)
   * @param keyAltName the alternative key name to add to the keyAltNames array
   * @return an Observable containing the previous version of the key document or an empty publisher if no match
   * @since 5.6
   */
  def addKeyAltName(id: BsonBinary, keyAltName: String): SingleObservable[BsonDocument] =
    wrapped.addKeyAltName(id, keyAltName)

  /**
   * Removes the key document with the given data key from the key vault collection.
   *
   * @param id the data key UUID (BSON binary subtype 0x04)
   * @return an Observable containing the delete result
   * @since 5.6
   */
  def deleteKey(id: BsonBinary): SingleObservable[DeleteResult] = wrapped.deleteKey(id)

  /**
   * Removes a keyAltName from the keyAltNames array of the key document in the key vault collection with the given id.
   *
   * @param id         the data key UUID (BSON binary subtype 0x04)
   * @param keyAltName the alternative key name
   * @return an Observable containing the previous version of the key document or an empty Observable if there is no match
   * @since 5.6
   */
  def removeKeyAltName(id: BsonBinary, keyAltName: String): SingleObservable[BsonDocument] =
    wrapped.removeKeyAltName(id, keyAltName)

  /**
   * Decrypts multiple data keys and (re-)encrypts them with the current masterKey.
   *
   * @param filter the filter
   * @return an Observable containing the result
   * @since 5.6
   */
  def rewrapManyDataKey(filter: Bson): SingleObservable[RewrapManyDataKeyResult] = wrapped.rewrapManyDataKey(filter)

  /**
   * Decrypts multiple data keys and (re-)encrypts them with a new masterKey, or with their current masterKey if a new one is not given.
   *
   * @param filter  the filter
   * @param options the options
   * @return an Observable containing the result
   * @since 5.6
   */
  def rewrapManyDataKey(filter: Bson, options: RewrapManyDataKeyOptions): SingleObservable[RewrapManyDataKeyResult] =
    wrapped.rewrapManyDataKey(filter, options)

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
   * @return An Observable of the (potentially updated) `encryptedFields` configuration that was used to create the collection.
   * A user may use this document to configure `com.mongodb.AutoEncryptionSettings.getEncryptedFieldsMap`.
   *
   * Produces MongoUpdatedEncryptedFieldsException` if an exception happens after creating at least one data key.
   * This exception makes the updated `encryptedFields` available to the caller.
   * @since 4.9
   * @note Requires MongoDB 7.0 or greater.
   * @see [[https://www.mongodb.com/docs/manual/reference/command/create/ Create Command]]
   */
  @Beta(Array(Reason.SERVER))
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
