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

package org.mongodb.scala.model

import org.mongodb.scala.MongoNamespace
import org.mongodb.scala.bson.conversions.Bson

import scala.collection.JavaConverters._

/**
 * Models, options, results for the client-level bulk write operation.
 *
 * @since 5.4
 */
package object bulk {

  /**
   * A model for inserting a document.
   */
  type ClientNamespacedInsertOneModel = com.mongodb.client.model.bulk.ClientNamespacedInsertOneModel

  /**
   * A model for updating at most one document matching a filter.
   */
  type ClientNamespacedUpdateOneModel = com.mongodb.client.model.bulk.ClientNamespacedUpdateOneModel

  /**
   * A model for updating all documents matching a filter.
   */
  type ClientNamespacedUpdateManyModel = com.mongodb.client.model.bulk.ClientNamespacedUpdateManyModel

  /**
   * A model for replacing at most one document matching a filter.
   */
  type ClientNamespacedReplaceOneModel = com.mongodb.client.model.bulk.ClientNamespacedReplaceOneModel

  /**
   * A model for deleting at most one document matching a filter.
   */
  type ClientNamespacedDeleteOneModel = com.mongodb.client.model.bulk.ClientNamespacedDeleteOneModel

  /**
   * A model for deleting all documents matching a filter.
   */
  type ClientNamespacedDeleteManyModel = com.mongodb.client.model.bulk.ClientNamespacedDeleteManyModel

  /**
   * A combination of an individual write operation and a [[MongoNamespace]]
   * the operation is targeted at.
   */
  type ClientNamespacedWriteModel = com.mongodb.client.model.bulk.ClientNamespacedWriteModel

  object ClientNamespacedWriteModel {

    def insertOne[TDocument](namespace: MongoNamespace, document: TDocument): ClientNamespacedInsertOneModel =
      com.mongodb.client.model.bulk.ClientNamespacedWriteModel.insertOne(namespace, document)

    def updateOne(namespace: MongoNamespace, filter: Bson, update: Bson): ClientNamespacedUpdateOneModel =
      com.mongodb.client.model.bulk.ClientNamespacedWriteModel.updateOne(namespace, filter, update)

    def updateOne(
        namespace: MongoNamespace,
        filter: Bson,
        update: Bson,
        options: ClientUpdateOneOptions
    ): ClientNamespacedUpdateOneModel =
      com.mongodb.client.model.bulk.ClientNamespacedWriteModel.updateOne(namespace, filter, update, options)

    def updateOne(
        namespace: MongoNamespace,
        filter: Bson,
        updatePipeline: Iterable[_ <: Bson]
    ): ClientNamespacedUpdateOneModel =
      com.mongodb.client.model.bulk.ClientNamespacedWriteModel.updateOne(namespace, filter, updatePipeline.asJava)

    def updateOne(
        namespace: MongoNamespace,
        filter: Bson,
        updatePipeline: Iterable[_ <: Bson],
        options: ClientUpdateOneOptions
    ): ClientNamespacedUpdateOneModel =
      com.mongodb.client.model.bulk.ClientNamespacedWriteModel.updateOne(
        namespace,
        filter,
        updatePipeline.asJava,
        options
      )

    def updateMany(namespace: MongoNamespace, filter: Bson, update: Bson): ClientNamespacedUpdateManyModel =
      com.mongodb.client.model.bulk.ClientNamespacedWriteModel.updateMany(namespace, filter, update)

    def updateMany(
        namespace: MongoNamespace,
        filter: Bson,
        update: Bson,
        options: ClientUpdateManyOptions
    ): ClientNamespacedUpdateManyModel =
      com.mongodb.client.model.bulk.ClientNamespacedWriteModel.updateMany(namespace, filter, update, options)

    def updateMany(
        namespace: MongoNamespace,
        filter: Bson,
        updatePipeline: Iterable[_ <: Bson]
    ): ClientNamespacedUpdateManyModel =
      com.mongodb.client.model.bulk.ClientNamespacedWriteModel.updateMany(namespace, filter, updatePipeline.asJava)

    def updateMany(
        namespace: MongoNamespace,
        filter: Bson,
        updatePipeline: Iterable[_ <: Bson],
        options: ClientUpdateManyOptions
    ): ClientNamespacedUpdateManyModel =
      com.mongodb.client.model.bulk.ClientNamespacedWriteModel.updateMany(
        namespace,
        filter,
        updatePipeline.asJava,
        options
      )

    def replaceOne[TDocument](
        namespace: MongoNamespace,
        filter: Bson,
        replacement: TDocument
    ): ClientNamespacedReplaceOneModel =
      com.mongodb.client.model.bulk.ClientNamespacedWriteModel.replaceOne(namespace, filter, replacement)

    def replaceOne[TDocument](
        namespace: MongoNamespace,
        filter: Bson,
        replacement: TDocument,
        options: ClientReplaceOneOptions
    ): ClientNamespacedReplaceOneModel =
      com.mongodb.client.model.bulk.ClientNamespacedWriteModel.replaceOne(namespace, filter, replacement, options)

    def deleteOne(namespace: MongoNamespace, filter: Bson): ClientNamespacedDeleteOneModel =
      com.mongodb.client.model.bulk.ClientNamespacedWriteModel.deleteOne(namespace, filter)

    def deleteOne(
        namespace: MongoNamespace,
        filter: Bson,
        options: ClientDeleteOneOptions
    ): ClientNamespacedDeleteOneModel =
      com.mongodb.client.model.bulk.ClientNamespacedWriteModel.deleteOne(namespace, filter, options)

    def deleteMany(namespace: MongoNamespace, filter: Bson): ClientNamespacedDeleteManyModel =
      com.mongodb.client.model.bulk.ClientNamespacedWriteModel.deleteMany(namespace, filter)

    def deleteMany(
        namespace: MongoNamespace,
        filter: Bson,
        options: ClientDeleteManyOptions
    ): ClientNamespacedDeleteManyModel =
      com.mongodb.client.model.bulk.ClientNamespacedWriteModel.deleteMany(namespace, filter, options)
  }

  /**
   * The options to apply when executing a client-level bulk write operation.
   */
  type ClientBulkWriteOptions = com.mongodb.client.model.bulk.ClientBulkWriteOptions

  object ClientBulkWriteOptions {
    def clientBulkWriteOptions(): ClientBulkWriteOptions =
      com.mongodb.client.model.bulk.ClientBulkWriteOptions.clientBulkWriteOptions()
  }

  /**
   * The options to apply when updating a document.
   */
  type ClientUpdateOneOptions = com.mongodb.client.model.bulk.ClientUpdateOneOptions

  object ClientUpdateOneOptions {
    def clientUpdateOneOptions(): ClientUpdateOneOptions =
      com.mongodb.client.model.bulk.ClientUpdateOneOptions.clientUpdateOneOptions()
  }

  /**
   * The options to apply when updating documents.
   */
  type ClientUpdateManyOptions = com.mongodb.client.model.bulk.ClientUpdateManyOptions

  object ClientUpdateManyOptions {
    def clientUpdateManyOptions(): ClientUpdateManyOptions =
      com.mongodb.client.model.bulk.ClientUpdateManyOptions.clientUpdateManyOptions()
  }

  /**
   * The options to apply when replacing a document.
   */
  type ClientReplaceOneOptions = com.mongodb.client.model.bulk.ClientReplaceOneOptions

  object ClientReplaceOneOptions {
    def clientReplaceOneOptions(): ClientReplaceOneOptions =
      com.mongodb.client.model.bulk.ClientReplaceOneOptions.clientReplaceOneOptions()
  }

  /**
   * The options to apply when deleting a document.
   */
  type ClientDeleteOneOptions = com.mongodb.client.model.bulk.ClientDeleteOneOptions

  object ClientDeleteOneOptions {
    def clientDeleteOneOptions(): ClientDeleteOneOptions =
      com.mongodb.client.model.bulk.ClientDeleteOneOptions.clientDeleteOneOptions()
  }

  /**
   * The options to apply when deleting documents.
   */
  type ClientDeleteManyOptions = com.mongodb.client.model.bulk.ClientDeleteManyOptions

  object ClientDeleteManyOptions {
    def clientDeleteManyOptions(): ClientDeleteManyOptions =
      com.mongodb.client.model.bulk.ClientDeleteManyOptions.clientDeleteManyOptions()
  }

  /**
   * The result of a successful or partially successful client-level bulk write operation.
   *
   */
  type ClientBulkWriteResult = com.mongodb.client.model.bulk.ClientBulkWriteResult
}
