package org.mongodb.scala.model

import org.mongodb.scala.MongoNamespace
import org.mongodb.scala.bson.conversions.Bson

import scala.collection.JavaConverters._

/**
 * Models, options, results for the client-level bulk write operation.
 *
 * @since 5.3
 */
package object bulk {

  /**
   * A combination of an individual write operation and a [[MongoNamespace]]
   * the operation is targeted at.
   *
   * @since 5.3
   */
  type ClientNamespacedWriteModel = com.mongodb.client.model.bulk.ClientNamespacedWriteModel

  /**
   * A model for inserting a document.
   *
   * @since 5.3
   */
  type ClientNamespacedInsertOneModel = com.mongodb.client.model.bulk.ClientNamespacedInsertOneModel

  /**
   * A model for updating at most one document matching a filter.
   *
   * @since 5.3
   */
  type ClientNamespacedUpdateOneModel = com.mongodb.client.model.bulk.ClientNamespacedUpdateOneModel

  /**
   * A model for updating all documents matching a filter.
   *
   * @since 5.3
   */
  type ClientNamespacedUpdateManyModel = com.mongodb.client.model.bulk.ClientNamespacedUpdateManyModel

  /**
   * A model for replacing at most one document matching a filter.
   *
   * @since 5.3
   */
  type ClientNamespacedReplaceOneModel = com.mongodb.client.model.bulk.ClientNamespacedReplaceOneModel

  /**
   * A model for deleting at most one document matching a filter.
   *
   * @since 5.3
   */
  type ClientNamespacedDeleteOneModel = com.mongodb.client.model.bulk.ClientNamespacedDeleteOneModel

  /**
   * A model for deleting all documents matching a filter.
   *
   * @since 5.3
   */
  type ClientNamespacedDeleteManyModel = com.mongodb.client.model.bulk.ClientNamespacedDeleteManyModel

  object ClientNamespacedWriteModel {

    def insertOne[TDocument](namespace: MongoNamespace, document: TDocument): ClientNamespacedInsertOneModel =
      com.mongodb.client.model.bulk.ClientNamespacedWriteModel.insertOne(namespace, document)

    def updateOne(namespace: MongoNamespace, filter: Bson, update: Bson): ClientNamespacedUpdateOneModel =
      com.mongodb.client.model.bulk.ClientNamespacedWriteModel.updateOne(namespace, filter, update)

    def updateOne(
        namespace: MongoNamespace,
        filter: Bson,
        update: Bson,
        options: ClientUpdateOptions
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
        options: ClientUpdateOptions
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
        options: ClientUpdateOptions
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
        options: ClientUpdateOptions
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
        options: ClientReplaceOptions
    ): ClientNamespacedReplaceOneModel =
      com.mongodb.client.model.bulk.ClientNamespacedWriteModel.replaceOne(namespace, filter, replacement, options)

    def deleteOne(namespace: MongoNamespace, filter: Bson): ClientNamespacedDeleteOneModel =
      com.mongodb.client.model.bulk.ClientNamespacedWriteModel.deleteOne(namespace, filter)

    def deleteOne(
        namespace: MongoNamespace,
        filter: Bson,
        options: ClientDeleteOptions
    ): ClientNamespacedDeleteOneModel =
      com.mongodb.client.model.bulk.ClientNamespacedWriteModel.deleteOne(namespace, filter, options)

    def deleteMany(namespace: MongoNamespace, filter: Bson): ClientNamespacedDeleteManyModel =
      com.mongodb.client.model.bulk.ClientNamespacedWriteModel.deleteMany(namespace, filter)

    def deleteMany(
        namespace: MongoNamespace,
        filter: Bson,
        options: ClientDeleteOptions
    ): ClientNamespacedDeleteManyModel =
      com.mongodb.client.model.bulk.ClientNamespacedWriteModel.deleteMany(namespace, filter, options)
  }

  /**
   * The options to apply when executing a client-level bulk write operation.
   *
   * @since 5.3
   */
  type ClientBulkWriteOptions = com.mongodb.client.model.bulk.ClientBulkWriteOptions

  object ClientBulkWriteOptions {
    def clientBulkWriteOptions(): ClientBulkWriteOptions =
      com.mongodb.client.model.bulk.ClientBulkWriteOptions.clientBulkWriteOptions()
  }

  /**
   * The options to apply when updating documents.
   *
   * @since 5.3
   */
  type ClientUpdateOptions = com.mongodb.client.model.bulk.ClientUpdateOptions

  object ClientUpdateOptions {
    def clientUpdateOptions(): ClientUpdateOptions =
      com.mongodb.client.model.bulk.ClientUpdateOptions.clientUpdateOptions()
  }

  /**
   * The options to apply when replacing documents.
   *
   * @since 5.3
   */
  type ClientReplaceOptions = com.mongodb.client.model.bulk.ClientReplaceOptions

  object ClientReplaceOptions {
    def clientReplaceOptions(): ClientReplaceOptions =
      com.mongodb.client.model.bulk.ClientReplaceOptions.clientReplaceOptions()
  }

  /**
   * The options to apply when deleting documents.
   *
   * @since 5.3
   */
  type ClientDeleteOptions = com.mongodb.client.model.bulk.ClientDeleteOptions

  object ClientDeleteOptions {
    def clientDeleteOptions(): ClientDeleteOptions =
      com.mongodb.client.model.bulk.ClientDeleteOptions.clientDeleteOptions()
  }

  /**
   * The result of a successful or partially successful client-level bulk write operation.
   */
  type ClientBulkWriteResult = com.mongodb.client.model.bulk.ClientBulkWriteResult
}
