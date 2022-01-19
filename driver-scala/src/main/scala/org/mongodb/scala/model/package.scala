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

import com.mongodb.annotations.Beta

import scala.jdk.CollectionConverters._
import com.mongodb.client.model.{ MongoTimeUnit => JMongoTimeUnit }
import org.mongodb.scala.bson.conversions.Bson

// scalastyle:off number.of.methods number.of.types
/**
 * The model package containing models and options that help describe `MongoCollection` operations
 */
package object model {

  /**
   * A representation of a BSON document field whose value is another BSON document.
   */
  type BsonField = com.mongodb.client.model.BsonField

  /**
   * A representation of a BSON document field whose value is another BSON document.
   */
  object BsonField {

    /**
     * Construct a new instance.
     *
     * @param name the name of the field
     * @param value the value for the field
     * @return a new BsonField instance
     */
    def apply(name: String, value: Bson): BsonField = {
      new com.mongodb.client.model.BsonField(name, value)
    }
  }

  /**
   * The options for a `\$bucketAuto` aggregation pipeline stage
   *
   * @since 1.2
   */
  type BucketAutoOptions = com.mongodb.client.model.BucketAutoOptions

  /**
   * The options for a `\$bucketAuto` aggregation pipeline stage
   *
   * @since 1.2
   */
  object BucketAutoOptions {
    def apply(): BucketAutoOptions = new com.mongodb.client.model.BucketAutoOptions()
  }

  /**
   * The options for a `\$bucket` aggregation pipeline stage
   *
   * @since 1.2
   */
  type BucketOptions = com.mongodb.client.model.BucketOptions

  /**
   * The options for a `\$bucket` aggregation pipeline stage
   *
   * @since 1.2
   */
  object BucketOptions {
    def apply(): BucketOptions = new com.mongodb.client.model.BucketOptions()
  }

  /**
   * Granularity values for automatic bucketing.
   *
   * @see [[https://en.wikipedia.org/wiki/Preferred_number">Preferred numbers]]
   * @since 1.2
   */
  type BucketGranularity = com.mongodb.client.model.BucketGranularity

  /**
   * The options to apply to a bulk write.
   */
  type BulkWriteOptions = com.mongodb.client.model.BulkWriteOptions

  /**
   * The options to apply to a bulk write.
   */
  object BulkWriteOptions {
    def apply(): BulkWriteOptions = new com.mongodb.client.model.BulkWriteOptions()
  }

  /**
   * The collation options.
   */
  type Collation = com.mongodb.client.model.Collation

  /**
   * The collation alternative options.
   */
  type CollationAlternate = com.mongodb.client.model.CollationAlternate

  /**
   * The collation max variable options
   */
  type CollationMaxVariable = com.mongodb.client.model.CollationMaxVariable

  /**
   * The collation configuration of how character cases are handled
   */
  type CollationCaseFirst = com.mongodb.client.model.CollationCaseFirst

  /**
   * The collation configuration of how differences between characters are handled.
   */
  type CollationStrength = com.mongodb.client.model.CollationStrength

  /**
   * The options to apply to a count operation.
   */
  type CountOptions = com.mongodb.client.model.CountOptions

  /**
   * The options to apply to a count operation.
   */
  object CountOptions {
    def apply(): CountOptions = new com.mongodb.client.model.CountOptions()
  }

  /**
   * The options to apply to an estimated count operation.
   * @since 2.4
   */
  type EstimatedDocumentCountOptions = com.mongodb.client.model.EstimatedDocumentCountOptions

  /**
   * The options to apply to an estimated count operation.
   * @since 2.4
   */
  object EstimatedDocumentCountOptions {
    def apply(): EstimatedDocumentCountOptions = new com.mongodb.client.model.EstimatedDocumentCountOptions()
  }

  /**
   * Options for creating a collection
   */
  type CreateCollectionOptions = com.mongodb.client.model.CreateCollectionOptions

  /**
   * Options for creating a collection
   */
  object CreateCollectionOptions {
    def apply(): CreateCollectionOptions = new com.mongodb.client.model.CreateCollectionOptions()
  }

  /**
   * Options for creating a time-series collection
   */
  type TimeSeriesOptions = com.mongodb.client.model.TimeSeriesOptions

  /**
   * Options for creating a time-series collection
   */
  object TimeSeriesOptions {
    def apply(timeFieldName: String): TimeSeriesOptions = new com.mongodb.client.model.TimeSeriesOptions(timeFieldName)
  }

  /**
   * Enumeration of values for time-series data granularity
   */
  type TimeSeriesGranularity = com.mongodb.client.model.TimeSeriesGranularity

  /**
   * Options for creating a view
   *
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  type CreateViewOptions = com.mongodb.client.model.CreateViewOptions

  /**
   * Options for creating a view
   *
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  object CreateViewOptions {
    def apply(): CreateViewOptions = new com.mongodb.client.model.CreateViewOptions()
  }

  /**
   * The default options for a collection to apply on the creation of indexes.
   *
   * @note Requires MongoDB 3.2 or greater
   * @see [[http://docs.mongodb.org/manual/reference/command/createIndexes Index options]]
   * @since 1.1
   */
  type IndexOptionDefaults = com.mongodb.client.model.IndexOptionDefaults

  /**
   * Options for creating an index
   */
  object IndexOptionDefaults {

    /**
     * Construct a new instance.
     */
    def apply(): IndexOptionDefaults = new com.mongodb.client.model.IndexOptionDefaults()
  }

  /**
   * The options to apply for delete operations
   */
  type DeleteOptions = com.mongodb.client.model.DeleteOptions

  /**
   * The options to apply for delete operations.
   */
  object DeleteOptions {
    def apply(): DeleteOptions = new com.mongodb.client.model.DeleteOptions()
  }

  /**
   * A model describing the removal of all documents matching the query filter.
   *
   * @tparam TResult the type of document to update.  In practice this doesn't actually apply to updates but is here for consistency with the
   *                 other write models
   */
  type DeleteManyModel[TResult] = com.mongodb.client.model.DeleteManyModel[TResult]

  /**
   * A model describing the removal of all documents matching the query filter.
   */
  object DeleteManyModel {
    def apply[T](filter: Bson): DeleteManyModel[T] = new com.mongodb.client.model.DeleteManyModel[T](filter)
    def apply[T](filter: Bson, options: DeleteOptions): DeleteManyModel[T] =
      new com.mongodb.client.model.DeleteManyModel[T](filter, options)
  }

  /**
   * A model describing the removal of at most one document matching the query filter.
   *
   * @tparam TResult the type of document to update.  In practice this doesn't actually apply to updates but is here for consistency with
   *                 the other write models
   */
  type DeleteOneModel[TResult] = com.mongodb.client.model.DeleteOneModel[TResult]

  /**
   * A model describing the removal of at most one document matching the query filter.
   */
  object DeleteOneModel {

    /**
     * Construct a new instance.
     *
     * @param filter the query filter
     * @return the new DeleteOneModel
     */
    def apply(filter: Bson): DeleteOneModel[Nothing] = new com.mongodb.client.model.DeleteOneModel(filter)
  }

  /**
   * Defines a Facet for use in `\$facet` pipeline stages.
   *
   * @since 1.2
   */
  type Facet = com.mongodb.client.model.Facet

  /**
   * Defines a Facet for use in `\$facet` pipeline stages.
   *
   * @since 1.2
   */
  object Facet {

    /**
     * Construct a new instance
     *
     * @param name     the name of this facet
     * @param pipeline the facet definition pipeline
     * @return the `\$facet` pipeline stage
     */
    def apply(name: String, pipeline: Bson*): Facet = {
      new com.mongodb.client.model.Facet(name, pipeline.asJava)
    }
  }

  /**
   * A helper to define new fields for the `\$addFields` pipeline stage
   *
   * @tparam TExpression the expression type
   * @since 1.2
   */
  type Field[TExpression] = com.mongodb.client.model.Field[TExpression]

  /**
   * A helper to define new fields for the `\$addFields` pipeline stage
   *
   * @since 1.2
   */
  object Field {

    /**
     * Construct a new instance.
     *
     * @param name  the name of the new field
     * @param value the value of the new field
     * @tparam TExpression the expression type
     */
    def apply[TExpression](name: String, value: TExpression): Field[TExpression] =
      new com.mongodb.client.model.Field(name, value)

  }

  /**
   * The options to apply to an operation that atomically finds a document and deletes it.
   */
  type FindOneAndDeleteOptions = com.mongodb.client.model.FindOneAndDeleteOptions

  /**
   * The options to apply to an operation that atomically finds a document and deletes it.
   */
  object FindOneAndDeleteOptions {
    def apply(): FindOneAndDeleteOptions = new com.mongodb.client.model.FindOneAndDeleteOptions()
  }

  /**
   * The options to apply to an operation that atomically finds a document and replaces it.
   */
  type FindOneAndReplaceOptions = com.mongodb.client.model.FindOneAndReplaceOptions

  /**
   * The options to apply to an operation that atomically finds a document and replaces it.
   */
  object FindOneAndReplaceOptions {
    def apply(): FindOneAndReplaceOptions = new com.mongodb.client.model.FindOneAndReplaceOptions()
  }

  /**
   * The options to apply to an operation that atomically finds a document and updates it.
   */
  type FindOneAndUpdateOptions = com.mongodb.client.model.FindOneAndUpdateOptions

  /**
   * The options to apply to an operation that atomically finds a document and updates it.
   */
  object FindOneAndUpdateOptions {
    def apply(): FindOneAndUpdateOptions = new com.mongodb.client.model.FindOneAndUpdateOptions()
  }

  /**
   * The options for a graphLookup aggregation pipeline stage
   * @since 1.2
   */
  type GraphLookupOptions = com.mongodb.client.model.GraphLookupOptions

  /**
   * The options for a graphLookup aggregation pipeline stage
   * @since 1.2
   */
  object GraphLookupOptions {
    def apply(): GraphLookupOptions = new com.mongodb.client.model.GraphLookupOptions()
  }

  /**
   * The options to apply to an operation that inserts multiple documents into a collection.
   */
  type InsertManyOptions = com.mongodb.client.model.InsertManyOptions

  /**
   * The options to apply to an operation that inserts multiple documents into a collection.
   */
  object InsertManyOptions {
    def apply(): InsertManyOptions = new com.mongodb.client.model.InsertManyOptions()
  }

  /**
   * The options to apply to the creation of an index.
   */
  type IndexOptions = com.mongodb.client.model.IndexOptions

  /**
   * The options to apply to the command when creating indexes.
   *
   * @since 2.2
   */
  type CreateIndexOptions = com.mongodb.client.model.CreateIndexOptions

  /**
   * The options to apply to the command when creating indexes.
   *
   * @since 2.2
   */
  type DropIndexOptions = com.mongodb.client.model.DropIndexOptions

  /**
   * The options to apply to the creation of an index.
   */
  object IndexOptions {
    def apply(): IndexOptions = new com.mongodb.client.model.IndexOptions()
  }

  /**
   * A model describing the creation of a single index.
   */
  type IndexModel = com.mongodb.client.model.IndexModel

  /**
   * A model describing the creation of a single index.
   */
  object IndexModel {

    /**
     * Construct an instance with the given keys.
     *
     * @param keys the index keys
     * @return the IndexModel
     */
    def apply(keys: Bson): IndexModel = new com.mongodb.client.model.IndexModel(keys)

    /**
     * Construct an instance with the given keys and options.
     *
     * @param keys the index keys
     * @param indexOptions the index options
     * @return the IndexModel
     */
    def apply(keys: Bson, indexOptions: IndexOptions): IndexModel =
      new com.mongodb.client.model.IndexModel(keys, indexOptions)
  }

  /**
   * The options to apply to an operation that inserts a single document into a collection.
   *
   * @note Requires MongoDB 3.2 or greater
   * @since 1.1
   */
  type InsertOneOptions = com.mongodb.client.model.InsertOneOptions

  /**
   * The options to apply to an operation that inserts a single document into a collection.
   *
   * @note Requires MongoDB 3.2 or greater
   * @since 1.1
   */
  object InsertOneOptions {

    /**
     * Construct a new instance.
     */
    def apply(): InsertOneOptions = new com.mongodb.client.model.InsertOneOptions()
  }

  /**
   * A model describing an insert of a single document.
   *
   * @tparam TResult the type of document to insert. This can be of any type for which a `Codec` is registered
   */
  type InsertOneModel[TResult] = com.mongodb.client.model.InsertOneModel[TResult]

  /**
   * A model describing an insert of a single document.
   */
  object InsertOneModel {

    /**
     * Construct a new instance.
     *
     * @param document the document to insert
     * @tparam TResult the type of document to insert. This can be of any type for which a `Codec` is registered
     * @return the new InsertOneModel
     */
    def apply[TResult](document: TResult): InsertOneModel[TResult] =
      new com.mongodb.client.model.InsertOneModel[TResult](document)
  }

  /**
   * The map reduce to collection actions.
   *
   * These actions are only available when passing out a collection that already exists. This option is not available on secondary members
   * of replica sets.  The Enum values dictate what to do with the output collection if it already exists when the map reduce is run.
   */
  type MapReduceAction = com.mongodb.client.model.MapReduceAction

  /**
   * The options to apply when renaming a collection
   */
  type RenameCollectionOptions = com.mongodb.client.model.RenameCollectionOptions

  /**
   * The options to apply when renaming a collection
   */
  object RenameCollectionOptions {
    def apply(): RenameCollectionOptions = new com.mongodb.client.model.RenameCollectionOptions()
  }

  /**
   * The options to apply to a `\$push` update operator.
   */
  type PushOptions = com.mongodb.client.model.PushOptions

  /**
   * The options to apply to a `\$push` update operator.
   */
  object PushOptions {
    def apply(): PushOptions = new com.mongodb.client.model.PushOptions()
  }

  /**
   * Indicates which document to return, the original document before change or the document after the change
   */
  type ReturnDocument = com.mongodb.client.model.ReturnDocument

  /**
   * A model describing the replacement of at most one document that matches the query filter.
   *
   * @tparam TResult the type of document to replace. This can be of any type for which a `Codec` is registered
   */
  type ReplaceOneModel[TResult] = com.mongodb.client.model.ReplaceOneModel[TResult]

  /**
   * The options to apply when replacing documents.
   * @since 2.3
   */
  type ReplaceOptions = com.mongodb.client.model.ReplaceOptions

  /**
   * The options to apply when replacing documents.
   * @since 2.3
   */
  object ReplaceOptions {
    def apply(): ReplaceOptions = new com.mongodb.client.model.ReplaceOptions()
  }

  /**
   * A model describing the replacement of at most one document that matches the query filter.
   */
  object ReplaceOneModel {

    /**
     * Construct a new instance.
     *
     * @param filter    a document describing the query filter.
     * @param replacement the replacement document
     * @tparam TResult the type of document to insert. This can be of any type for which a `Codec` is registered
     * @return the new ReplaceOneModel
     */
    def apply[TResult](filter: Bson, replacement: TResult): ReplaceOneModel[TResult] =
      new com.mongodb.client.model.ReplaceOneModel[TResult](filter, replacement)

    /**
     * Construct a new instance.
     *
     * @param filter    a document describing the query filter.
     * @param replacement the replacement document
     * @param replaceOptions the options to apply
     * @tparam TResult the type of document to insert. This can be of any type for which a `Codec` is registered
     * @return the new ReplaceOneModel
     * @since 2.3
     */
    def apply[TResult](filter: Bson, replacement: TResult, replaceOptions: ReplaceOptions): ReplaceOneModel[TResult] =
      new com.mongodb.client.model.ReplaceOneModel[TResult](filter, replacement, replaceOptions)
  }

  /**
   * Text search options for the [[Filters]] text helper
   *
   * @see [[http://docs.mongodb.org/manual/reference/operator/query/text \$text]]
   * @since 1.1
   */
  type TextSearchOptions = com.mongodb.client.model.TextSearchOptions

  /**
   * Text search options for the [[Filters]] text helper
   * @since 1.1
   */
  object TextSearchOptions {

    /**
     * Construct a new instance.
     */
    def apply(): TextSearchOptions = new com.mongodb.client.model.TextSearchOptions()
  }

  /**
   * Validation options for documents being inserted or updated in a collection
   *
   * @note Requires MongoDB 3.2 or greater
   * @since 1.1
   */
  type ValidationOptions = com.mongodb.client.model.ValidationOptions

  /**
   * Validation options for documents being inserted or updated in a collection
   *
   * @note Requires MongoDB 3.2 or greater
   * @since 1.1
   */
  object ValidationOptions {

    /**
     * Construct a new instance.
     */
    def apply(): ValidationOptions = new com.mongodb.client.model.ValidationOptions()
  }

  /**
   * A model describing an update to all documents that matches the query filter. The update to apply must include only update
   * operators.
   *
   * @tparam TResult the type of document to update.  In practice this doesn't actually apply to updates but is here for consistency with the
   *                 other write models
   */
  type UpdateManyModel[TResult] = com.mongodb.client.model.UpdateManyModel[TResult]

  /**
   * A model describing an update to all documents that matches the query filter. The update to apply must include only update
   * operators.
   */
  object UpdateManyModel {

    /**
     * Construct a new instance.
     *
     * @param filter a document describing the query filter.
     * @param update a document describing the update. The update to apply must include only update operators.
     * @return the new UpdateManyModel
     */
    def apply(filter: Bson, update: Bson): UpdateManyModel[Nothing] =
      new com.mongodb.client.model.UpdateManyModel(filter, update)

    /**
     * Construct a new instance.
     *
     * @param filter a document describing the query filter.
     * @param update a document describing the update. The update to apply must include only update operators.
     * @param updateOptions the options to apply
     * @return the new UpdateManyModel
     */
    def apply(filter: Bson, update: Bson, updateOptions: UpdateOptions): UpdateManyModel[Nothing] =
      new com.mongodb.client.model.UpdateManyModel(filter, update, updateOptions)
  }

  /**
   * The options to apply when updating documents.
   */
  type UpdateOptions = com.mongodb.client.model.UpdateOptions

  /**
   * The options to apply when updating documents.
   */
  object UpdateOptions {
    def apply(): UpdateOptions = new com.mongodb.client.model.UpdateOptions()
  }

  /**
   * A model describing an update to at most one document that matches the query filter. The update to apply must include only update
   * operators.
   *
   * @tparam TResult the type of document to update.  In practice this doesn't actually apply to updates but is here for consistency with the
   *                 other write models
   */
  type UpdateOneModel[TResult] = com.mongodb.client.model.UpdateOneModel[TResult]

  /**
   * A model describing an update to at most one document that matches the query filter. The update to apply must include only update
   * operators.
   */
  object UpdateOneModel {

    /**
     * Construct a new instance.
     *
     * @param filter a document describing the query filter.
     * @param update a document describing the update. The update to apply must include only update operators.
     * @return the new UpdateOneModel
     */
    def apply(filter: Bson, update: Bson): UpdateOneModel[Nothing] =
      new com.mongodb.client.model.UpdateOneModel(filter, update)

    /**
     * Construct a new instance.
     *
     * @param filter a document describing the query filter.
     * @param update a document describing the update. The update to apply must include only update operators.
     * @param updateOptions the options to apply
     * @return the new UpdateOneModel
     */
    def apply(filter: Bson, update: Bson, updateOptions: UpdateOptions): UpdateOneModel[Nothing] =
      new com.mongodb.client.model.UpdateOneModel(filter, update, updateOptions)
  }

  /**
   * The options for an unwind aggregation pipeline stage
   *
   * @note Requires MongoDB 3.2 or greater
   * @since 1.1
   */
  type UnwindOptions = com.mongodb.client.model.UnwindOptions

  /**
   * The options for an unwind aggregation pipeline stage
   *
   * @note Requires MongoDB 3.2 or greater
   * @since 1.1
   */
  object UnwindOptions {

    /**
     * Construct a new instance.
     */
    def apply(): UnwindOptions = new com.mongodb.client.model.UnwindOptions()
  }

  /**
   * Determines whether to error on invalid documents or just warn about the violations but allow invalid documents.
   *
   * @note Requires MongoDB 3.2 or greater
   * @since 1.1
   */
  type ValidationAction = com.mongodb.client.model.ValidationAction

  /**
   * Determines how strictly MongoDB applies the validation rules to existing documents during an insert or update.
   *
   * @note Requires MongoDB 3.2 or greater
   * @since 1.1
   */
  type ValidationLevel = com.mongodb.client.model.ValidationLevel

  /**
   * A base class for models that can be used in a bulk write operations.
   *
   * @tparam TResult the document type for storage
   */
  type WriteModel[TResult] = com.mongodb.client.model.WriteModel[TResult]

  /**
   * Helps define new variable for the `\$lookup` pipeline stage
   *
   * @tparam TExpression the type of the value for the new variable
   * @since 2.3
   */
  type Variable[TExpression] = com.mongodb.client.model.Variable[TExpression]

  /**
   * Helps define new variable for the `\$lookup` pipeline stage
   * @since 2.3
   */
  object Variable {

    /**
     * Creates a new variable definition for use in `\$lookup` pipeline stages
     *
     * @param name  the name of the new variable
     * @param value the value of the new variable
     */
    def apply[TExpression](name: String, value: TExpression): Variable[TExpression] =
      new com.mongodb.client.model.Variable[TExpression](name, value)

  }

  /**
   * Units for specifying time-based bounds for [[Window windows]] and output units for some time-based
   * [[WindowedComputation windowed computations]].
   *
   * @since 4.3
   * @note Requires MongoDB 5.0 or greater.
   */
  @Beta
  object MongoTimeUnit {

    val YEAR = JMongoTimeUnit.YEAR

    val QUARTER = JMongoTimeUnit.QUARTER

    val MONTH = JMongoTimeUnit.MONTH

    val WEEK = JMongoTimeUnit.WEEK

    val DAY = JMongoTimeUnit.DAY

    val HOUR = JMongoTimeUnit.HOUR

    val MINUTE = JMongoTimeUnit.MINUTE

    val SECOND = JMongoTimeUnit.SECOND

    val MILLISECOND = JMongoTimeUnit.MILLISECOND
  }

  /**
   * A subset of documents within a partition in the [[Aggregates.setWindowFields \$setWindowFields]] pipeline stage
   * of an aggregation pipeline (see `partitionBy` in [[Aggregates.setWindowFields]]).
   *
   * @see [[Windows]]
   * @since 4.3
   */
  @Beta
  type Window = com.mongodb.client.model.Window

  /**
   * The core part of the [[Aggregates.setWindowFields \$setWindowFields]] pipeline stage of an aggregation pipeline.
   * A triple of a window function, a [[Window window]] and a path to a field to be computed by the window function over the window.
   *
   * @see [[WindowedComputations]]
   * @since 4.3
   */
  @Beta
  type WindowedComputation = com.mongodb.client.model.WindowedComputation
}

// scalastyle:on number.of.methods number.of.types
