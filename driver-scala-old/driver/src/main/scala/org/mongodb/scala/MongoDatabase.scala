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

import com.mongodb.client.model.{CreateCollectionOptions, CreateViewOptions}
import com.mongodb.reactivestreams.client.{MongoDatabase => JMongoDatabase}
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.bson.DefaultHelper.DefaultsTo
import org.mongodb.scala.bson.conversions.Bson

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
 * The MongoDatabase representation.
 *
 * @param wrapped the underlying java MongoDatabase
 * @since 1.0
 */
case class MongoDatabase(private[scala] val wrapped: JMongoDatabase) {

  /**
   * Gets the name of the database.
   *
   * @return the database name
   */
  lazy val name: String = wrapped.getName

  /**
   * Get the codec registry for the MongoDatabase.
   *
   * @return the { @link org.bson.codecs.configuration.CodecRegistry}
   */
  lazy val codecRegistry: CodecRegistry = wrapped.getCodecRegistry

  /**
   * Get the read preference for the MongoDatabase.
   *
   * @return the { @link com.mongodb.ReadPreference}
   */
  lazy val readPreference: ReadPreference = wrapped.getReadPreference

  /**
   * Get the write concern for the MongoDatabase.
   *
   * @return the { @link com.mongodb.WriteConcern}
   */
  lazy val writeConcern: WriteConcern = wrapped.getWriteConcern

  /**
   * Get the read concern for the MongoDatabase.
   *
   * @return the [[ReadConcern]]
   * @since 1.1
   */
  lazy val readConcern: ReadConcern = wrapped.getReadConcern

  /**
   * Create a new MongoDatabase instance with a different codec registry.
   *
   * @param codecRegistry the new { @link org.bson.codecs.configuration.CodecRegistry} for the collection
   * @return a new MongoDatabase instance with the different codec registry
   */
  def withCodecRegistry(codecRegistry: CodecRegistry): MongoDatabase =
    MongoDatabase(wrapped.withCodecRegistry(codecRegistry))

  /**
   * Create a new MongoDatabase instance with a different read preference.
   *
   * @param readPreference the new { @link com.mongodb.ReadPreference} for the collection
   * @return a new MongoDatabase instance with the different readPreference
   */
  def withReadPreference(readPreference: ReadPreference): MongoDatabase =
    MongoDatabase(wrapped.withReadPreference(readPreference))

  /**
   * Create a new MongoDatabase instance with a different write concern.
   *
   * @param writeConcern the new { @link com.mongodb.WriteConcern} for the collection
   * @return a new MongoDatabase instance with the different writeConcern
   */
  def withWriteConcern(writeConcern: WriteConcern): MongoDatabase =
    MongoDatabase(wrapped.withWriteConcern(writeConcern))

  /**
   * Create a new MongoDatabase instance with a different read concern.
   *
   * @param readConcern the new [[ReadConcern]] for the collection
   * @return a new MongoDatabase instance with the different ReadConcern
   * @since 1.1
   */
  def withReadConcern(readConcern: ReadConcern): MongoDatabase =
    MongoDatabase(wrapped.withReadConcern(readConcern))

  /**
   * Gets a collection, with a specific default document class.
   *
   * @param collectionName the name of the collection to return
   * @tparam TResult       the type of the class to use instead of [[Document]].
   * @return the collection
   */
  def getCollection[TResult](collectionName: String)(implicit e: TResult DefaultsTo Document, ct: ClassTag[TResult]): MongoCollection[TResult] =
    MongoCollection(wrapped.getCollection(collectionName, ct))

  /**
   * Executes command in the context of the current database using the primary server.
   *
   * @param command  the command to be run
   * @tparam TResult the type of the class to use instead of [[Document]].
   * @return a Observable containing the command result
   */
  def runCommand[TResult](command: Bson)(implicit e: TResult DefaultsTo Document, ct: ClassTag[TResult]): SingleObservable[TResult] =
    wrapped.runCommand[TResult](command, ct)

  /**
   * Executes command in the context of the current database.
   *
   * @param command        the command to be run
   * @param readPreference the [[ReadPreference]] to be used when executing the command
   * @tparam TResult       the type of the class to use instead of [[Document]].
   * @return a Observable containing the command result
   */
  def runCommand[TResult](command: Bson, readPreference: ReadPreference)(
    implicit
    e: TResult DefaultsTo Document, ct: ClassTag[TResult]
  ): SingleObservable[TResult] =
    wrapped.runCommand(command, readPreference, ct)

  /**
   * Executes command in the context of the current database using the primary server.
   *
   * @param clientSession the client session with which to associate this operation
   * @param command  the command to be run
   * @tparam TResult the type of the class to use instead of [[Document]].
   * @return a Observable containing the command result
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def runCommand[TResult](clientSession: ClientSession, command: Bson)(
    implicit
    e: TResult DefaultsTo Document, ct: ClassTag[TResult]
  ): SingleObservable[TResult] =
    wrapped.runCommand[TResult](clientSession, command, ct)

  /**
   * Executes command in the context of the current database.
   *
   * @param command        the command to be run
   * @param readPreference the [[ReadPreference]] to be used when executing the command
   * @tparam TResult       the type of the class to use instead of [[Document]].
   * @return a Observable containing the command result
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def runCommand[TResult](clientSession: ClientSession, command: Bson, readPreference: ReadPreference)(
    implicit
    e: TResult DefaultsTo Document, ct: ClassTag[TResult]
  ): SingleObservable[TResult] =
    wrapped.runCommand(clientSession, command, readPreference, ct)

  /**
   * Drops this database.
   *
   * [[http://docs.mongodb.org/manual/reference/commands/dropDatabase/#dbcmd.dropDatabase Drop database]]
   * @return a Observable identifying when the database has been dropped
   */
  def drop(): SingleObservable[Completed] = wrapped.drop()

  /**
   * Drops this database.
   *
   * [[http://docs.mongodb.org/manual/reference/commands/dropDatabase/#dbcmd.dropDatabase Drop database]]
   * @param clientSession the client session with which to associate this operation
   * @return a Observable identifying when the database has been dropped
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def drop(clientSession: ClientSession): SingleObservable[Completed] = wrapped.drop(clientSession)

  /**
   * Gets the names of all the collections in this database.
   *
   * @return a Observable with all the names of all the collections in this database
   */
  def listCollectionNames(): Observable[String] = wrapped.listCollectionNames()

  /**
   * Finds all the collections in this database.
   *
   * [[http://docs.mongodb.org/manual/reference/command/listCollections listCollections]]
   * @tparam TResult the target document type of the iterable.
   * @return the fluent list collections interface
   */
  def listCollections[TResult]()(implicit e: TResult DefaultsTo Document, ct: ClassTag[TResult]): ListCollectionsObservable[TResult] =
    ListCollectionsObservable(wrapped.listCollections(ct))

  /**
   * Gets the names of all the collections in this database.
   *
   * @param clientSession the client session with which to associate this operation
   * @return a Observable with all the names of all the collections in this database
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def listCollectionNames(clientSession: ClientSession): Observable[String] = wrapped.listCollectionNames(clientSession)

  /**
   * Finds all the collections in this database.
   *
   * [[http://docs.mongodb.org/manual/reference/command/listCollections listCollections]]
   * @param clientSession the client session with which to associate this operation
   * @tparam TResult the target document type of the iterable.
   * @return the fluent list collections interface
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def listCollections[TResult](clientSession: ClientSession)(
    implicit
    e: TResult DefaultsTo Document, ct: ClassTag[TResult]
  ): ListCollectionsObservable[TResult] =
    ListCollectionsObservable(wrapped.listCollections(clientSession, ct))

  /**
   * Create a new collection with the given name.
   *
   * [[http://docs.mongodb.org/manual/reference/commands/create Create Command]]
   * @param collectionName the name for the new collection to create
   * @return a Observable identifying when the collection has been created
   */
  def createCollection(collectionName: String): SingleObservable[Completed] =
    wrapped.createCollection(collectionName)

  /**
   * Create a new collection with the selected options
   *
   * [[http://docs.mongodb.org/manual/reference/commands/create Create Command]]
   * @param collectionName the name for the new collection to create
   * @param options        various options for creating the collection
   * @return a Observable identifying when the collection has been created
   */
  def createCollection(collectionName: String, options: CreateCollectionOptions): SingleObservable[Completed] =
    wrapped.createCollection(collectionName, options)

  /**
   * Create a new collection with the given name.
   *
   * [[http://docs.mongodb.org/manual/reference/commands/create Create Command]]
   * @param clientSession the client session with which to associate this operation
   * @param collectionName the name for the new collection to create
   * @return a Observable identifying when the collection has been created
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def createCollection(clientSession: ClientSession, collectionName: String): SingleObservable[Completed] =
    wrapped.createCollection(clientSession, collectionName)

  /**
   * Create a new collection with the selected options
   *
   * [[http://docs.mongodb.org/manual/reference/commands/create Create Command]]
   * @param clientSession the client session with which to associate this operation
   * @param collectionName the name for the new collection to create
   * @param options        various options for creating the collection
   * @return a Observable identifying when the collection has been created
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def createCollection(clientSession: ClientSession, collectionName: String, options: CreateCollectionOptions): SingleObservable[Completed] =
    wrapped.createCollection(clientSession, collectionName, options)

  /**
   * Creates a view with the given name, backing collection/view name, and aggregation pipeline that defines the view.
   *
   * [[http://docs.mongodb.org/manual/reference/commands/create Create Command]]
   * @param viewName the name of the view to create
   * @param viewOn   the backing collection/view for the view
   * @param pipeline the pipeline that defines the view
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def createView(viewName: String, viewOn: String, pipeline: Seq[Bson]): SingleObservable[Completed] =
    wrapped.createView(viewName, viewOn, pipeline.asJava)

  /**
   * Creates a view with the given name, backing collection/view name, aggregation pipeline, and options that defines the view.
   *
   * [[http://docs.mongodb.org/manual/reference/commands/create Create Command]]
   * @param viewName          the name of the view to create
   * @param viewOn            the backing collection/view for the view
   * @param pipeline          the pipeline that defines the view
   * @param createViewOptions various options for creating the view
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def createView(viewName: String, viewOn: String, pipeline: Seq[Bson], createViewOptions: CreateViewOptions): SingleObservable[Completed] =
    wrapped.createView(viewName, viewOn, pipeline.asJava, createViewOptions)

  /**
   * Creates a view with the given name, backing collection/view name, and aggregation pipeline that defines the view.
   *
   * [[http://docs.mongodb.org/manual/reference/commands/create Create Command]]
   * @param clientSession the client session with which to associate this operation
   * @param viewName the name of the view to create
   * @param viewOn   the backing collection/view for the view
   * @param pipeline the pipeline that defines the view
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def createView(clientSession: ClientSession, viewName: String, viewOn: String, pipeline: Seq[Bson]): SingleObservable[Completed] =
    wrapped.createView(clientSession, viewName, viewOn, pipeline.asJava)

  /**
   * Creates a view with the given name, backing collection/view name, aggregation pipeline, and options that defines the view.
   *
   * [[http://docs.mongodb.org/manual/reference/commands/create Create Command]]
   * @param clientSession the client session with which to associate this operation
   * @param viewName          the name of the view to create
   * @param viewOn            the backing collection/view for the view
   * @param pipeline          the pipeline that defines the view
   * @param createViewOptions various options for creating the view
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def createView(clientSession: ClientSession, viewName: String, viewOn: String, pipeline: Seq[Bson],
                 createViewOptions: CreateViewOptions): SingleObservable[Completed] =
    wrapped.createView(clientSession, viewName, viewOn, pipeline.asJava, createViewOptions)

  /**
   * Creates a change stream for this collection.
   *
   * @tparam C   the target document type of the observable.
   * @return the change stream observable
   * @since 2.4
   * @note Requires MongoDB 4.0 or greater
   */
  def watch[C]()(implicit e: C DefaultsTo Document, ct: ClassTag[C]): ChangeStreamObservable[C] = ChangeStreamObservable(wrapped.watch(ct))

  /**
   * Creates a change stream for this collection.
   *
   * @param pipeline the aggregation pipeline to apply to the change stream
   * @tparam C   the target document type of the observable.
   * @return the change stream observable
   * @since 2.4
   * @note Requires MongoDB 4.0 or greater
   */
  def watch[C](pipeline: Seq[Bson])(implicit e: C DefaultsTo Document, ct: ClassTag[C]): ChangeStreamObservable[C] =
    ChangeStreamObservable(wrapped.watch(pipeline.asJava, ct))

  /**
   * Creates a change stream for this collection.
   *
   * @param clientSession the client session with which to associate this operation
   * @tparam C   the target document type of the observable.
   * @return the change stream observable
   * @since 2.4
   * @note Requires MongoDB 4.0 or greater
   */
  def watch[C](clientSession: ClientSession)(implicit e: C DefaultsTo Document, ct: ClassTag[C]): ChangeStreamObservable[C] =
    ChangeStreamObservable(wrapped.watch(clientSession, ct))

  /**
   * Creates a change stream for this collection.
   *
   * @param clientSession the client session with which to associate this operation
   * @param pipeline the aggregation pipeline to apply to the change stream
   * @tparam C   the target document type of the observable.
   * @return the change stream observable
   * @since 2.4
   * @note Requires MongoDB 4.0 or greater
   */
  def watch[C](clientSession: ClientSession, pipeline: Seq[Bson])(implicit e: C DefaultsTo Document, ct: ClassTag[C]): ChangeStreamObservable[C] =
    ChangeStreamObservable(wrapped.watch(clientSession, pipeline.asJava, ct))

  /**
   * Aggregates documents according to the specified aggregation pipeline.
   *
   * @param pipeline the aggregate pipeline
   * @return a Observable containing the result of the aggregation operation
   *         [[http://docs.mongodb.org/manual/aggregation/ Aggregation]]
   * @since 2.6
   * @note Requires MongoDB 3.6 or greater
   */
  def aggregate[C](pipeline: Seq[Bson])(implicit e: C DefaultsTo Document, ct: ClassTag[C]): AggregateObservable[C] =
    AggregateObservable(wrapped.aggregate[C](pipeline.asJava, ct))

  /**
   * Aggregates documents according to the specified aggregation pipeline.
   *
   * @param clientSession the client session with which to associate this operation
   * @param pipeline the aggregate pipeline
   * @return a Observable containing the result of the aggregation operation
   *         [[http://docs.mongodb.org/manual/aggregation/ Aggregation]]
   * @since 2.6
   * @note Requires MongoDB 3.6 or greater
   */
  def aggregate[C](clientSession: ClientSession, pipeline: Seq[Bson])(implicit e: C DefaultsTo Document, ct: ClassTag[C]): AggregateObservable[C] =
    AggregateObservable(wrapped.aggregate[C](clientSession, pipeline.asJava, ct))
}
