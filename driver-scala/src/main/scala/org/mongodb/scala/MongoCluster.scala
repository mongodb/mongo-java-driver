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

import com.mongodb.annotations.{ Alpha, Reason }
import com.mongodb.{ ReadConcern, ReadPreference, WriteConcern }
import com.mongodb.reactivestreams.client.{ MongoCluster => JMongoCluster }
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.bson.DefaultHelper.DefaultsTo
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.bulk.{ ClientBulkWriteOptions, ClientBulkWriteResult, ClientNamespacedWriteModel }

import scala.collection.JavaConverters._
import scala.concurrent.duration.{ Duration, MILLISECONDS }
import scala.reflect.ClassTag

/**
 * Companion object for creating new [[MongoCluster]] instances
 *
 * @since 1.0
 */
object MongoCluster {

  /**
   * Create a new `MongoCluster` wrapper
   *
   * @param wrapped the java `MongoCluster` instance
   * @return MongoCluster
   */
  def apply(wrapped: JMongoCluster): MongoCluster = new MongoCluster(wrapped)
}

/**
 * The client-side representation of a MongoDB cluster operations.
 *
 * The originating [[MongoClient]] is responsible for the closing of resources.
 * If the originator [[MongoClient]] is closed, then any operations will fail.
 *
 * @see MongoClient
 * @since 5.2
 */
class MongoCluster(private val wrapped: JMongoCluster) {

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
   */
  lazy val readConcern: ReadConcern = wrapped.getReadConcern

  /**
   * The time limit for the full execution of an operation.
   *
   * If not null the following deprecated options will be ignored: `waitQueueTimeoutMS`, `socketTimeoutMS`,
   * `wTimeoutMS`, `maxTimeMS` and `maxCommitTimeMS`.
   *
   *   - `null` means that the timeout mechanism for operations will defer to using:
   *      - `waitQueueTimeoutMS`: The maximum wait time in milliseconds that a thread may wait for a connection to become available
   *      - `socketTimeoutMS`: How long a send or receive on a socket can take before timing out.
   *      - `wTimeoutMS`: How long the server will wait for  the write concern to be fulfilled before timing out.
   *      - `maxTimeMS`: The time limit for processing operations on a cursor.
   *        See: [cursor.maxTimeMS](https://docs.mongodb.com/manual/reference/method/cursor.maxTimeMS").
   *      - `maxCommitTimeMS`: The maximum amount of time to allow a single `commitTransaction` command to execute.
   *   - `0` means infinite timeout.
   *   - `> 0` The time limit to use for the full execution of an operation.
   *
   * @return the optional timeout duration
   */
  @Alpha(Array(Reason.CLIENT))
  lazy val timeout: Option[Duration] =
    Option.apply(wrapped.getTimeout(MILLISECONDS)).map(t => Duration(t, MILLISECONDS))

  /**
   * Create a new MongoCluster instance with a different codec registry.
   *
   * The { @link CodecRegistry} configured by this method is effectively treated by the driver as an
   * instance of { @link CodecProvider}, which { @link CodecRegistry} extends.
   * So there is no benefit to defining a class that implements { @link CodecRegistry}. Rather, an
   * application should always create { @link CodecRegistry} instances using the factory methods in
   * { @link CodecRegistries}.
   *
   * @param codecRegistry the new { @link org.bson.codecs.configuration.CodecRegistry} for the collection
   * @return a new MongoCluster instance with the different codec registry
   * @see CodecRegistries
   */
  def withCodecRegistry(codecRegistry: CodecRegistry): MongoCluster =
    MongoCluster(wrapped.withCodecRegistry(codecRegistry))

  /**
   * Create a new MongoCluster instance with a different read preference.
   *
   * @param readPreference the new { @link com.mongodb.ReadPreference} for the collection
   * @return a new MongoCluster instance with the different readPreference
   */
  def withReadPreference(readPreference: ReadPreference): MongoCluster =
    MongoCluster(wrapped.withReadPreference(readPreference))

  /**
   * Create a new MongoCluster instance with a different write concern.
   *
   * @param writeConcern the new { @link com.mongodb.WriteConcern} for the collection
   * @return a new MongoCluster instance with the different writeConcern
   */
  def withWriteConcern(writeConcern: WriteConcern): MongoCluster =
    MongoCluster(wrapped.withWriteConcern(writeConcern))

  /**
   * Create a new MongoCluster instance with a different read concern.
   *
   * @param readConcern the new [[ReadConcern]] for the collection
   * @return a new MongoCluster instance with the different ReadConcern
   * @since 1.1
   */
  def withReadConcern(readConcern: ReadConcern): MongoCluster =
    MongoCluster(wrapped.withReadConcern(readConcern))

  /**
   * Create a new MongoCluster instance with the set time limit for the full execution of an operation.
   *
   * - `0` means infinite timeout.
   * - `> 0` The time limit to use for the full execution of an operation.
   *
   * @param timeout the timeout, which must be greater than or equal to 0
   * @return a new MongoCluster instance with the set time limit for operations
   * @since 5.2
   */
  @Alpha(Array(Reason.CLIENT))
  def withTimeout(timeout: Duration): MongoCluster =
    MongoCluster(wrapped.withTimeout(timeout.toMillis, MILLISECONDS))

  /**
   * Creates a client session.
   *
   * '''Note:''' A ClientSession instance can not be used concurrently in multiple asynchronous operations.
   *
   * @since 2.4
   * @note Requires MongoDB 3.6 or greater
   */
  def startSession(): SingleObservable[ClientSession] = wrapped.startSession()

  /**
   * Creates a client session.
   *
   * '''Note:''' A ClientSession instance can not be used concurrently in multiple asynchronous operations.
   *
   * @param options  the options for the client session
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def startSession(options: ClientSessionOptions): SingleObservable[ClientSession] = wrapped.startSession(options)

  /**
   * Gets the database with the given name.
   *
   * @param name the name of the database
   * @return the database
   */
  def getDatabase(name: String): MongoDatabase = MongoDatabase(wrapped.getDatabase(name))

  /**
   * Get a list of the database names
   *
   * [[https://www.mongodb.com/docs/manual/reference/commands/listDatabases List Databases]]
   * @return an iterable containing all the names of all the databases
   */
  def listDatabaseNames(): Observable[String] = wrapped.listDatabaseNames()

  /**
   * Get a list of the database names
   *
   * [[https://www.mongodb.com/docs/manual/reference/commands/listDatabases List Databases]]
   *
   * @param clientSession the client session with which to associate this operation
   * @return an iterable containing all the names of all the databases
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def listDatabaseNames(clientSession: ClientSession): Observable[String] = wrapped.listDatabaseNames(clientSession)

  /**
   * Gets the list of databases
   *
   * @tparam TResult   the type of the class to use instead of `Document`.
   * @return the fluent list databases interface
   */
  def listDatabases[TResult]()(
      implicit e: TResult DefaultsTo Document,
      ct: ClassTag[TResult]
  ): ListDatabasesObservable[TResult] =
    ListDatabasesObservable(wrapped.listDatabases(ct))

  /**
   * Gets the list of databases
   *
   * @param clientSession the client session with which to associate this operation
   * @tparam TResult the type of the class to use instead of `Document`.
   * @return the fluent list databases interface
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def listDatabases[TResult](
      clientSession: ClientSession
  )(implicit e: TResult DefaultsTo Document, ct: ClassTag[TResult]): ListDatabasesObservable[TResult] =
    ListDatabasesObservable(wrapped.listDatabases(clientSession, ct))

  /**
   * Creates a change stream for this collection.
   *
   * @tparam C   the target document type of the observable.
   * @return the change stream observable
   * @since 2.4
   * @note Requires MongoDB 4.0 or greater
   */
  def watch[C]()(implicit e: C DefaultsTo Document, ct: ClassTag[C]): ChangeStreamObservable[C] =
    ChangeStreamObservable(wrapped.watch(ct))

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
  def watch[C](
      clientSession: ClientSession
  )(implicit e: C DefaultsTo Document, ct: ClassTag[C]): ChangeStreamObservable[C] =
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
  def watch[C](
      clientSession: ClientSession,
      pipeline: Seq[Bson]
  )(implicit e: C DefaultsTo Document, ct: ClassTag[C]): ChangeStreamObservable[C] =
    ChangeStreamObservable(wrapped.watch(clientSession, pipeline.asJava, ct))

  /**
   * Executes a client-level bulk write operation.
   * This method is functionally equivalent to `bulkWrite(List, ClientBulkWriteOptions)`
   * with the [[org.mongodb.scala.model.bulk.ClientBulkWriteOptions.clientBulkWriteOptions default options]].
   *
   * This operation supports retryable writes.
   * Depending on the number of `models`, encoded size of `models`, and the size limits in effect,
   * executing this operation may require multiple `bulkWrite` commands.
   * The eligibility for retries is determined per each `bulkWrite` command:
   * [[org.mongodb.scala.model.bulk.ClientNamespacedUpdateManyModel]], [[org.mongodb.scala.model.bulk.ClientNamespacedDeleteManyModel]] in a command render it non-retryable.
   *
   * This operation is not supported by MongoDB Atlas Serverless instances.
   *
   * [[https://www.mongodb.com/docs/manual/reference/command/bulkWrite/ bulkWrite]]
   * @param models The [[org.mongodb.scala.model.bulk.ClientNamespacedWriteModel]] individual write operations.
   * @return The [[SingleObservable]] signalling at most one element [[org.mongodb.scala.model.bulk.ClientBulkWriteResult]] if the operation is successful,
   *         or the following errors:
   *         - [[ClientBulkWriteException]]: If and only if the operation is unsuccessful or partially unsuccessful,
   *           and there is at least one of the following pieces of information to report:
   *           [[ClientBulkWriteException ClientBulkWriteException#getWriteConcernErrors]],
   *           [[ClientBulkWriteException ClientBulkWriteException#getWriteErrors]],
   *           [[ClientBulkWriteException ClientBulkWriteException#getPartialResult]].
   *         - [[MongoException]]: Only if the operation is unsuccessful.
   * @since 5.4
   * @note Requires MongoDB 8.0 or greater.
   */
  def bulkWrite(models: List[_ <: ClientNamespacedWriteModel]): SingleObservable[ClientBulkWriteResult] =
    wrapped.bulkWrite(models.asJava)

  /**
   * Executes a client-level bulk write operation.
   *
   * This operation supports retryable writes.
   * Depending on the number of `models`, encoded size of `models`, and the size limits in effect,
   * executing this operation may require multiple `bulkWrite` commands.
   * The eligibility for retries is determined per each `bulkWrite` command:
   * [[org.mongodb.scala.model.bulk.ClientNamespacedUpdateManyModel]], [[org.mongodb.scala.model.bulk.ClientNamespacedDeleteManyModel]] in a command render it non-retryable.
   *
   * This operation is not supported by MongoDB Atlas Serverless instances.
   *
   * [[https://www.mongodb.com/docs/manual/reference/command/bulkWrite/ bulkWrite]]
   * @param models The [[org.mongodb.scala.model.bulk.ClientNamespacedWriteModel]] individual write operations.
   * @param options The options.
   * @return The [[SingleObservable]] signalling at most one element [[org.mongodb.scala.model.bulk.ClientBulkWriteResult]] if the operation is successful,
   *         or the following errors:
   *         - [[ClientBulkWriteException]]: If and only if the operation is unsuccessful or partially unsuccessful,
   *           and there is at least one of the following pieces of information to report:
   *           [[ClientBulkWriteException ClientBulkWriteException#getWriteConcernErrors]],
   *           [[ClientBulkWriteException ClientBulkWriteException#getWriteErrors]],
   *           [[ClientBulkWriteException ClientBulkWriteException#getPartialResult]].
   *         - [[MongoException]]: Only if the operation is unsuccessful.
   * @since 5.4
   * @note Requires MongoDB 8.0 or greater.
   */
  def bulkWrite(
      models: List[_ <: ClientNamespacedWriteModel],
      options: ClientBulkWriteOptions
  ): SingleObservable[ClientBulkWriteResult] = wrapped.bulkWrite(models.asJava, options)

  /**
   * Executes a client-level bulk write operation.
   * This method is functionally equivalent to `bulkWrite(ClientSession, List, ClientBulkWriteOptions)`
   * with the [[org.mongodb.scala.model.bulk.ClientBulkWriteOptions.clientBulkWriteOptions default options]].
   *
   * This operation supports retryable writes.
   * Depending on the number of `models`, encoded size of `models`, and the size limits in effect,
   * executing this operation may require multiple `bulkWrite` commands.
   * The eligibility for retries is determined per each `bulkWrite` command:
   * [[org.mongodb.scala.model.bulk.ClientNamespacedUpdateManyModel]], [[org.mongodb.scala.model.bulk.ClientNamespacedDeleteManyModel]] in a command render it non-retryable.
   *
   * This operation is not supported by MongoDB Atlas Serverless instances.
   *
   * [[https://www.mongodb.com/docs/manual/reference/command/bulkWrite/ bulkWrite]]
   * @param clientSession [[ClientSession client session]] with which to associate this operation.
   * @param models The [[org.mongodb.scala.model.bulk.ClientNamespacedWriteModel]] individual write operations.
   * @return The [[SingleObservable]] signalling at most one element [[org.mongodb.scala.model.bulk.ClientBulkWriteResult]] if the operation is successful,
   *         or the following errors:
   *         - [[ClientBulkWriteException]]: If and only if the operation is unsuccessful or partially unsuccessful,
   *           and there is at least one of the following pieces of information to report:
   *           [[ClientBulkWriteException ClientBulkWriteException#getWriteConcernErrors]],
   *           [[ClientBulkWriteException ClientBulkWriteException#getWriteErrors]],
   *           [[ClientBulkWriteException ClientBulkWriteException#getPartialResult]].
   *         - [[MongoException]]: Only if the operation is unsuccessful.
   * @since 5.4
   * @note Requires MongoDB 8.0 or greater.
   */
  def bulkWrite(
      clientSession: ClientSession,
      models: List[_ <: ClientNamespacedWriteModel]
  ): SingleObservable[ClientBulkWriteResult] = wrapped.bulkWrite(clientSession, models.asJava)

  /**
   * Executes a client-level bulk write operation.
   *
   * This operation supports retryable writes.
   * Depending on the number of `models`, encoded size of `models`, and the size limits in effect,
   * executing this operation may require multiple `bulkWrite` commands.
   * The eligibility for retries is determined per each `bulkWrite` command:
   * [[org.mongodb.scala.model.bulk.ClientNamespacedUpdateManyModel]], [[org.mongodb.scala.model.bulk.ClientNamespacedDeleteManyModel]] in a command render it non-retryable.
   *
   * [[https://www.mongodb.com/docs/manual/reference/command/bulkWrite/ bulkWrite]]
   * @param clientSession The [[ClientSession client session]] with which to associate this operation.
   * @param models The [[org.mongodb.scala.model.bulk.ClientNamespacedWriteModel]] individual write operations.
   * @param options The options.
   * @return The [[SingleObservable]] signalling at most one element [[org.mongodb.scala.model.bulk.ClientBulkWriteResult]] if the operation is successful,
   *         or the following errors:
   *         - [[ClientBulkWriteException]]: If and only if the operation is unsuccessful or partially unsuccessful,
   *           and there is at least one of the following pieces of information to report:
   *           [[ClientBulkWriteException ClientBulkWriteException#getWriteConcernErrors]],
   *           [[ClientBulkWriteException ClientBulkWriteException#getWriteErrors]],
   *           [[ClientBulkWriteException ClientBulkWriteException#getPartialResult]].
   *         - [[MongoException]]: Only if the operation is unsuccessful.
   * @since 5.4
   * @note Requires MongoDB 8.0 or greater.
   */
  def bulkWrite(
      clientSession: ClientSession,
      models: List[_ <: ClientNamespacedWriteModel],
      options: ClientBulkWriteOptions
  ): SingleObservable[ClientBulkWriteResult] = wrapped.bulkWrite(clientSession, models.asJava, options)
}
