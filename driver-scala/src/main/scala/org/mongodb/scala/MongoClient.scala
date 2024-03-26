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

import com.mongodb.connection.ClusterDescription
import com.mongodb.reactivestreams.client.{ MongoClient => JMongoClient, MongoClients }
import org.bson.codecs.configuration.CodecRegistries.{ fromProviders, fromRegistries }
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.bson.codecs.{ DocumentCodecProvider, IterableCodecProvider }

import java.io.Closeable

/**
 * Companion object for creating new [[MongoClient]] instances
 *
 * @since 1.0
 */
object MongoClient {

  /**
   * Create a default MongoClient at localhost:27017
   *
   * @return MongoClient
   */
  def apply(): MongoClient = apply("mongodb://localhost:27017")

  /**
   * Create a MongoClient instance from a connection string uri
   *
   * @param uri the connection string
   * @return MongoClient
   */
  def apply(uri: String): MongoClient = MongoClient(uri, None)

  /**
   * Create a MongoClient instance from a connection string uri
   *
   * @param uri the connection string
   * @param mongoDriverInformation any driver information to associate with the MongoClient
   * @return MongoClient
   * @note the `mongoDriverInformation` is intended for driver and library authors to associate extra driver metadata with the connections.
   */
  def apply(uri: String, mongoDriverInformation: Option[MongoDriverInformation]): MongoClient = {
    apply(
      MongoClientSettings
        .builder()
        .applyConnectionString(new ConnectionString(uri))
        .codecRegistry(DEFAULT_CODEC_REGISTRY)
        .build(),
      mongoDriverInformation
    )
  }

  /**
   * Create a MongoClient instance from the MongoClientSettings
   *
   * @param clientSettings MongoClientSettings to use for the MongoClient
   * @return MongoClient
   * @since 2.3
   */
  def apply(clientSettings: MongoClientSettings): MongoClient = MongoClient(clientSettings, None)

  /**
   * Create a MongoClient instance from the MongoClientSettings
   *
   * @param clientSettings MongoClientSettings to use for the MongoClient
   * @param mongoDriverInformation any driver information to associate with the MongoClient
   * @return MongoClient
   * @note the `mongoDriverInformation` is intended for driver and library authors to associate extra driver metadata with the connections.
   * @since 2.3
   */
  def apply(
      clientSettings: MongoClientSettings,
      mongoDriverInformation: Option[MongoDriverInformation]
  ): MongoClient = {
    val builder = mongoDriverInformation match {
      case Some(info) => MongoDriverInformation.builder(info)
      case None       => MongoDriverInformation.builder()
    }
    builder.driverName("scala").driverPlatform(s"Scala/${scala.util.Properties.versionString}")
    MongoClient(MongoClients.create(clientSettings, builder.build()))
  }

  val DEFAULT_CODEC_REGISTRY: CodecRegistry = fromRegistries(
    fromProviders(DocumentCodecProvider(), IterableCodecProvider()),
    com.mongodb.MongoClientSettings.getDefaultCodecRegistry
  )
}

/**
 * A client-side representation of a MongoDB cluster.  Instances can represent either a standalone MongoDB instance, a replica set,
 * or a sharded cluster.  Instance of this class are responsible for maintaining an up-to-date state of the cluster,
 * and possibly cache resources related to this, including background threads for monitoring, and connection pools.
 *
 * Instance of this class server as factories for [[MongoDatabase]] instances.
 *
 * @param wrapped the underlying java MongoClient
 * @since 1.0
 */
case class MongoClient(private val wrapped: JMongoClient) extends MongoCluster(wrapped) with Closeable {

  /**
   * Close the client, which will close all underlying cached resources, including, for example,
   * sockets and background monitoring threads.
   */
  def close(): Unit = wrapped.close()

  /**
   * Gets the current cluster description.
   *
   * This method will not block, meaning that it may return a `ClusterDescription` whose `clusterType` is unknown
   * and whose { @link com.mongodb.connection.ServerDescription}s are all in the connecting state.  If the application requires
   * notifications after the driver has connected to a member of the cluster, it should register a `ClusterListener` via
   * the `ClusterSettings` in `MongoClientSettings`.
   *
   * @return the current cluster description
   * @since 4.1
   */
  def getClusterDescription: ClusterDescription =
    wrapped.getClusterDescription
}
