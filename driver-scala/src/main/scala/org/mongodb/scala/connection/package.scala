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

/**
 * The connection package contains classes that manage connecting to MongoDB servers.
 */
package object connection {

  /**
   * Settings for the cluster.
   */
  type ClusterSettings = com.mongodb.connection.ClusterSettings

  /**
   * All settings that relate to the pool of connections to a MongoDB server.
   */
  type ConnectionPoolSettings = com.mongodb.connection.ConnectionPoolSettings

  /**
   * Settings relating to monitoring of each server.
   */
  type ServerSettings = com.mongodb.connection.ServerSettings

  /**
   * An immutable class representing socket settings used for connections to a MongoDB server.
   */
  type SocketSettings = com.mongodb.connection.SocketSettings

  /**
   * This setting is only applicable when communicating with a MongoDB server using the synchronous variant of `MongoClient`.
   *
   * This setting is furthermore ignored if:
   * <ul>
   *    <li>the communication is via `com.mongodb.UnixServerAddress` (Unix domain socket).</li>
   *    <li>a `StreamFactoryFactory` is `MongoClientSettings.Builder.streamFactoryFactory` configured.</li>
   * </ul>
   *
   * @see [[org.mongodb.scala.connection.SocketSettings]]
   * @see [[org.mongodb.scala.AutoEncryptionSettings]]
   * @see [[org.mongodb.scala.ClientEncryptionSettings]]
   * @since 4.11
   */
  type ProxySettings = com.mongodb.connection.ProxySettings

  /**
   * Settings for connecting to MongoDB via SSL.
   */
  type SslSettings = com.mongodb.connection.SslSettings

  /**
   * Transport settings for the driver.
   *
   * @since 4.11
   */
  type TransportSettings = com.mongodb.connection.TransportSettings

  /**
   * TransportSettings for a Netty-based transport implementation.
   *
   * @since 4.11
   */
  type NettyTransportSettings = com.mongodb.connection.NettyTransportSettings

  /**
   * TransportSettings for an async transport implementation.
   *
   * @since 5.2
   */
  type AsyncTransportSettings = com.mongodb.connection.AsyncTransportSettings
}
