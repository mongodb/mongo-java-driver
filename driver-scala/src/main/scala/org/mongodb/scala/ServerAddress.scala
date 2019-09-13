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

import java.net.{ InetAddress, InetSocketAddress }

import com.mongodb.{ ServerAddress => JServerAddress }

/**
 * Represents the location of a MongoDB server - i.e. server name and port number
 *
 * @since 1.0
 */
object ServerAddress {

  /**
   * Creates a ServerAddress with default host and port
   */
  def apply(): JServerAddress = new JServerAddress()

  /**
   * Creates a ServerAddress with default port
   *
   * @param host hostname
   */
  def apply(host: String): JServerAddress = new JServerAddress(host)

  /**
   * Creates a ServerAddress with default port
   *
   * @param inetAddress host address
   */
  def apply(inetAddress: InetAddress): JServerAddress = new JServerAddress(inetAddress)

  /**
   * Creates a ServerAddress
   *
   * @param inetAddress host address
   * @param port        mongod port
   */
  def apply(inetAddress: InetAddress, port: Int): JServerAddress = new JServerAddress(inetAddress, port)

  /**
   * Creates a ServerAddress
   *
   * @param inetSocketAddress inet socket address containing hostname and port
   */
  def apply(inetSocketAddress: InetSocketAddress): JServerAddress = new JServerAddress(inetSocketAddress)

  /**
   * Creates a ServerAddress
   *
   * @param host hostname
   * @param port mongod port
   */
  def apply(host: String, port: Int): JServerAddress = new JServerAddress(host, port)

}
