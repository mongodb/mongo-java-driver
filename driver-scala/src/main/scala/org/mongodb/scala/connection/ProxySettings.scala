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

package org.mongodb.scala.connection

import com.mongodb.connection.{ ProxySettings => JProxySettings }

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
object ProxySettings {

  /**
   * Creates a builder for ProxySettings.
   *
   * @return a new Builder for creating ProxySettings.
   */
  def builder(): Builder = JProxySettings.builder()

  /**
   * ProxySettings builder type
   */
  type Builder = JProxySettings.Builder

}
