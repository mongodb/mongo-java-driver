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
 * An immutable class representing settings for connecting to MongoDB via a SOCKS5 proxy server.
 * NOTE: This setting is only applicable to the synchronous variant of MongoClient and Key Management Service settings.
 *
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
