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

import com.mongodb.connection.{ SocketSettings => JSocketSettings }

/**
 * An immutable class representing socket settings used for connections to a MongoDB server.
 *
 * @since 1.0
 */
object SocketSettings {

  /**
   * Creates a builder for SocketSettings.
   *
   * @return a new Builder for creating SocketSettings.
   */
  def builder(): Builder = JSocketSettings.builder()

  /**
   * SocketSettings builder type
   */
  type Builder = JSocketSettings.Builder

}
