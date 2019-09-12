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

import com.mongodb.connection.netty.{NettyStreamFactoryFactory => JNettyStreamFactoryFactory}

/**
 * A `StreamFactoryFactory` implementation for <a href='http://netty.io/'>Netty</a>-based streams.
 *
 * @since 1.0
 */
object NettyStreamFactoryFactory {
  def apply(): StreamFactoryFactory = JNettyStreamFactoryFactory.builder().build()

  /**
   * Create a builder for Netty-based streams
   *
   * @return the builder
   * @since 2.2
   */
  def builder(): Builder = JNettyStreamFactoryFactory.builder()

  /**
   * NettyStreamFactoryFactory builder type
   */
  type Builder = JNettyStreamFactoryFactory.Builder
}
