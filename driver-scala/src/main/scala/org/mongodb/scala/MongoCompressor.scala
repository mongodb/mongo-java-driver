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

import com.mongodb.{ MongoCompressor => JMongoCompressor }

/**
 * Metadata describing a compressor to use for sending and receiving messages to a MongoDB server.
 *
 * @since 2.2
 * @note Requires MongoDB 3.4 or greater
 */
object MongoCompressor {

  /**
   * Create an instance for snappy compression.
   *
   * @return A compressor based on the snappy compression algorithm
   */
  def createSnappyCompressor: MongoCompressor = JMongoCompressor.createSnappyCompressor()

  /**
   * Create an instance for zlib compression.
   *
   * @return A compressor based on the zlib compression algorithm
   * @note Requires MongoDB 3.6 or greater
   */
  def createZlibCompressor: MongoCompressor = JMongoCompressor.createZlibCompressor()

  /**
   * Create an instance for zstd compression.
   *
   * @return A compressor based on the zstd compression algorithm
   * @note Requires MongoDB 4.2 or greater
   * @since 4.0
   */
  def createZstdCompressor: MongoCompressor = JMongoCompressor.createZstdCompressor()
}
