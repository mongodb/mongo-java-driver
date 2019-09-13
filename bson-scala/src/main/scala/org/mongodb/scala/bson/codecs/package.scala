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

package org.mongodb.scala.bson

import org.bson.codecs.{ BsonValueCodecProvider, ValueCodecProvider }
import org.bson.codecs.configuration.CodecRegistries.fromProviders
import org.bson.codecs.configuration.CodecRegistry

package object codecs {
  val DEFAULT_CODEC_REGISTRY: CodecRegistry = fromProviders(
    DocumentCodecProvider(),
    IterableCodecProvider(),
    new ValueCodecProvider(),
    new BsonValueCodecProvider()
  )

  /**
   * Type alias to the `BsonTypeClassMap`
   */
  type BsonTypeClassMap = org.bson.codecs.BsonTypeClassMap

  /**
   * Companion to return the default `BsonTypeClassMap`
   */
  object BsonTypeClassMap {
    def apply(): BsonTypeClassMap = new BsonTypeClassMap()
  }

  /**
   * Type alias to the `BsonTypeCodecMap`
   */
  type BsonTypeCodecMap = org.bson.codecs.BsonTypeCodecMap

}
