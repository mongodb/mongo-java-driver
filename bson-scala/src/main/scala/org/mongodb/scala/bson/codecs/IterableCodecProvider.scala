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

package org.mongodb.scala.bson.codecs

import org.bson.Transformer
import org.bson.codecs.Codec
import org.bson.codecs.configuration.{ CodecProvider, CodecRegistry }

/**
 * IterableCodecProvider companion object
 *
 * @since 1.2
 */
object IterableCodecProvider {

  /**
   * Create a `IterableCodecProvider` with the default `BsonTypeClassMap` and `Transformer`.
   * @return the new instance
   */
  def apply(): IterableCodecProvider = new IterableCodecProvider(BsonTypeClassMap(), None)

  /**
   * Create a `IterableCodecProvider` with the given `BsonTypeClassMap` and the default `Transformer`.
   *
   * @param bsonTypeClassMap the bson type class map
   * @return the new instance
   */
  def apply(bsonTypeClassMap: BsonTypeClassMap): IterableCodecProvider =
    new IterableCodecProvider(bsonTypeClassMap, None)

  /**
   * Create a `IterableCodecProvider` with the default `BsonTypeClassMap` and the given `Transformer`.
   *
   * @param valueTransformer the value transformer for decoded values
   * @return the new instance
   */
  def apply(valueTransformer: Transformer): IterableCodecProvider =
    new IterableCodecProvider(BsonTypeClassMap(), Option(valueTransformer))
}

/**
 * A `CodecProvider` for classes than implement the `Iterable` interface.
 *
 * @param bsonTypeClassMap the non-null `BsonTypeClassMap` with which to construct instances of `DocumentCodec` and `ListCodec`.
 * @param valueTransformer the value transformer for decoded values
 *
 * @since 1.2
 */
case class IterableCodecProvider(bsonTypeClassMap: BsonTypeClassMap, valueTransformer: Option[Transformer])
    extends CodecProvider {

  @SuppressWarnings(Array("unchecked"))
  def get[T](clazz: Class[T], registry: CodecRegistry): Codec[T] = {
    if (classOf[Iterable[_]].isAssignableFrom(clazz)) {
      IterableCodec(registry, bsonTypeClassMap, valueTransformer).asInstanceOf[Codec[T]]
    } else {
      null // scalastyle:ignore
    }
  }
}
