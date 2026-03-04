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

import scala.annotation.compileTimeOnly
import scala.language.experimental.macros
import scala.language.implicitConversions

import org.bson.codecs.Codec
import org.bson.codecs.configuration.{ CodecProvider, CodecRegistry }

import org.mongodb.scala.bson.codecs.macrocodecs.{ CaseClassCodec, CaseClassProvider }

/**
 * Macro based Codecs
 *
 * Allows the compile time creation of Codecs for case classes.
 *
 * The recommended approach is to use the implicit [[Macros.createCodecProvider[T](clazz:Class[T])*]] method to help build a codecRegistry:
 * ```
 * import org.mongodb.scala.bson.codecs.Macros.createCodecProvider
 * import org.bson.codecs.configuration.CodecRegistries.{fromRegistries, fromProviders}
 *
 * case class Contact(phone: String)
 * case class User(_id: Int, username: String, age: Int, hobbies: List[String], contacts: List[Contact])
 *
 * val codecRegistry = fromRegistries(fromProviders(classOf[User], classOf[Contact]), MongoClient.DEFAULT_CODEC_REGISTRY)
 * ```
 *
 * @since 2.0
 */
object Macros {

  /**
   * Creates a CodecProvider for a case class.
   *
   * `Option[T]` fields set to `None` are encoded as BSON null. Use [[createCodecProviderIgnoreNone]] to omit them instead.
   *
   * @tparam T the case class to create a Codec from
   * @return the CodecProvider for the case class
   */
  inline def createCodecProvider[T](): CodecProvider =
  ${ CaseClassProvider.createCodecProviderEncodeNone[T] }

  /**
   * Creates a CodecProvider for a case class using the given class to represent the case class.
   *
   * `Option[T]` fields set to `None` are encoded as BSON null. Use [[createCodecProviderIgnoreNone]] to omit them instead.
   *
   * @param clazz the clazz that is the case class
   * @tparam T the case class to create a Codec from
   * @return the CodecProvider for the case class
   */
  inline implicit def createCodecProvider[T](clazz: Class[T]): CodecProvider =
  ${ CaseClassProvider.createCodecProviderEncodeNone[T] }

  /**
   * Creates a CodecProvider for a case class where `Option[T]` fields set to `None` are omitted from the BSON output
   * entirely (the field is not written). Use [[createCodecProvider]] to encode `None` as BSON null instead.
   *
   * @tparam T the case class to create a Codec from
   * @return the CodecProvider for the case class
   * @since 2.1
   */
  inline def createCodecProviderIgnoreNone[T](): CodecProvider =
  ${ CaseClassProvider.createCodecProviderIgnoreNone[T] }

  /**
   * Creates a CodecProvider for a case class where `Option[T]` fields set to `None` are omitted from the BSON output
   * entirely (the field is not written). Use [[createCodecProvider]] to encode `None` as BSON null instead.
   *
   * @param clazz the clazz that is the case class
   * @tparam T the case class to create a Codec from
   * @return the CodecProvider for the case class
   * @since 2.1
   */
  inline def createCodecProviderIgnoreNone[T](clazz: Class[T]): CodecProvider =
  ${ CaseClassProvider.createCodecProviderIgnoreNone[T] }

  /**
   * Creates a Codec for a case class using a default `CodecRegistry`.
   *
   * `Option[T]` fields set to `None` are encoded as BSON null. Use [[createCodecIgnoreNone]] to omit them instead.
   *
   * @tparam T the case class to create a Codec from
   * @return the Codec for the case class
   */
  inline def createCodec[T](): Codec[T] =
  ${ CaseClassCodec.createCodecBasicCodecRegistryEncodeNone[T] }

  /**
   * Creates a Codec for a case class using the provided `CodecRegistry`.
   *
   * `Option[T]` fields set to `None` are encoded as BSON null. Use [[createCodecIgnoreNone]] to omit them instead.
   *
   * @param codecRegistry the Codec Registry to use
   * @tparam T the case class to create a codec from
   * @return the Codec for the case class
   */
  inline def createCodec[T](codecRegistry: CodecRegistry): Codec[T] =
  ${ CaseClassCodec.createCodecEncodeNone[T]('codecRegistry) }

  /**
   * Creates a Codec for a case class using a default `CodecRegistry`, where `Option[T]` fields set to `None` are
   * omitted from the BSON output entirely. Use [[createCodec]] to encode `None` as BSON null instead.
   *
   * @tparam T the case class to create a Codec from
   * @return the Codec for the case class
   * @since 2.1
   */
  inline def createCodecIgnoreNone[T](): Codec[T] =
  ${ CaseClassCodec.createCodecBasicCodecRegistryIgnoreNone[T] }

  /**
   * Creates a Codec for a case class using the provided `CodecRegistry`, where `Option[T]` fields set to `None` are
   * omitted from the BSON output entirely. Use [[createCodec]] to encode `None` as BSON null instead.
   *
   * @param codecRegistry the Codec Registry to use
   * @tparam T the case class to create a codec from
   * @return the Codec for the case class
   * @since 2.1
   */
  inline def createCodecIgnoreNone[T](codecRegistry: CodecRegistry): Codec[T] =
  ${ CaseClassCodec.createCodecIgnoreNone[T]('codecRegistry) }

}
