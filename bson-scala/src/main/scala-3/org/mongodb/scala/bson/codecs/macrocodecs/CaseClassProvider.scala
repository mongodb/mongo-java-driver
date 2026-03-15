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

package org.mongodb.scala.bson.codecs.macrocodecs

import scala.quoted.*
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}

private[bson] object CaseClassProvider {

  def createCodecProviderEncodeNone[T: Type](using Quotes): Expr[CodecProvider] =
    createCodecProvider[T](Expr(true))

  def createCodecProviderIgnoreNone[T: Type](using Quotes): Expr[CodecProvider] =
    createCodecProvider[T](Expr(false))

  def createCodecProvider[T: Type](encodeNone: Expr[Boolean])(using Quotes): Expr[CodecProvider] = {
    import quotes.reflect.*
    val classOfT = Literal(ClassOfConstant(TypeRepr.of[T])).asExprOf[Class[?]]
      '{
        import org.bson.codecs.Codec
        import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}

        val _clazz = $classOfT
        new CodecProvider {
          @SuppressWarnings(Array("unchecked"))
          def get[C](clazz: Class[C], codecRegistry: CodecRegistry): Codec[C] = {
            if (_clazz.isAssignableFrom(clazz)) {
              ${ CaseClassCodec.createCodec[T]('codecRegistry, encodeNone) }.asInstanceOf[Codec[C]]
            } else {
              null
            }
          }
        }
      }
  }
}
