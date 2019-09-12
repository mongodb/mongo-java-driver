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

import scala.language.implicitConversions

/**
 * A magnet pattern implementation enforcing the validity of user provided native values being able to be converted into [[BsonValue]]s.
 *
 * @since 1.0
 */
protected[bson] object BsonMagnets {

  /**
   * Represents any single [[BsonValue]]
   *
   * This is a `BsonValue` or any type of `T` that has a [[BsonTransformer]] in scope for the given type.
   */
  sealed trait CanBeBsonValue {
    val value: BsonValue
  }

  /**
   * Implicitly converts type `T` to a [[BsonValue]] as long as there is a [[BsonTransformer]] in scope for the given type.
   *
   * @param v the initial value
   * @param transformer implicitly provided [[BsonTransformer]] that needs to be in scope for type `T` to be transformed into a [[BsonValue]]
   * @tparam T the type of the initial value
   * @return A CanBeBsonValue that holds the transformed [[BsonValue]]
   */
  implicit def singleToCanBeBsonValue[T](v: T)(implicit transformer: BsonTransformer[T]): CanBeBsonValue = {
    new CanBeBsonValue {
      override val value: BsonValue = transformer(v)
    }
  }

  /**
   * Represents a single [[BsonElement]]
   *
   * This is essentially a `(String, BsonValue)` key value pair. Any pair of `(String, T)` where type `T` has a [[BsonTransformer]] in
   * scope into a [[BsonValue]] is also a valid pair.
   */
  sealed trait CanBeBsonElement {
    val bsonElement: BsonElement

    /**
     * The key of the [[BsonElement]]
     * @return the key
     */
    def key: String = bsonElement.getName

    /**
     * The value of the [[BsonElement]]
     * @return the BsonValue
     */
    def value: BsonValue = bsonElement.getValue
  }

  /**
   * Implicitly converts key/value tuple of type (String, T) into a `CanBeBsonElement`
   *
   * @param kv the key value pair
   * @param transformer the implicit [[BsonTransformer]] for the value
   * @tparam T the type of the value
   * @return a CanBeBsonElement representing the key/value pair
   */
  implicit def tupleToCanBeBsonElement[T](kv: (String, T))(implicit transformer: BsonTransformer[T]): CanBeBsonElement = {
    new CanBeBsonElement {
      override val bsonElement: BsonElement = BsonElement(kv._1, transformer(kv._2))
    }
  }

  /**
   * Represents a sequence of [[BsonElement]]s
   *
   * This is essentially a `Iterable[(String, BsonValue)]` of key value pairs.  Any pair of `(String, T)` where type `T` has a
   * [[BsonTransformer]] in scope into a [[BsonValue]] is also a valid pair.
   */
  sealed trait CanBeBsonElements {
    /**
     * The `BsonElement` sequence
     */
    val values: Iterable[BsonElement]
  }

  /**
   * Implicitly converts any iterable of key/value pairs into a [[CanBeBsonElements]].
   *
   * @param elems the iterable of key/value pairs
   * @param transformer the implicit transformer for the values
   * @tparam T the type of the values
   * @return CanBeBsonElements representing the key/value pairs
   */
  implicit def iterableToCanBeBsonElements[T](elems: Iterable[(String, T)])(implicit transformer: BsonTransformer[T]): CanBeBsonElements =
    new CanBeBsonElements {
      override val values: Iterable[BsonElement] = elems.map(kv => BsonElement(kv._1, transformer(kv._2)))
    }

}
