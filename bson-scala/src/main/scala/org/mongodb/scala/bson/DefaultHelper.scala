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

/**
 * A helper containing the sealed `DefaultsTo` trait which is used to determine the default type for a given method.
 *
 * @since 1.0
 */
protected[scala] object DefaultHelper {

  /**
   * Neat helper to obtain a default type should one not be given eg:
   *
   * {{{
   *   def find[T]()(implicit e: T DefaultsTo Document) { ... }
   * }}}
   *
   * The signature of the `find` method ensures that it can only be called if the caller can supply an object of type
   * `DefaultsTo[T, Document]`. Of course, the [[DefaultsTo.default]] and `[[DefaultsTo.overrideDefault]] methods make it easy to create
   * such an object for any type `T`.  Since these methods are implicit, the compiler automatically handles the business of calling one of
   * them and passing the result into `find`.
   *
   * ''But how does the compiler know which method to call?'' It uses its type inference and implicit resolution rules to determine the
   * appropriate method. There are three cases to consider:
   *
   * 1. `find` is called with no type parameter. In this case, type T must be inferred. Searching for an implicit method that can provide
   * an object of type `DefaultsTo[T, Document]`, the compiler finds `default` and `overrideDefault`. `default` is chosen since it has
   * priority (because it's defined in a proper subclass of the trait that defines overrideDefault). As a result, T must be bound to
   * `Document.
   *
   * 2. `find`  is called with a non-Document type parameter (e.g., `find[BsonDocument]()`). In this case, an object of type
   * `DefaultsTo[BsonDocument, Document]` must be supplied. Only the `overrideDefault` method can supply it, so the compiler inserts the
   * appropriate call.
   *
   * 3. `find` is called with `Document` as the type parameter. Again, either method is applicable, but default wins due to its higher
   * priority.
   *
   */
  sealed class DefaultsTo[A, B]

  /**
   * Companion object for [[DefaultsTo]]
   */
  object DefaultsTo extends LowPriorityDefaultsTo {
    /**
     * Implicitly sets a default type of B. See [[DefaultsTo]]
     *
     * @tparam B the default type
     * @return Defaults[B, B] instance
     */
    implicit def default[B]: DefaultsTo[B, B] = new DefaultsTo[B, B]
  }

  /**
   * Lower priority defaultsTo implicit helper
   */
  trait LowPriorityDefaultsTo {

    /**
     * Overrides the default with the set type of A.  See [[DefaultsTo]]
     *
     * @tparam A The type to use
     * @tparam B The default type incase type A was missing
     * @return Defaults[A, B] instance
     */
    implicit def overrideDefault[A, B]: DefaultsTo[A, B] = new DefaultsTo[A, B]
  }
}
