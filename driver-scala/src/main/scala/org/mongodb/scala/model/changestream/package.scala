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

package org.mongodb.scala.model

package object changestream {

  /**
   * Represents the `\$changeStream` aggregation output document.
   *
   * '''Note:''' this class will not be applicable for all change stream outputs. If using custom pipelines that radically change the
   * change stream result, then an alternative document format should be used.
   *
   * @tparam T  The type that this collection will encode the `fullDocument` field into.
   */
  type ChangeStreamDocument[T] = com.mongodb.client.model.changestream.ChangeStreamDocument[T]

  /**
   * Change Stream fullDocument configuration.
   */
  type FullDocument = com.mongodb.client.model.changestream.FullDocument

  object F
}
