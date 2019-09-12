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

/**
 * Result based types
 *
 * @since 1.0
 */
package object result {

  /**
   * The result of a delete operation. If the delete was unacknowledged, then `wasAcknowledged` will return false and all other methods
   * with throw an `UnsupportedOperationException`.
   */
  type DeleteResult = com.mongodb.client.result.DeleteResult

  /**
   * The result of an update operation. If the update was unacknowledged, then `wasAcknowledged` will return false and all other methods
   * with throw an `UnsupportedOperationException`.
   */
  type UpdateResult = com.mongodb.client.result.UpdateResult

}
