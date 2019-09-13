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

import scala.util.Try

import com.mongodb.client.model.{ ValidationLevel => JValidationLevel }

/**
 * Determines how strictly MongoDB applies the validation rules to existing documents during an insert or update.
 *
 * @note Requires MongoDB 3.2 or greater
 * @since 1.1
 */
object ValidationLevel {

  /**
   * No validation for inserts or updates.
   */
  val OFF: ValidationLevel = JValidationLevel.OFF

  /**
   * Apply validation rules to all inserts and all updates.
   */
  val STRICT: ValidationLevel = JValidationLevel.STRICT

  /**
   * Applies validation rules to inserts and to updates on existing valid documents.
   *
   * Does not apply rules to updates on existing invalid documents.
   */
  val MODERATE: ValidationLevel = JValidationLevel.MODERATE

  /**
   * Returns the ValidationLevel from the string representation of the validation level.
   *
   * @param validationLevel the string representation of the validation level.
   * @return the validation level
   */
  def fromString(validationLevel: String): Try[ValidationLevel] = Try(JValidationLevel.fromString(validationLevel))
}
