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

import com.mongodb.client.model.{ ValidationAction => JValidationAction }

/**
 * Determines how strictly MongoDB applies the validation rules to existing documents during an insert or update.
 *
 * @note Requires MongoDB 3.2 or greater
 * @since 1.1
 */
object ValidationAction {

  /**
   * Documents must pass validation before the write occurs. Otherwise, the write operation fails.
   */
  val ERROR: ValidationAction = JValidationAction.ERROR

  /**
   * Documents do not have to pass validation. If the document fails validation, the write operation logs the validation failure to
   * the mongod logs.
   */
  val WARN: ValidationAction = JValidationAction.WARN

  /**
   * Returns the validationAction from the string representation of a validation action.
   *
   * @param validationAction the string representation of the validation action.
   * @return the validation action
   */
  def fromString(validationAction: String): Try[ValidationAction] = Try(JValidationAction.fromString(validationAction))
}
