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

import java.lang.reflect.Modifier._

import org.mongodb.scala.BaseSpec
import org.scalatest.prop.TableDrivenPropertyChecks._

import scala.util.{ Success, Try }

class ValidationActionSpec extends BaseSpec {

  "ValidationAction" should "have the same static fields as the wrapped ValidationAction" in {
    val ValidationActionClass: Class[ValidationAction] = classOf[com.mongodb.client.model.ValidationAction]
    val wrappedFields =
      ValidationActionClass.getDeclaredFields.filter(f => isStatic(f.getModifiers)).map(_.getName).toSet
    val wrappedMethods =
      ValidationActionClass.getDeclaredMethods.filter(f => isStatic(f.getModifiers)).map(_.getName).toSet
    val exclusions = Set("$VALUES", "valueOf", "values")

    val wrapped = (wrappedFields ++ wrappedMethods) -- exclusions
    val local = ValidationAction.getClass.getDeclaredMethods.map(_.getName).toSet -- Set(
      "apply",
      "$deserializeLambda$",
      "$anonfun$fromString$1"
    )

    local should equal(wrapped)
  }

  it should "return the expected ValidationActions" in {
    forAll(validationActions) { (stringValue: String, expectedValue: Try[ValidationAction]) =>
      ValidationAction.fromString(stringValue) should equal(expectedValue)
    }
  }

  it should "handle invalid strings" in {
    forAll(invalidValidationActions) { (stringValue: String) =>
      ValidationAction.fromString(stringValue) should be a Symbol("failure")
    }
  }

  val validationActions =
    Table(
      ("stringValue", "JavaValue"),
      ("error", Success(ValidationAction.ERROR)),
      ("ERROR", Success(ValidationAction.ERROR)),
      ("warn", Success(ValidationAction.WARN)),
      ("WARN", Success(ValidationAction.WARN))
    )

  val invalidValidationActions = Table("invalid strings", "all", "none")
}
