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

class ValidationLevelSpec extends BaseSpec {

  "ValidationLevel" should "have the same static fields as the wrapped ValidationLevel" in {
    val validationLevelClass: Class[ValidationLevel] = classOf[com.mongodb.client.model.ValidationLevel]
    val wrappedFields =
      validationLevelClass.getDeclaredFields.filter(f => isStatic(f.getModifiers)).map(_.getName).toSet
    val wrappedMethods =
      validationLevelClass.getDeclaredMethods.filter(f => isStatic(f.getModifiers)).map(_.getName).toSet
    val exclusions = Set("$VALUES", "$values", "valueOf", "values")

    val wrapped = (wrappedFields ++ wrappedMethods) -- exclusions
    val local = ValidationLevel.getClass.getDeclaredMethods.map(_.getName).toSet -- Set(
      "apply",
      "$deserializeLambda$",
      "$anonfun$fromString$1"
    )

    local should equal(wrapped)
  }

  it should "return the expected ValidationLevels" in {
    forAll(validationLevels) { (stringValue: String, expectedValue: Try[ValidationLevel]) =>
      ValidationLevel.fromString(stringValue) should equal(expectedValue)
    }
  }

  it should "handle invalid strings" in {
    forAll(invalidValidationLevels) { (stringValue: String) =>
      ValidationLevel.fromString(stringValue) should be a Symbol("failure")
    }
  }

  val validationLevels =
    Table(
      ("stringValue", "JavaValue"),
      ("off", Success(ValidationLevel.OFF)),
      ("OFF", Success(ValidationLevel.OFF)),
      ("strict", Success(ValidationLevel.STRICT)),
      ("STRICT", Success(ValidationLevel.STRICT)),
      ("OFF", Success(ValidationLevel.OFF)),
      ("moderate", Success(ValidationLevel.MODERATE)),
      ("MODERATE", Success(ValidationLevel.MODERATE))
    )

  val invalidValidationLevels = Table("invalid strings", "all", "none")
}
