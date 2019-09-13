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

class CollationCaseFirstSpec extends BaseSpec {

  "CollationCaseFirst" should "have the same static fields as the wrapped CollationCaseFirst" in {
    val collationCaseFirstClass: Class[CollationCaseFirst] = classOf[com.mongodb.client.model.CollationCaseFirst]
    val wrappedFields =
      collationCaseFirstClass.getDeclaredFields.filter(f => isStatic(f.getModifiers)).map(_.getName).toSet
    val wrappedMethods =
      collationCaseFirstClass.getDeclaredMethods.filter(f => isStatic(f.getModifiers)).map(_.getName).toSet
    val exclusions = Set("$VALUES", "valueOf", "values")

    val wrapped = (wrappedFields ++ wrappedMethods) -- exclusions
    val local = CollationCaseFirst.getClass.getDeclaredMethods.map(_.getName).toSet -- Set(
      "apply",
      "$deserializeLambda$",
      "$anonfun$fromString$1"
    )

    local should equal(wrapped)
  }

  it should "return the expected CollationCaseFirst" in {
    forAll(collationCaseFirsts) { (value: String, expectedValue: Try[CollationCaseFirst]) =>
      CollationCaseFirst.fromString(value) should equal(expectedValue)
    }
  }

  it should "handle invalid values" in {
    forAll(invalidCollationCaseFirsts) { (value: String) =>
      CollationCaseFirst.fromString(value) should be a Symbol("failure")
    }
  }

  val collationCaseFirsts =
    Table(
      ("stringValue", "JavaValue"),
      ("upper", Success(CollationCaseFirst.UPPER)),
      ("lower", Success(CollationCaseFirst.LOWER)),
      ("off", Success(CollationCaseFirst.OFF))
    )

  val invalidCollationCaseFirsts = Table("invalid values", "OFF", "LOWER")
}
