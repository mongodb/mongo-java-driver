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

import scala.util.{Success, Try}

class CollationStrengthSpec extends BaseSpec {

  "CollationStrength" should "have the same static fields as the wrapped CollationStrength" in {
    val collationStrengthClass: Class[CollationStrength] = classOf[com.mongodb.client.model.CollationStrength]
    val wrappedFields = collationStrengthClass.getDeclaredFields.filter(f => isStatic(f.getModifiers)).map(_.getName).toSet
    val wrappedMethods = collationStrengthClass.getDeclaredMethods.filter(f => isStatic(f.getModifiers)).map(_.getName).toSet
    val exclusions = Set("$VALUES", "valueOf", "values")

    val wrapped = (wrappedFields ++ wrappedMethods) -- exclusions
    val local = CollationStrength.getClass.getDeclaredMethods.map(_.getName).toSet -- Set("apply", "$deserializeLambda$", "$anonfun$fromInt$1")

    local should equal(wrapped)
  }

  it should "return the expected CollationStrength" in {
    forAll(collationStrengths) { (value: Int, expectedValue: Try[CollationStrength]) =>
      CollationStrength.fromInt(value) should equal(expectedValue)
    }
  }

  it should "handle invalid values" in {
    forAll(invalidCollationStrengths) { (value: Int) =>
      CollationStrength.fromInt(value) should be a Symbol("failure")
    }
  }

  val collationStrengths =
    Table(
      ("intValue", "JavaValue"),
      (1, Success(CollationStrength.PRIMARY)),
      (2, Success(CollationStrength.SECONDARY)),
      (3, Success(CollationStrength.TERTIARY)),
      (4, Success(CollationStrength.QUATERNARY)),
      (5, Success(CollationStrength.IDENTICAL))

    )

  val invalidCollationStrengths = Table("invalid values", 0, 6)
}
