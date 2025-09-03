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

class CollationAlternateSpec extends BaseSpec {

  "CollationAlternate" should "have the same static fields as the wrapped CollationAlternate" in {
    val collationAlternateClass: Class[CollationAlternate] = classOf[com.mongodb.client.model.CollationAlternate]
    val wrappedFields =
      collationAlternateClass.getDeclaredFields.filter(f => isStatic(f.getModifiers)).map(_.getName).toSet
    val wrappedMethods =
      collationAlternateClass.getDeclaredMethods.filter(f => isStatic(f.getModifiers)).map(_.getName).toSet
    val exclusions = Set("$VALUES", "$values", "valueOf", "values")

    val wrapped = (wrappedFields ++ wrappedMethods) -- exclusions
    val local = CollationAlternate.getClass.getDeclaredMethods.map(_.getName).toSet -- Set(
      "apply",
      "$deserializeLambda$",
      "$anonfun$fromString$1"
    )

    local should equal(wrapped)
  }

  it should "return the expected CollationAlternate" in {
    forAll(collationAlternates) { (value: String, expectedValue: Try[CollationAlternate]) =>
      CollationAlternate.fromString(value) should equal(expectedValue)
    }
  }

  it should "handle invalid values" in {
    forAll(invalidCollationAlternates) { (value: String) =>
      CollationAlternate.fromString(value) should be a Symbol("failure")
    }
  }

  val collationAlternates =
    Table(
      ("stringValue", "JavaValue"),
      ("non-ignorable", Success(CollationAlternate.NON_IGNORABLE)),
      ("shifted", Success(CollationAlternate.SHIFTED))
    )

  val invalidCollationAlternates = Table("invalid values", "NON_IGNORABLE", "SHIFTED")
}
