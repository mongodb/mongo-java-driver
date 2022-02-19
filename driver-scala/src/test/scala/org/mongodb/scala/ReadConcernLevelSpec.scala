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

import java.lang.reflect.Modifier._

import scala.util.{ Success, Try }

import org.scalatest.prop.TableDrivenPropertyChecks._

class ReadConcernLevelSpec extends BaseSpec {

  "ReadConcernLevel" should "have the same static fields as the wrapped ReadConcern" in {
    val wrappedFields =
      classOf[com.mongodb.ReadConcernLevel].getDeclaredFields.filter(f => isStatic(f.getModifiers)).map(_.getName).toSet
    val wrappedMethods = classOf[com.mongodb.ReadConcernLevel].getDeclaredMethods
      .filter(f => isStatic(f.getModifiers))
      .map(_.getName)
      .toSet
    val exclusions = Set("$VALUES", "$values", "valueOf", "values")

    val wrapped = (wrappedFields ++ wrappedMethods) -- exclusions
    val local = ReadConcernLevel.getClass.getDeclaredMethods.map(_.getName).toSet -- Set(
      "apply",
      "$deserializeLambda$",
      "$anonfun$fromString$1"
    )

    local should equal(wrapped)
  }

  it should "return the expected ReadConcerns" in {
    forAll(readConcernLevels) { (stringValue: String, expectedValue: Try[ReadConcernLevel]) =>
      ReadConcernLevel.fromString(stringValue) should equal(expectedValue)
      ReadConcernLevel.fromString(stringValue.toUpperCase()) should equal(expectedValue)
    }
  }

  it should "handle invalid strings" in {
    forAll(invalidReadConcernLevels) { (stringValue: String) =>
      ReadConcernLevel.fromString(stringValue) should be a Symbol("failure")
    }
  }

  val readConcernLevels =
    Table(
      ("stringValue", "JavaValue"),
      ("local", Success(ReadConcernLevel.LOCAL)),
      ("linearizable", Success(ReadConcernLevel.LINEARIZABLE)),
      ("majority", Success(ReadConcernLevel.MAJORITY)),
      ("snapshot", Success(ReadConcernLevel.SNAPSHOT)),
      ("available", Success(ReadConcernLevel.AVAILABLE))
    )

  val invalidReadConcernLevels = Table("invalid strings", "all", "none")
}
