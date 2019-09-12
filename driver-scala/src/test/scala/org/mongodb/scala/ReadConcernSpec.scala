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

import com.mongodb.{ReadConcern => JReadConcern}

import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.{FlatSpec, Matchers}

class ReadConcernSpec extends BaseSpec {

  "ReadConcern" should "have the same static fields as the wrapped ReadConcern" in {
    val wrapped = classOf[com.mongodb.ReadConcern].getDeclaredFields.filter(f => isStatic(f.getModifiers)).map(_.getName).toSet
    val local = ReadConcern.getClass.getDeclaredMethods.map(_.getName).toSet -- Set("apply", "$deserializeLambda$", "$anonfun$fromString$1")

    local should equal(wrapped)
  }

  it should "return the expected ReadConcerns" in {
    forAll(readConcerns) { (scalaValue: ReadConcern, javaValue: JReadConcern) =>
      scalaValue should equal(javaValue)
    }
  }

  val readConcerns =
    Table(
      ("ScalaValue", "JavaValue"),
      (ReadConcern.DEFAULT, JReadConcern.DEFAULT),
      (ReadConcern.LOCAL, JReadConcern.LOCAL),
      (ReadConcern.LINEARIZABLE, JReadConcern.LINEARIZABLE),
      (ReadConcern.MAJORITY, JReadConcern.MAJORITY),
      (ReadConcern(ReadConcernLevel.LOCAL), JReadConcern.LOCAL)
    )
}
