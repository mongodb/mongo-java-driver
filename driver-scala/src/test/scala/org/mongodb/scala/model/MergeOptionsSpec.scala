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

import com.mongodb.client.model.{MergeOptions => JMergeOptions}
import org.mongodb.scala.BaseSpec

class MergeOptionsSpec extends BaseSpec {

  case class Default(wrapped: String = "")

  "MergeOptions" should "mirror com.mongodb.client.model.MergeOptions" in {
    val setters = classOf[JMergeOptions].getDeclaredMethods.filter(f => isPublic(f.getModifiers) && !f.getName.startsWith("get")).map(_.getName).toSet
    val enums = classOf[JMergeOptions].getDeclaredFields.map(_.getName).toSet
    val wrapped = (setters ++ enums) -- Set("hashCode", "toString", "equals")

    val exclusions = Default().getClass.getDeclaredMethods.filter(f => isPublic(f.getModifiers)).map(_.getName).toSet ++ Set("apply", "unapply")
    val local = MergeOptions().getClass.getDeclaredMethods.filter(f => isPublic(f.getModifiers) && !f.getName.contains("$"))
      .map(_.getName).toSet -- exclusions

    local should equal(wrapped)
  }

  it should "have the same values for WhenMatched" in {
    val wrapped = classOf[JMergeOptions.WhenMatched].getEnumConstants.map(_.toString).toSet
    val local = MergeOptions.WhenMatched.getClass.getDeclaredMethods.filter(f => isPublic(f.getModifiers)).map(_.getName).toSet

    local should equal(wrapped)

    MergeOptions.WhenMatched.FAIL should equal(JMergeOptions.WhenMatched.FAIL)
    MergeOptions.WhenMatched.KEEP_EXISTING should equal(JMergeOptions.WhenMatched.KEEP_EXISTING)
    MergeOptions.WhenMatched.MERGE should equal(JMergeOptions.WhenMatched.MERGE)
    MergeOptions.WhenMatched.PIPELINE should equal(JMergeOptions.WhenMatched.PIPELINE)
    MergeOptions.WhenMatched.REPLACE should equal(JMergeOptions.WhenMatched.REPLACE)

    wrapped.size should equal(5)
  }

  it should "have the same values for WhenNotMatched" in {
    val wrapped = classOf[JMergeOptions.WhenNotMatched].getEnumConstants.map(_.toString).toSet
    val local = MergeOptions.WhenNotMatched.getClass.getDeclaredMethods.filter(f => isPublic(f.getModifiers)).map(_.getName).toSet

    local should equal(wrapped)

    MergeOptions.WhenNotMatched.DISCARD should equal(JMergeOptions.WhenNotMatched.DISCARD)
    MergeOptions.WhenNotMatched.FAIL should equal(JMergeOptions.WhenNotMatched.FAIL)
    MergeOptions.WhenNotMatched.INSERT should equal(JMergeOptions.WhenNotMatched.INSERT)

    wrapped.size should equal(3)
  }

}
