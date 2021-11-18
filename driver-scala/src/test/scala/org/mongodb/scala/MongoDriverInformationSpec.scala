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

import java.lang.reflect.Modifier.isStatic

import org.scalatest.{ FlatSpec, Matchers }

class MongoDriverInformationSpec extends BaseSpec {

  "MongoDriverInformation" should "have the same static fields as the wrapped MongoDriverInformation" in {
    val MongoDriverInformationClass: Class[MongoDriverInformation] = classOf[com.mongodb.MongoDriverInformation]
    val wrappedFields =
      MongoDriverInformationClass.getDeclaredFields.filter(f => isStatic(f.getModifiers)).map(_.getName).toSet
    val wrappedMethods =
      MongoDriverInformationClass.getDeclaredMethods.filter(f => isStatic(f.getModifiers)).map(_.getName).toSet
    val exclusions = Set("$VALUES", "$values", "valueOf", "values")

    val wrapped = (wrappedFields ++ wrappedMethods) -- exclusions
    val local = MongoDriverInformation.getClass.getDeclaredMethods.map(_.getName).toSet -- Set(
      "apply",
      "$deserializeLambda$",
      "$anonfun$fromString$1"
    )

    local should equal(wrapped)
  }

  it should "return the underlying builder" in {
    MongoDriverInformation.builder().getClass should equal(classOf[com.mongodb.MongoDriverInformation.Builder])
    MongoDriverInformation.builder(MongoDriverInformation.builder().build()).getClass should equal(
      classOf[com.mongodb.MongoDriverInformation.Builder]
    )
  }

}
