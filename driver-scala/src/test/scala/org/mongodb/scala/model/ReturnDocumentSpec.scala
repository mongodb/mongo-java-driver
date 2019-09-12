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

import com.mongodb.client.model.{ReturnDocument => JReturnDocument}
import org.mongodb.scala.BaseSpec

class ReturnDocumentSpec extends BaseSpec {

  "ReturnDocument" should "mirror com.mongodb.client.model.ReturnDocument" in {
    val wrapped = classOf[JReturnDocument].getEnumConstants.map(_.toString).toSet
    val local = ReturnDocument.getClass.getDeclaredMethods.filter(f => isPublic(f.getModifiers)).map(_.getName).toSet

    local should equal(wrapped)
  }

  it should "have the same values" in {
    ReturnDocument.BEFORE should equal(JReturnDocument.BEFORE)

    ReturnDocument.AFTER should equal(JReturnDocument.AFTER)
  }

}
