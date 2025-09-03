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

package org.mongodb.scala.internal

import org.mongodb.scala.{ BaseSpec, Observable }
import org.scalatest.concurrent.ScalaFutures

class CollectObservableTest extends BaseSpec with ScalaFutures {
  "CollectObservable" should "apply a partial function to the elements in the Observable" in {
    val justStrings = Observable(Iterable("this", 1, 2, "that"))
      .collect { case s: String => s }
      .toFuture()
      .futureValue

    assert(justStrings === Seq("this", "that"))
  }
}
