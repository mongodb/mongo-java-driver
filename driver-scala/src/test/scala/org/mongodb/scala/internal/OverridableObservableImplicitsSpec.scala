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

import scala.concurrent.duration.DurationInt
import org.mongodb.scala.{ BaseSpec, Observable }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ Await, Future }

object ObservableImplicitOverride {
  implicit class ObservableFuture[T](obs: => Observable[T]) {
    def toFuture(): Future[String] = Future("Overridden observable")
  }

}

class OverridableObservableImplicitsSpec extends BaseSpec {

  "Observable implicits" should "be overrideable" in {
    import ObservableImplicitOverride._

    val observable: Observable[Int] = Observable(1 to 10)

    Await.result(observable.toFuture(), 1.second) should equal("Overridden observable")
  }

  it should "also allow the default implementation to work" in {
    import org.mongodb.scala._
    val observable: Observable[Int] = Observable(1 to 10)

    Await.result(observable.toFuture(), 1.second) should equal((1 to 10).toList)

  }

}
