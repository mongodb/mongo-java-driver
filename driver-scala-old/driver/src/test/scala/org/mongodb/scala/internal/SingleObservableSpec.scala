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

import org.mongodb.scala.{Observer, SingleObservable, Subscription}
import org.scalatest.{FlatSpec, Matchers}

class SingleObservableSpec extends FlatSpec with Matchers {

  "ScalaObservable" should "allow for inline subscription" in {
    var result = 0
    observable().subscribe((res: Int) => result = res)
    result should equal(42)

    var thrown = false
    failedObservable().subscribe((res: Int) => (), (t: Throwable) => thrown = true)
    thrown should equal(true)

    var completed = false
    observable().subscribe((res: Int) => (), (t: Throwable) => (), () => completed = true)
    completed should equal(true)
  }

  def observable(): SingleObservable[Int] = SingleObservable(42)

  def failedObservable(): SingleObservable[Int] = new SingleObservable[Int] {
    override def subscribe(observer: Observer[_ >: Int]) = {
      observer.onSubscribe(new Subscription {
        override def isUnsubscribed: Boolean = false
        override def request(n: Long): Unit = observer.onError(new Exception("Failed"))
        override def unsubscribe(): Unit = {}
      })
    }
  }
}
