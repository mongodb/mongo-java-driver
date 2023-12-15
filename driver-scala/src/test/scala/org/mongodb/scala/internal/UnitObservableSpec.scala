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

import org.mongodb.scala.{ BaseSpec, Observer, Subscription }

class UnitObservableSpec extends BaseSpec {
  it should "emit exactly one element" in {
    var onNextCounter = 0
    var errorActual: Option[Throwable] = None
    var completed = false
    UnitObservable(TestObservable(1 to 9)).subscribe(
      (_: Unit) => onNextCounter += 1,
      (error: Throwable) => errorActual = Some(error),
      () => completed = true
    )
    onNextCounter shouldBe 1
    errorActual shouldBe None
    completed shouldBe true
  }

  it should "signal the underlying error" in {
    var onNextCounter = 0
    val errorMessageExpected = "error message"
    var errorActual: Option[Throwable] = None
    var completed = false
    UnitObservable(TestObservable(1 to 9, failOn = 5, errorMessage = errorMessageExpected)).subscribe(
      (_: Unit) => onNextCounter += 1,
      (error: Throwable) => errorActual = Some(error),
      () => completed = true
    )
    onNextCounter shouldBe 0
    errorActual.map(e => e.getMessage) shouldBe Some(errorMessageExpected)
    completed shouldBe false
  }

  it should "work with explicit request" in {
    var onNextCounter = 0
    var errorActual: Option[Throwable] = None
    var completed = false
    UnitObservable(TestObservable(1 to 9)).subscribe(new Observer[Unit] {
      override def onSubscribe(subscription: Subscription): Unit = subscription.request(1)

      override def onNext(result: Unit): Unit = onNextCounter += 1

      override def onError(error: Throwable): Unit = errorActual = Some(error)

      override def onComplete(): Unit = completed = true
    })
    onNextCounter shouldBe 1
    errorActual shouldBe None
    completed shouldBe true
  }
}
