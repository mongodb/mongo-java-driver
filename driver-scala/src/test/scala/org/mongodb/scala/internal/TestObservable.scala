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

import com.mongodb.MongoException
import org.mongodb.scala.{Observable, Observer, Subscription}

object TestObservable {

  def apply[A](from: Iterable[A]): TestObservable[A] = {
    new TestObservable(Observable[A](from))
  }

  def apply[A](from: Iterable[A], failOn: Int): TestObservable[A] = {
    new TestObservable(Observable[A](from), failOn)
  }

  def apply[A](from: Iterable[A], failOn: Int, errorMessage: String): TestObservable[A] = {
    new TestObservable(Observable[A](from), failOn, errorMessage)
  }
}

case class TestObservable[A](
    delegate:     Observable[A] = Observable[Int]((1 to 100).toStream),
    failOn:       Int           = Int.MaxValue,
    errorMessage: String        = "Failed"
) extends Observable[A] {

  override def subscribe(observer: Observer[_ >: A]): Unit = {
    delegate.subscribe(
      new Observer[A] {
        var failed = false
        var subscription: Option[Subscription] = None
        override def onError(throwable: Throwable): Unit = observer.onError(throwable)

        override def onSubscribe(sub: Subscription): Unit = {
          subscription = Some(sub)
          observer.onSubscribe(sub)
        }

        override def onComplete(): Unit = if (!failed) observer.onComplete()

        override def onNext(tResult: A): Unit = {
          if (!failed) {
            if (tResult == failOn) {
              failed = true
              onError(new MongoException(errorMessage))
            } else {
              observer.onNext(tResult)
            }
          }
        }
      }
    )
  }
}
