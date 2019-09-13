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

import scala.util.{ Failure, Try }

import org.mongodb.scala.{ Observable, Observer, Subscription }

private[scala] case class AndThenObservable[T, U](observable: Observable[T], pf: PartialFunction[Try[T], U])
    extends Observable[T] {
  override def subscribe(observer: Observer[_ >: T]): Unit = {
    observable.subscribe(
      SubscriptionCheckingObserver[T](
        new Observer[T] {
          private var finalResult: Option[T] = None

          override def onError(throwable: Throwable): Unit = {
            observer.onError(throwable)
            Try(pf(Failure(throwable)))
          }

          override def onSubscribe(sub: Subscription): Unit = {
            observer.onSubscribe(sub)
          }

          override def onComplete(): Unit = {
            observer.onComplete()
            Try(pf(Try(finalResult.get)))
          }

          override def onNext(tResult: T): Unit = {
            finalResult = Some(tResult)
            observer.onNext(tResult)
          }
        }
      )
    )
  }
}
