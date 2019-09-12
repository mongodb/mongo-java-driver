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

import org.mongodb.scala.{Observable, Observer, Subscription}

private[scala] case class MapObservable[T, S](observable: Observable[T], s: T => S,
                                              f: Throwable => Throwable = t => t) extends Observable[S] {
  override def subscribe(observer: Observer[_ >: S]): Unit = {
    observable.subscribe(SubscriptionCheckingObserver(
      new Observer[T] {
        override def onError(throwable: Throwable): Unit = observer.onError(f(throwable))

        override def onSubscribe(subscription: Subscription): Unit = observer.onSubscribe(subscription)

        override def onComplete(): Unit = observer.onComplete()

        override def onNext(tResult: T): Unit = observer.onNext(s(tResult))
      }
    ))
  }
}
