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

import java.util.concurrent.atomic.AtomicBoolean

import org.mongodb.scala.{Observable, Observer, SingleObservable, Subscription}

private[scala] case class FoldLeftObservable[T, S](observable: Observable[T], initialValue: S, accumulator: (S, T) => S) extends SingleObservable[S] {

  override def subscribe(observer: Observer[_ >: S]): Unit = {
    observable.subscribe(SubscriptionCheckingObserver(
      new Observer[T] {

        @volatile
        private var currentValue: S = initialValue

        private val requested = new AtomicBoolean(false)

        override def onError(throwable: Throwable): Unit = observer.onError(throwable)

        override def onSubscribe(subscription: Subscription): Unit = {
          val masterSub = new Subscription() {
            override def isUnsubscribed: Boolean = subscription.isUnsubscribed

            override def request(n: Long): Unit = {
              require(n > 0L, s"Number requested must be greater than zero: $n")
              if (requested.compareAndSet(false, true)) subscription.request(Long.MaxValue)
            }
            override def unsubscribe(): Unit = subscription.unsubscribe()
          }

          observer.onSubscribe(masterSub)
        }

        override def onComplete(): Unit = {
          observer.onNext(currentValue)
          observer.onComplete()
        }

        override def onNext(tResult: T): Unit = {
          currentValue = accumulator(currentValue, tResult)
        }
      }
    ))
  }
}
