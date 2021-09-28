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

import org.mongodb.scala.{ Observable, Observer, SingleObservable, Subscription }

private[scala] case class CollectObservable[T, S](observable: Observable[T], pf: PartialFunction[T, S])
    extends SingleObservable[S] {

  override def subscribe(observer: Observer[_ >: S]): Unit = {
    observable.subscribe(
      SubscriptionCheckingObserver(
        new Observer[T] {

          @volatile private var terminated: Boolean = false
          @volatile private var subscription: Option[Subscription] = None

          override def onError(throwable: Throwable): Unit = {
            terminated = true
            observer.onError(throwable)
          }

          override def onSubscribe(subscription: Subscription): Unit = {
            this.subscription = Some(subscription)
            observer.onSubscribe(subscription)
          }

          override def onComplete(): Unit = {
            terminated = true
            observer.onComplete()
          }

          override def onNext(tResult: T): Unit =
            if (pf.isDefinedAt(tResult)) {
              observer.onNext(pf.apply(tResult))
            } else if (!terminated) {
              subscription.foreach(_.request(1)) // No match, request more from down stream
            }
        }
      )
    )
  }
}
