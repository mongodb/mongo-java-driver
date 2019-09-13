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

import java.util.concurrent.ConcurrentLinkedQueue

import org.mongodb.scala.{ Observable, Observer, Subscription }

private[scala] case class ZipObservable[T, U](
    observable1: Observable[T],
    observable2: Observable[U]
) extends Observable[(T, U)] {

  def subscribe(observer: Observer[_ >: (T, U)]): Unit = {
    val helper = SubscriptionHelper(observer)
    observable1.subscribe(SubscriptionCheckingObserver(helper.createFirstObserver))
    observable2.subscribe(SubscriptionCheckingObserver(helper.createSecondObserver))
  }

  case class SubscriptionHelper(observer: Observer[_ >: (T, U)]) {
    private val thisQueue: ConcurrentLinkedQueue[(Long, T)] = new ConcurrentLinkedQueue[(Long, T)]()
    private val thatQueue: ConcurrentLinkedQueue[(Long, U)] = new ConcurrentLinkedQueue[(Long, U)]()

    @volatile private var terminated: Boolean = false
    @volatile private var observable1Subscription: Option[Subscription] = None
    @volatile private var observable2Subscription: Option[Subscription] = None

    def createFirstObserver: Observer[T] = createSubObserver[T](thisQueue, observer, firstSub = true)

    def createSecondObserver: Observer[U] = createSubObserver[U](thatQueue, observer, firstSub = false)

    private def createSubObserver[A](
        queue: ConcurrentLinkedQueue[(Long, A)],
        observer: Observer[_ >: (T, U)],
        firstSub: Boolean
    ): Observer[A] = {
      new Observer[A] {
        @volatile private var counter: Long = 0
        override def onError(throwable: Throwable): Unit = {
          terminated = true
          observer.onError(throwable)
        }

        override def onSubscribe(subscription: Subscription): Unit = {
          if (firstSub) {
            observable1Subscription = Some(subscription)
          } else {
            observable2Subscription = Some(subscription)
          }

          if (observable1Subscription.nonEmpty && observable2Subscription.nonEmpty) {
            observer.onSubscribe(jointSubscription)
          }
        }

        override def onComplete(): Unit = {
          if (!firstSub) {
            terminated = true
            observer.onComplete()
          }
        }

        override def onNext(tResult: A): Unit = {
          counter += 1
          queue.add((counter, tResult))
          if (!firstSub) processNext(observer)
        }
      }
    }

    private def processNext(observer: Observer[_ >: (T, U)]): Unit = {
      (thisQueue.peek, thatQueue.peek) match {
        case ((k1: Long, _), (k2: Long, _)) if k1 == k2 => observer.onNext((thisQueue.poll()._2, thatQueue.poll()._2))
        case _ =>
          if (!terminated && !jointSubscription.isUnsubscribed) jointSubscription.request(1) // Uneven queues request more data
        // from downstream so to honor the original request for data.
      }
    }

    private val jointSubscription: Subscription = new Subscription() {
      var subscribed: Boolean = true
      override def isUnsubscribed: Boolean = !subscribed

      override def request(n: Long): Unit = {
        observable1Subscription.foreach(_.request(n))
        observable2Subscription.foreach(_.request(n))
      }

      override def unsubscribe(): Unit = {
        subscribed = false
        observable1Subscription.foreach(_.unsubscribe())
        observable2Subscription.foreach(_.unsubscribe())
      }
    }
  }

}
