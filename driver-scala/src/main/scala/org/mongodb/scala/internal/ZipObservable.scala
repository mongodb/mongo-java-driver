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
import java.util.concurrent.atomic.AtomicLong

import org.mongodb.scala.{ Observable, Observer, Subscription }

private[scala] case class ZipObservable[L, R](
    leftObservable: Observable[L],
    rightObservable: Observable[R]
) extends Observable[(L, R)] {

  def subscribe(observer: Observer[_ >: (L, R)]): Unit = {
    val helper = SubscriptionHelper(observer)
    leftObservable.subscribe(SubscriptionCheckingObserver(helper.createLeftObserver))
    rightObservable.subscribe(SubscriptionCheckingObserver(helper.createRightObserver))
  }

  case class SubscriptionHelper(observer: Observer[_ >: (L, R)]) {
    private val leftQueue: ConcurrentLinkedQueue[(Long, L)] = new ConcurrentLinkedQueue[(Long, L)]()
    private val rightQueue: ConcurrentLinkedQueue[(Long, R)] = new ConcurrentLinkedQueue[(Long, R)]()

    private val leftCounter: AtomicLong = new AtomicLong()
    private val rightCounter: AtomicLong = new AtomicLong()
    @volatile private var completedLeft: Boolean = false
    @volatile private var completedRight: Boolean = false
    @volatile private var terminated: Boolean = false
    @volatile private var leftSubscription: Option[Subscription] = None
    @volatile private var rightSubscription: Option[Subscription] = None

    def createLeftObserver: Observer[L] = createSubObserver[L](leftQueue, observer, isLeftSub = true)
    def createRightObserver: Observer[R] = createSubObserver[R](rightQueue, observer, isLeftSub = false)

    private def createSubObserver[A](
        queue: ConcurrentLinkedQueue[(Long, A)],
        observer: Observer[_ >: (L, R)],
        isLeftSub: Boolean
    ): Observer[A] = {
      new Observer[A] {
        @volatile private var counter: Long = 0
        override def onError(throwable: Throwable): Unit = {
          terminated = true
          observer.onError(throwable)
        }

        override def onSubscribe(subscription: Subscription): Unit = {
          if (isLeftSub) {
            leftSubscription = Some(subscription)
          } else {
            rightSubscription = Some(subscription)
          }

          if (leftSubscription.nonEmpty && rightSubscription.nonEmpty) {
            observer.onSubscribe(jointSubscription)
          }
        }

        override def onComplete(): Unit = {
          markCompleted(isLeftSub)
          processNext(observer)
        }

        override def onNext(tResult: A): Unit = {
          if (isLeftSub) leftCounter.incrementAndGet() else rightCounter.incrementAndGet()
          counter += 1
          queue.add((counter, tResult))
          processNext(observer)
        }
      }
    }

    private def markCompleted(isLeftSub: Boolean): Unit = synchronized {
      if (isLeftSub) {
        completedLeft = true
      } else {
        completedRight = true
      }
    }

    private def completed(): Unit = synchronized {
      if (!terminated) {
        terminated = true
        leftSubscription.foreach(_.unsubscribe())
        rightSubscription.foreach(_.unsubscribe())
        observer.onComplete()
      }
    }

    private def processNext(observer: Observer[_ >: (L, R)]): Unit = synchronized {
      (leftQueue.peek, rightQueue.peek) match {
        case ((k1: Long, _), (k2: Long, _)) if k1 == k2 =>
          observer.onNext((leftQueue.poll()._2, rightQueue.poll()._2))
          processNext(observer)
        case _ =>
          if (!terminated && !jointSubscription.isUnsubscribed) {
            if (completedLeft && rightCounter.get() >= leftCounter.get()) {
              completed()
            } else if (completedRight && leftCounter.get() >= rightCounter.get()) {
              completed()
            }
          }
      }
    }

    private val jointSubscription: Subscription = new Subscription() {
      var subscribed: Boolean = true
      override def isUnsubscribed: Boolean = !subscribed

      override def request(n: Long): Unit = {
        leftSubscription.foreach(_.request(n))
        rightSubscription.foreach(_.request(n))
      }

      override def unsubscribe(): Unit = {
        subscribed = false
        leftSubscription.foreach(_.unsubscribe())
        rightSubscription.foreach(_.unsubscribe())
      }
    }
  }

}
