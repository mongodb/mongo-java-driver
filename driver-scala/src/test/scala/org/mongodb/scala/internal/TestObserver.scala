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

import scala.collection.mutable

import org.mongodb.scala.{ Observer, Subscription }

object TestObserver {

  def apply[A](): TestObserver[A] = {
    new TestObserver[A](new Observer[A] {
      override def onError(throwable: Throwable): Unit = {}

      override def onSubscribe(subscription: Subscription): Unit = {}

      override def onComplete(): Unit = {}

      override def onNext(tResult: A): Unit = {}
    })
  }

}

case class TestObserver[A](delegate: Observer[A]) extends Observer[A] {
  @volatile var subscription: Option[OneAtATimeSubscription] = None
  @volatile var error: Option[Throwable] = None
  @volatile var completed: Boolean = false
  @volatile var terminated: Boolean = false
  val results: mutable.ListBuffer[A] = mutable.ListBuffer[A]()

  override def onError(throwable: Throwable): Unit = {
    require(!terminated, "onError called after the observer has already been terminated")
    terminated = true
    error = Some(throwable)
    delegate.onError(throwable)
  }

  override def onSubscribe(sub: Subscription): Unit = {
    require(subscription.isEmpty, "observer already subscribed to")
    val oneAtATimeSubscription = OneAtATimeSubscription(sub)
    subscription = Some(oneAtATimeSubscription)
    delegate.onSubscribe(oneAtATimeSubscription)
  }

  override def onComplete(): Unit = {
    require(!terminated, "onComplete called after the observer has already been terminated")
    terminated = true
    delegate.onComplete()
    completed = true
  }

  override def onNext(result: A): Unit = {
    require(!terminated, "onNext called after the observer has already been terminated")
    this.synchronized {
      results.append(result)
    }
    delegate.onNext(result)
    subscription.foreach(_.innerRequestNext())
  }

  case class OneAtATimeSubscription(inner: Subscription) extends Subscription {

    @volatile var demand: Long = 0

    override def request(n: Long): Unit = {
      require(n > 0L, s"Number requested must be greater than zero: $n")
      addDemand(n)
      innerRequestNext()
    }

    override def unsubscribe(): Unit = inner.unsubscribe()

    override def isUnsubscribed: Boolean = inner.isUnsubscribed

    def innerRequestNext(): Unit = {
      if (!terminated && !isUnsubscribed && addDemand(-1) > 0) {
        inner.request(1)
      }
    }

    private def addDemand(extraDemand: Long): Long = {
      this.synchronized {
        demand += extraDemand
        if (demand > 0) {
          demand
        } else if (demand < 0 && extraDemand > 0) {
          demand = Long.MaxValue
          demand
        } else {
          0
        }
      }
    }
  }
}
