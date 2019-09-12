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

import org.mongodb.scala.{Observer, Subscription}

private[scala] case class SubscriptionCheckingObserver[T](wrapped: Observer[T]) extends Observer[T] {
  @volatile
  private var subscription: Option[Subscription] = None

  override def onSubscribe(sub: Subscription): Unit = {
    check(subscription.isEmpty, "The Observable has already been subscribed to.")
    subscription = Some(sub)
    wrapped.onSubscribe(sub)
  }

  override def onNext(result: T): Unit = {
    check(subscription.isDefined, "The Observable has not been subscribed to.")
    wrapped.onNext(result)
  }

  override def onError(e: Throwable): Unit = {
    check(subscription.isDefined, "The Observable has not been subscribed to.")
    wrapped.onError(e)
  }

  override def onComplete(): Unit = {
    check(subscription.isDefined, "The Observable has not been subscribed to.")
    wrapped.onComplete()
  }

  private def check(requirement: Boolean, message: String): Unit = {
    if (!requirement) throw new IllegalStateException(message)
  }
}
