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

package org.mongodb.scala

import org.reactivestreams.{ Subscription => JSubscription, Subscriber }

/**
 * A Scala based wrapper of the `Subscriber` interface which provides a mechanism for receiving push-based notifications.
 *
 * Will receive a call to `Observer.onSubscribe(subscription: Subscription)` on subscription to the [[Observable]].
 *
 * Default implementations of this trait are greedy and will call [[Subscription.request]] with `Long.MaxValue` so that all results are
 * requested.  Custom implementations of the `onSubscribe` method can be used to control "back-pressure" and ensure that only demand that
 * the `Observer` is capable of handling is requested.
 *
 * After signaling demand:
 *
 * - One or more invocations of [[Observer.onNext]] up to the maximum number defined by [[Subscription.request]]
 * - Single invocation of [[Observer.onError]] or [[Observer.onComplete]] which signals a terminal state after which no
 * further events will be sent.
 *
 * @tparam T The type of element signaled.
 */
trait Observer[T] extends Subscriber[T] {

  /**
   * Invoked on subscription to an [[Observable]].
   *
   * No operation will happen until [[Subscription.request]] is invoked.
   *
   * It is the responsibility of this Subscriber instance to call [[Subscription.request]] whenever more data is wanted.
   *
   * @param subscription [[Subscription]] that allows requesting data via [[Subscription.request]]
   */
  def onSubscribe(subscription: Subscription): Unit = subscription.request(Long.MaxValue)

  /**
   * Provides the Observer with a new item to observe.
   *
   * The Observer may call this method 0 or more times.
   *
   * The [[Observable]] will not call this method again after it calls either [[onComplete]] or
   * [[onError]].
   *
   * @param result the item emitted by the [[Observable]]
   */
  def onNext(result: T): Unit

  /**
   * Notifies the Observer that the [[Observable]] has experienced an error condition.
   *
   * If the [[Observable]] calls this method, it will not thereafter call [[onNext]] or [[onComplete]].
   *
   * @param e the exception encountered by the [[Observable]]
   */
  def onError(e: Throwable): Unit

  /**
   * Notifies the Subscriber that the [[Observable]] has finished sending push-based notifications.
   *
   * The [[Observable]] will not call this method if it calls [[onError]].
   */
  def onComplete(): Unit

  /**
   * Handles the automatic boxing of a Java subscription so it conforms to the interface.
   *
   * @note Users should not have to implement this method but rather use the Scala `Subscription`.
   * @param subscription the Java subscription
   */
  override def onSubscribe(subscription: JSubscription): Unit = onSubscribe(BoxedSubscription(subscription))
}
