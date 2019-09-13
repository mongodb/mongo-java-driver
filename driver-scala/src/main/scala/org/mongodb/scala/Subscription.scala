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

import org.reactivestreams.{ Subscription => JSubscription }

/**
 * A `Subscription` represents a one-to-one lifecycle of a [[Observer]] subscribing to an [[Observable]].
 *
 * Instances can only be used once by a single [[Observer]].
 *
 * It is used to both signal desire for data and to allow for unsubscribing.
 */
trait Subscription extends JSubscription {

  /**
   * No operation will be sent to MongoDB from the [[Observable]] until demand is signaled via this method.
   *
   * It can be called however often and whenever needed, but the outstanding cumulative demand must never exceed `Long.MaxValue`.
   * An outstanding cumulative demand of `Long.MaxValue` may be treated by the [[Observable]] as "effectively unbounded".
   *
   * Whatever has been requested might be sent, so only signal demand for what can be safely handled.
   *
   * An [[Observable]] can send less than is requested if the stream ends but then must emit either
   * [[Observer.onError]] or [[Observer.onComplete]].
   *
   * @param n the strictly positive number of elements to requests to the upstream [[Observable]]
   */
  def request(n: Long): Unit

  /**
   * Request the [[Observable]] to stop sending data and clean up resources.
   *
   * As this request is asynchronous data may still be sent to meet previously signalled demand after calling cancel.
   */
  def unsubscribe(): Unit

  /**
   * Indicates whether this `Subscription` is currently unsubscribed.
   *
   * @return `true` if this `Subscription` is currently unsubscribed, `false` otherwise
   */
  def isUnsubscribed: Boolean

  /**
   * Request the [[Observable]] to stop sending data and clean up resources.
   *
   * As this request is asynchronous data may still be sent to meet previously signalled demand after calling cancel.
   */
  override def cancel(): Unit = unsubscribe()
}
