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

import org.mongodb.scala.internal.SingleItemObservable
import org.reactivestreams.Subscriber

/**
 * A companion object for [[SingleObservable]]
 *
 * @since 2.0
 */
object SingleObservable {

  /**
   * Creates an SingleObservable from an item.
   *
   * Convenient for testing and or debugging.
   *
   * @param item the item to create an observable from
   * @tparam A the type of the SingleObservable
   * @return an Observable that emits the item
   */
  def apply[A](item: A): SingleObservable[A] = SingleItemObservable(item)

}

/**
 * A `SingleObservable` represents an [[Observable]] that contains only a single item.
 *
 * @tparam T the type of element signaled.
 * @since 2.0
 */
trait SingleObservable[T] extends Observable[T] {

  /**
   * Request `SingleObservable` to start streaming data.
   *
   * This is a "factory method" and can be called multiple times, each time starting a new [[Subscription]].
   * Each `Subscription` will work for only a single [[Observer]].
   *
   * If the `Observable` rejects the subscription attempt or otherwise fails it will signal the error via [[Observer.onError]].
   *
   * @param observer the `Observer` that will consume signals from this `Observable`
   */
  def subscribe(observer: Observer[_ >: T]): Unit

  /**
   * Handles the automatic boxing of a Java `Observable` so it conforms to the interface.
   *
   * @note Users should not have to implement this method but rather use the Scala `Observable`.
   * @param observer the `Observer` that will consume signals from this `Observable`
   */
  override def subscribe(observer: Subscriber[_ >: T]): Unit = this.subscribe(BoxedSubscriber(observer))
}
