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

import org.mongodb.scala.{ Observable, Observer, Subscription }

private[scala] case class IterableObservable[A](from: Iterable[A]) extends Observable[A] {

  override def subscribe(observer: Observer[_ >: A]): Unit = {
    observer.onSubscribe(
      new Subscription {
        @volatile
        private var left: Iterable[A] = from
        @volatile
        private var subscribed: Boolean = true
        @volatile
        private var completed: Boolean = false

        override def isUnsubscribed: Boolean = !subscribed

        override def request(n: Long): Unit = {
          require(n > 0L, s"Number requested must be greater than zero: $n")
          var counter = n
          while (!completed && subscribed && counter > 0 && left.nonEmpty) {
            val head = left.head
            left = left.tail
            observer.onNext(head)
            counter -= 1
          }

          if (!completed && subscribed && left.isEmpty) {
            completed = true
            observer.onComplete()
          }
        }

        override def unsubscribe(): Unit = subscribed = false
      }
    )
  }
}
