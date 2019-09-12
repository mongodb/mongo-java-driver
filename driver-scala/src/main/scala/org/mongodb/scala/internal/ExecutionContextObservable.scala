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

import java.util.concurrent.atomic.AtomicInteger

import org.mongodb.scala.{Observable, Observer, Subscription}

import scala.concurrent.ExecutionContext

private[scala] case class ExecutionContextObservable[T](observable: Observable[T], context: ExecutionContext) extends Observable[T] {

  // scalastyle:off method.length
  override def subscribe(observer: Observer[_ >: T]): Unit = {
    observable.subscribe(SubscriptionCheckingObserver(
      new Observer[T] {
        private val referenceCount = new AtomicInteger(1)
        @volatile
        private var error: Option[Throwable] = None
        @volatile
        private var onCompleteCalled = false

        override def onSubscribe(subscription: Subscription): Unit = withContext(() => { observer.onSubscribe(subscription) })

        override def onNext(tResult: T): Unit = {
          referenceCount.incrementAndGet()
          withContext(() => {
            observer.onNext(tResult)
            checkTerminated()
          })
        }

        override def onError(throwable: Throwable): Unit = {
          error = Some(throwable)
          checkTerminated()
        }

        override def onComplete(): Unit = {
          onCompleteCalled = true
          checkTerminated()
        }

        def checkTerminated(decrement: Boolean = false): Unit = {
          val counter = referenceCount.decrementAndGet()
          if (counter == 0 && error.isDefined) {
            withContext(() => observer.onError(error.get))
          } else if (counter == 0 && onCompleteCalled) {
            withContext(() => observer.onComplete())
          }
        }

        private def withContext(f: () => Unit): Unit = {
          context.execute(new Runnable {
            override def run(): Unit = f()
          })
        }
      }
    ))
  }
}
