/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.mongodb.scala.syncadapter

import java.util.NoSuchElementException
import java.util.concurrent.{ CountDownLatch, LinkedBlockingDeque, TimeUnit }

import com.mongodb.{ MongoInterruptedException, MongoTimeoutException }
import com.mongodb.client.MongoCursor
import org.mongodb.scala.Observable
import org.reactivestreams.{ Subscriber, Subscription }

case class SyncMongoCursor[T](val observable: Observable[T]) extends MongoCursor[T] {
  val COMPLETED = new Object()

  private var subscription: Option[Subscription] = None
  private var nextResult: Option[T] = None
  private val results = new LinkedBlockingDeque[Any]

  val latch = new CountDownLatch(1)
  observable.subscribe(new Subscriber[T]() {
    def onSubscribe(s: Subscription): Unit = {
      subscription = Some(s)
      s.request(Long.MaxValue)
      latch.countDown()
    }

    def onNext(t: T): Unit = {
      results.addLast(t)
    }

    def onError(t: Throwable): Unit = {
      results.addLast(t)
    }

    def onComplete(): Unit = {
      results.addLast(COMPLETED)
    }
  })
  try {
    if (!latch.await(WAIT_DURATION.toSeconds, TimeUnit.SECONDS)) {
      throw new MongoTimeoutException("Timeout waiting for subscription")
    }
  } catch {
    case e: InterruptedException =>
      throw new MongoInterruptedException("Interrupted awaiting latch", e)
  }

  override def close(): Unit = {
    subscription.foreach(_.cancel())
    subscription = None
  }

  override def hasNext: Boolean = {
    if (nextResult.isDefined) {
      return true
    }
    val first = results.pollFirst(WAIT_DURATION.toSeconds, TimeUnit.SECONDS)
    first match {
      case n if n == null                 => throw new MongoTimeoutException("Time out!!!")
      case t if t.isInstanceOf[Throwable] => throw translateError(t.asInstanceOf[Throwable])
      case c if c == COMPLETED            => false
      case n => {
        nextResult = Some(n.asInstanceOf[T])
        true
      }
    }
  }

  override def next: T = {
    if (!hasNext) {
      throw new NoSuchElementException
    }
    val retVal = nextResult.get
    nextResult = None
    retVal
  }

  override def available(): Int = throw new UnsupportedOperationException

  override def remove(): Unit = throw new UnsupportedOperationException

  def tryNext = throw new UnsupportedOperationException // No good way to fulfill this contract with a Publisher<T>

  def getServerCursor = throw new UnsupportedOperationException

  def getServerAddress = throw new UnsupportedOperationException

  private def translateError(throwable: Throwable): RuntimeException = {
    throwable match {
      case exception: RuntimeException => exception
      case e                           => new RuntimeException(e)
    }
  }
}
