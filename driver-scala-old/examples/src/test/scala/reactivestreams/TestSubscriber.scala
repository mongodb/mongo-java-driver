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

package reactivestreams

import java.util.concurrent.{ CountDownLatch, TimeUnit }

import org.mongodb.scala.Completed

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.util.{ Failure, Success, Try }
import org.reactivestreams.{ Subscriber, Subscription }

object TestSubscriber {
  def apply[T](): TestSubscriber[T] = {
    TestSubscriber(new Subscriber[T]() {
      def onSubscribe(subscription: Subscription) {}

      def onNext(result: T) {}

      def onComplete() {}

      def onError(e: Throwable) {}
    })
  }
}

case class TestSubscriber[T](delegate: Subscriber[T]) extends Subscriber[T] {
  private final val latch: CountDownLatch = new CountDownLatch(1)
  private final val onNextEvents: ArrayBuffer[T] = new ArrayBuffer[T]
  private final val onErrorEvents: ArrayBuffer[Throwable] = new ArrayBuffer[Throwable]
  private final val onCompleteEvents: ArrayBuffer[Completed] = new ArrayBuffer[Completed]

  private var subscription: Option[Subscription] = None

  def onSubscribe(subscription: Subscription) {
    this.subscription = Some(subscription)
  }

  /**
   * Provides the Subscriber with a new item to observe.
   *
   * The `Publisher` may call this method 0 or more times.
   *
   * The `Publisher` will not call this method again after it calls either `onComplete` or `onError`.
   *
   * @param result the item emitted by the obserable
   */
  def onNext(result: T): Unit = {
    onNextEvents += result
    delegate.onNext(result)
  }

  /**
   * Notifies the Subscriber that the obserable has experienced an error condition.
   *
   * If the obserable calls this method, it will not thereafter call `onNext` or
   * `onComplete`.
   *
   * @param e the exception encountered by the obserable
   */
  def onError(e: Throwable): Unit = {
    try {
      onErrorEvents += e
      delegate.onError(e)
    } finally {
      latch.countDown()
    }
  }

  /**
   * Notifies the Subscriber that the obserable has finished sending push-based notifications.
   *
   * The obserable will not call this method if it calls `onError`.
   *
   */
  def onComplete(): Unit = {
    try {
      onCompleteEvents += null
      delegate.onComplete()
    } finally {
      latch.countDown()
    }
  }

  /**
   * Allow calling the protected `Subscription.request(long)` from unit tests.
   *
   * @param n the maximum number of items you want the obserable to emit to the Subscriber at this time, or
   *          `Long.MaxValue` if you want the obserable to emit items at its own pace
   */
  def requestMore(n: Long): Unit = {
    subscription match {
      case Some(sub) => sub.request(n)
      case None =>
    }
  }

  /**
   * Get the `Throwable`s this `Subscriber` was notified of via `onError` as a `Seq`.
   *
   * @return a list of the Throwables that were passed to this Subscriber's { @link #onError} method
   */
  def getOnErrorEvents: Seq[Throwable] = onErrorEvents.toSeq

  /**
   * Get the sequence of items observed by this `Subscriber`, as an ordered `List`.
   *
   * @return a list of items observed by this Subscriber, in the order in which they were observed
   */
  def getOnNextEvents: Seq[T] = onNextEvents.toSeq

  /**
   * Returns the subscription to the this `Subscriber`.
   *
   * @return the subscription or null if not subscribed to
   */
  def getSubscription: Option[Subscription] = subscription

  /**
   * Assert that a particular sequence of items was received by this `Subscriber` in order.
   *
   * @param items the sequence of items expected to have been observed
   * @throws AssertionError if the sequence of items observed does not exactly match `items`
   */
  def assertReceivedOnNext(items: Seq[T]) {
    if (getOnNextEvents.size != items.size) {
      throw new AssertionError(s"Number of items does not match. Provided: ${items.size} Actual: ${getOnNextEvents.size}")
    }

    items.indices.foreach(i => {
      items(i) == onNextEvents(i) match {
        case false => throw new AssertionError(s"Value at index: $i expected to be [${items(i)}] but was: [${onNextEvents(i)}]")
        case true =>
      }
    })
  }

  /**
   * Assert that a single terminal event occurred, either `onComplete` or `onError`.
   *
   * @throws AssertionError if not exactly one terminal event notification was received
   */
  def assertTerminalEvent(): Unit = {
    if (onErrorEvents.size > 1) {
      throw new AssertionError("Too many onError events: " + onErrorEvents.size)
    }
    if (onCompleteEvents.size > 1) {
      throw new AssertionError("Too many onCompleted events: " + onCompleteEvents.size)
    }
    if (onCompleteEvents.size == 1 && onErrorEvents.size == 1) {
      throw new AssertionError("Received both an onError and onCompleted. Should be one or the other.")
    }
    if (onCompleteEvents.isEmpty && onErrorEvents.isEmpty) {
      throw new AssertionError("No terminal events received.")
    }
  }

  /**
   * Assert that no terminal event occurred, either `onComplete` or `onError`.
   *
   * @throws AssertionError if a terminal event notification was received
   */
  def assertNoTerminalEvent(): Unit = {
    if (onCompleteEvents.nonEmpty && onErrorEvents.nonEmpty) {
      throw new AssertionError("Terminal events received.")
    }
  }

  /**
   * Assert that this `Subscriber` has received no `onError` notifications.
   *
   * @throws AssertionError if this { @link Subscriber} has received one or more { @link #onError} notifications
   */
  def assertNoErrors(): Unit = {
    if (onErrorEvents.nonEmpty) {
      throw new RuntimeException("Unexpected onError events: " + onErrorEvents.size, getOnErrorEvents.head)
    }
  }

  /**
   * Assert that this `Subscriber` has received an `onError` notification.
   *
   * @throws AssertionError if this { @link Subscriber} did not received an { @link #onError} notifications
   */
  def assertErrored(): Unit = {
    if (onErrorEvents.isEmpty) {
      throw new RuntimeException("No onError events")
    }
  }

  /**
   * Blocks until this `Subscriber` receives a notification that the `Observable` is complete (either an `onCompleted` or
   * `onError` notification).
   *
   * @throws RuntimeException if the Subscriber is interrupted before the Observable is able to complete
   */
  def awaitTerminalEvent(): Unit = {
    Try(latch.await()) match {
      case Failure(ex) => throw new RuntimeException("Interrupted", ex)
      case _ =>
    }
  }

  /**
   * Blocks until this `Subscriber` receives a notification that the `Observable` is complete (either an `onCompleted` or
   * `onError` notification).
   *
   * @param duration the duration of the timeout
   * @throws RuntimeException
   *          if the Subscriber is interrupted before the Observable is able to complete
   */
  def awaitTerminalEvent(duration: Duration) {
    Try(latch.await(duration.toMillis, TimeUnit.MILLISECONDS)) match {
      case Failure(ex) => throw new RuntimeException("Interrupted", ex)
      case Success(false) => throw new RuntimeException("Failed to return in time")
      case Success(true) =>
    }
  }
}

