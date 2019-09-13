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

import java.util.concurrent.atomic.AtomicBoolean

import com.mongodb.reactivestreams.client.Success
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.gridfs.GridFSFile
import org.mongodb.scala.internal._
import org.reactivestreams.{ Publisher, Subscriber, Subscription => JSubscription }

import scala.concurrent.Future

/**
 * Implicit conversion support for Publishers, Observables and Subscriptions
 *
 * Automatically imported into the `org.mongodb.scala` namespace
 */
trait ObservableImplicits {

  implicit class BoxedPublisher[T](publisher: Publisher[T]) extends Observable[T] {
    private def sub(observer: Observer[_ >: T]): Unit = publisher.subscribe(observer)

    /**
     * @return an [[Observable]] (extended) publisher
     */
    def toObservable(): Observable[T] = this

    override def subscribe(observer: Observer[_ >: T]): Unit = sub(observer)
    override def subscribe(s: Subscriber[_ >: T]): Unit = sub(BoxedSubscriber(s))
  }

  implicit class BoxedSubscriber[T](subscriber: Subscriber[_ >: T]) extends Observer[T] {

    override def onSubscribe(subscription: Subscription): Unit = subscriber.onSubscribe(subscription)

    override def onError(e: Throwable): Unit = subscriber.onError(e)

    override def onComplete(): Unit = subscriber.onComplete()

    override def onNext(result: T): Unit = subscriber.onNext(result)
  }

  implicit class BoxedSubscription(subscription: JSubscription) extends Subscription {
    val cancelled = new AtomicBoolean(false)
    override def request(n: Long): Unit = subscription.request(n)

    override def unsubscribe(): Unit = {
      cancelled.set(true)
      subscription.cancel()
    }

    override def isUnsubscribed: Boolean = cancelled.get()

  }

  implicit class ToObservableString(publisher: Publisher[java.lang.String]) extends Observable[String] {
    override def subscribe(observer: Observer[_ >: String]): Unit = publisher.toObservable().subscribe(observer)
  }

  implicit class ToSingleObservablePublisher[T](publisher: Publisher[T]) extends SingleObservable[T] {

    /**
     * Converts the [[Observable]] to a single result [[Observable]].
     *
     * @return a single result Observable
     */
    def toSingle(): SingleObservable[T] = this

    override def subscribe(observer: Observer[_ >: T]): Unit = {
      publisher.subscribe(
        SubscriptionCheckingObserver(new Observer[T]() {
          @volatile
          var results: Option[T] = None

          @volatile
          var terminated: Boolean = false

          override def onSubscribe(subscription: Subscription): Unit = {
            observer.onSubscribe(subscription)
            subscription.request(1)
          }

          override def onError(throwable: Throwable): Unit = throwable match {
            case npe: NullPointerException => onComplete()
            case _                         => completeWith("onError", () => observer.onError(throwable))
          }

          override def onComplete(): Unit = {
            completeWith("onComplete", { () =>
              results.foreach { (result: T) =>
                observer.onNext(result)
              }
              observer.onComplete()
            })
          }

          override def onNext(tResult: T): Unit = {
            check(results.isEmpty, "SingleObservable.onNext cannot be called with multiple results.")
            results = Some(tResult)
          }

          private def completeWith(method: String, action: () => Any): Unit = {
            check(!terminated, s"$method called after the Observer has already completed or errored. $observer")
            terminated = true
            action()
          }

          private def check(requirement: Boolean, message: String): Unit = {
            if (!requirement) throw new IllegalStateException(message)
          }
        })
      )
    }
  }

  implicit class ToSingleObservableVoid(publisher: Publisher[Void]) extends SingleObservable[Completed] {
    override def subscribe(observer: Observer[_ >: Completed]): Unit = publisher.toSingle().subscribe(observer)
  }

  implicit class ToSingleObservableInt(publisher: Publisher[java.lang.Integer]) extends SingleObservable[Int] {
    override def subscribe(observer: Observer[_ >: Int]): Unit =
      publisher.toObservable().map(_.intValue()).toSingle().subscribe(observer)
  }

  implicit class ToSingleObservableLong(publisher: Publisher[java.lang.Long]) extends SingleObservable[Long] {
    override def subscribe(observer: Observer[_ >: Long]): Unit =
      publisher.toObservable().map(_.longValue()).toSingle().subscribe(observer)
  }

  implicit class ToSingleObservableObjectId(publisher: Publisher[org.bson.types.ObjectId])
      extends SingleObservable[ObjectId] {
    override def subscribe(observer: Observer[_ >: ObjectId]): Unit = publisher.toSingle().subscribe(observer)
  }

  implicit class ToSingleObservableGridFS(publisher: Publisher[com.mongodb.client.gridfs.model.GridFSFile])
      extends SingleObservable[GridFSFile] {
    override def subscribe(observer: Observer[_ >: GridFSFile]): Unit = publisher.toSingle().subscribe(observer)
  }

  implicit class ToSingleObservableCompleted(publisher: Publisher[Success]) extends SingleObservable[Completed] {
    override def subscribe(observer: Observer[_ >: Completed]): Unit =
      publisher
        .toSingle()
        .subscribe(new Observer[Success] {

          override def onSubscribe(subscription: Subscription): Unit = observer.onSubscribe(subscription)

          override def onNext(result: Success): Unit = {}

          override def onError(e: Throwable): Unit = observer.onError(e)

          override def onComplete(): Unit = observer.onComplete()
        })
  }

  implicit class ObservableFuture[T](observable: Observable[T]) {

    /**
     * Collects the [[Observable]] results and converts to a [[scala.concurrent.Future]].
     *
     * Automatically subscribes to the `Observable` and uses the [[collect]] method to aggregate the results.
     *
     * @note If the Observable is large then this will consume lots of memory!
     *       If the underlying Observable is infinite this Observable will never complete.
     * @return a future representation of the whole Observable
     */
    def toFuture(): Future[Seq[T]] = observable.collect().head()

  }

  implicit class SingleObservableFuture[T](observable: SingleObservable[T]) {

    /**
     * Collects the [[Observable]] results and converts to a [[scala.concurrent.Future]].
     *
     * Automatically subscribes to the `Observable` and uses the [[collect]] method to aggregate the results.
     *
     * @note If the Observable is large then this will consume lots of memory!
     *       If the underlying Observable is infinite this Observable will never complete.
     * @return a future representation of the whole Observable
     */
    def toFuture(): Future[T] = observable.head()

    /**
     * Collects the [[Observable]] result and converts to a [[scala.concurrent.Future]].
     * @return a future representation of the Observable
     *
     */
    def toFutureOption(): Future[Option[T]] = observable.headOption()
  }

}
