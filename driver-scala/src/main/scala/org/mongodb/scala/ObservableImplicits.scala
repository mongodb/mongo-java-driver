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

import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.gridfs.GridFSFile
import org.mongodb.scala.internal.MapObservable
import org.reactivestreams.{ Publisher, Subscriber, Subscription => JSubscription }
import reactor.core.publisher.{ Flux, Mono }

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.Future

/**
 * Implicit conversion support for Publishers, Observables and Subscriptions
 *
 * Automatically imported into the `org.mongodb.scala` namespace
 */
trait ObservableImplicits {

  implicit class BoxedPublisher[T](pub: => Publisher[T]) extends Observable[T] {
    val publisher = pub

    /**
     * @return an [[Observable]] (extended) publisher
     */
    def toObservable(): Observable[T] = this

    override def subscribe(observer: Observer[_ >: T]): Unit = Flux.from(publisher).subscribe(observer)
    override def subscribe(s: Subscriber[_ >: T]): Unit = Flux.from(publisher).subscribe(s)
  }

  implicit class BoxedSubscriber[T](sub: => Subscriber[_ >: T]) extends Observer[T] {
    val subscriber = sub

    override def onSubscribe(subscription: Subscription): Unit = subscriber.onSubscribe(subscription)

    override def onError(e: Throwable): Unit = subscriber.onError(e)

    override def onComplete(): Unit = subscriber.onComplete()

    override def onNext(result: T): Unit = subscriber.onNext(result)
  }

  implicit class BoxedSubscription(subscription: => JSubscription) extends Subscription {
    val cancelled = new AtomicBoolean(false)
    override def request(n: Long): Unit = subscription.request(n)

    override def unsubscribe(): Unit = {
      cancelled.set(true)
      subscription.cancel()
    }

    override def isUnsubscribed: Boolean = cancelled.get()

  }

  implicit class ToObservableString(pub: => Publisher[java.lang.String]) extends Observable[String] {
    val publisher = pub
    override def subscribe(observer: Observer[_ >: String]): Unit = Flux.from(publisher).subscribe(observer)
  }

  implicit class ToSingleObservablePublisher[T](pub: => Publisher[T]) extends SingleObservable[T] {
    val publisher = pub

    /**
     * Converts the [[Observable]] to a single result [[Observable]].
     *
     * @return a single result Observable
     */
    def toSingle(): SingleObservable[T] = this

    override def subscribe(observer: Observer[_ >: T]): Unit = Mono.from(publisher).subscribe(observer)
  }

  implicit class ToSingleObservableInt(pub: => Publisher[java.lang.Integer]) extends SingleObservable[Int] {
    val publisher = pub
    override def subscribe(observer: Observer[_ >: Int]): Unit = {
      Mono
        .from(MapObservable(publisher.toObservable(), (i: Integer) => i.toInt))
        .subscribe(observer)
    }
  }

  implicit class ToSingleObservableLong(pub: => Publisher[java.lang.Long]) extends SingleObservable[Long] {
    val publisher = pub
    override def subscribe(observer: Observer[_ >: Long]): Unit =
      Mono
        .from(MapObservable(publisher.toObservable(), (i: java.lang.Long) => i.toLong))
        .subscribe(observer)
  }

  implicit class ToSingleObservableObjectId(pub: => Publisher[org.bson.types.ObjectId])
      extends SingleObservable[ObjectId] {
    val publisher = pub
    override def subscribe(observer: Observer[_ >: ObjectId]): Unit = Mono.from(publisher).subscribe(observer)
  }

  implicit class ToSingleObservableGridFS(pub: => Publisher[com.mongodb.client.gridfs.model.GridFSFile])
      extends SingleObservable[GridFSFile] {
    val publisher = pub
    override def subscribe(observer: Observer[_ >: GridFSFile]): Unit = Mono.from(publisher).subscribe(observer)
  }

  implicit class ToSingleObservableVoid(pub: => Publisher[Void]) extends SingleObservable[Void] {
    val publisher = pub
    override def subscribe(observer: Observer[_ >: Void]): Unit =
      Mono
        .from(pub)
        .subscribe(
          (_: Void) => {},
          (e: Throwable) => observer.onError(e),
          () => observer.onComplete(),
          (s: JSubscription) => observer.onSubscribe(s)
        )
  }

  implicit class ObservableFuture[T](obs: => Observable[T]) {
    val observable = obs

    /**
     * Collects the [[Observable]] results and converts to a `scala.concurrent.Future`.
     *
     * Automatically subscribes to the `Observable` and uses the [[[Observable.collect[S]()*]]] method to aggregate the results.
     *
     * @note If the Observable is large then this will consume lots of memory!
     *       If the underlying Observable is infinite this Observable will never complete.
     * @return a future representation of the whole Observable
     */
    def toFuture(): Future[Seq[T]] = observable.collect().head()

  }

  implicit class SingleObservableFuture[T](obs: => SingleObservable[T]) {
    val observable = obs

    /**
     * Collects the [[Observable]] results and converts to a `scala.concurrent.Future`.
     *
     * Automatically subscribes to the `Observable` and uses the [[Observable.head]] method to aggregate the results.
     *
     * @note If the Observable is large then this will consume lots of memory!
     *       If the underlying Observable is infinite this Observable will never complete.
     * @return a future representation of the whole Observable
     */
    def toFuture(): Future[T] = observable.head()

    /**
     * Collects the [[Observable]] result and converts to a `scala.concurrent.Future`.
     * @return a future representation of the Observable
     *
     */
    def toFutureOption(): Future[Option[T]] = observable.headOption()
  }

}
