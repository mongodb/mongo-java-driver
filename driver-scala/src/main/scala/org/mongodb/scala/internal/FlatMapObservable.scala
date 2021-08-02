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

import org.mongodb.scala._

import java.util.concurrent.atomic.AtomicReference

sealed trait State
case object Init extends State
case class WaitingOnChild(s: Subscription) extends State
case object LastChildNotified extends State
case object LastChildResponded extends State
case object Done extends State
case object Error extends State

private[scala] case class FlatMapObservable[T, S](observable: Observable[T], f: T => Observable[S])
    extends Observable[S] {
  // scalastyle:off cyclomatic.complexity method.length
  override def subscribe(observer: Observer[_ >: S]): Unit = {
    observable.subscribe(
      SubscriptionCheckingObserver(
        new Observer[T] {
          @volatile private var outerSubscription: Option[Subscription] = None
          @volatile private var demand: Long = 0
          private val state = new AtomicReference[State](Init)

          override def onSubscribe(subscription: Subscription): Unit = {
            val masterSub = new Subscription() {
              override def isUnsubscribed: Boolean = subscription.isUnsubscribed
              override def unsubscribe(): Unit = subscription.unsubscribe()
              override def request(n: Long): Unit = {
                require(n > 0L, s"Number requested must be greater than zero: $n")
                val localDemand = addDemand(n)
                state.get() match {
                  case Init              => subscription.request(1L)
                  case WaitingOnChild(s) => s.request(localDemand)
                  case _                 => // noop
                }
              }
            }
            outerSubscription = Some(masterSub)
            state.set(Init)
            observer.onSubscribe(masterSub)
          }

          override def onComplete(): Unit = {
            state.get() match {
              case Done  => // ok
              case Error => // ok
              case Init if state.compareAndSet(Init, Done) =>
                observer.onComplete()
              case w @ WaitingOnChild(_) if state.compareAndSet(w, LastChildNotified) =>
              // letting the child know that we delegate onComplete call to it
              case LastChildNotified =>
              // wait for the child to do the delegated onCompleteCall
              case LastChildResponded if state.compareAndSet(LastChildResponded, Done) =>
                observer.onComplete()
              case other =>
                // state machine is broken, let's fail
                // normally this won't happen
                throw new IllegalStateException(s"Unexpected state in FlatMapObservable `onComplete` handler: ${other}")
            }
          }

          override def onError(throwable: Throwable): Unit = {
            observer.onError(throwable)
          }

          override def onNext(tResult: T): Unit = {
            f(tResult).subscribe(
              new Observer[S]() {
                override def onError(throwable: Throwable): Unit = {
                  state.set(Error)
                  observer.onError(throwable)
                }

                override def onSubscribe(subscription: Subscription): Unit = {
                  state.set(WaitingOnChild(subscription))
                  if (demand > 0) subscription.request(demand)
                }

                override def onComplete(): Unit = {
                  state.get() match {
                    case Done                                                                            => // no need to call parent's onComplete
                    case Error                                                                           => // no need to call parent's onComplete
                    case LastChildNotified if state.compareAndSet(LastChildNotified, LastChildResponded) =>
                      // parent told us to call onComplete
                      observer.onComplete()
                    case _ if demand > 0 =>
                      // otherwise we are not the last child, let's tell the parent
                      // it's not dealing with us anymore.
                      // Init -> * will be handled by possible later items in the stream
                      state.set(Init)
                      addDemand(-1) // reduce demand by 1 as it will be incremented by the outerSubscription
                      outerSubscription.foreach(_.request(1))
                    case _ =>
                      // no demand
                      state.set(Init)
                  }
                }

                override def onNext(tResult: S): Unit = {
                  addDemand(-1)
                  observer.onNext(tResult)
                }
              }
            )
          }

          /**
           * Adds extra demand and protects against Longs rolling over
           *
           * @param extraDemand the amount of extra demand
           * @return the updated demand
           */
          private def addDemand(extraDemand: Long): Long = {
            this.synchronized {
              demand += extraDemand
              if (demand < 0) {
                if (extraDemand < 0) {
                  throw new IllegalStateException("Demand cannot be reduced to below zero")
                }
                demand = Long.MaxValue
              }
            }
            demand
          }
        }
      )
    )
  }

  // scalastyle:on cyclomatic.complexity method.length
}
