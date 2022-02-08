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
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux

import scala.util.{ Failure, Success, Try }

private[scala] case class RecoverWithObservable[T, U >: T](
    observable: Observable[T],
    pf: PartialFunction[Throwable, Observable[U]],
    throwOriginalException: Boolean = false
) extends Observable[U] {

  override def subscribe(observer: Observer[_ >: U]): Unit =
    Flux
      .from(observable)
      .onErrorResume((t: Throwable) => {
        Try(pf(t)) match {
          case Success(res) =>
            Flux
              .from(res)
              .onErrorResume((ex: Throwable) => {
                if (throwOriginalException) {
                  throw t
                } else {
                  throw ex
                }
              })
              .asInstanceOf[Publisher[T]]
          case Failure(_) => throw t
        }
      })
      .subscribe(observer)

}
