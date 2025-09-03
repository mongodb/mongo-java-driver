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

import org.mongodb.scala.{ Observable, Observer }
import reactor.core.publisher.Flux
import org.reactivestreams.{ Subscription => JSubscription }
import reactor.util.function.{ Tuple2 => RTuple2 }

private[scala] case class ZipObservable[L, R](
    leftObservable: Observable[L],
    rightObservable: Observable[R]
) extends Observable[(L, R)] {

  def subscribe(observer: Observer[_ >: (L, R)]): Unit =
    Flux
      .zip(leftObservable, rightObservable)
      .subscribe(
        (t: RTuple2[L, R]) => observer.onNext((t.getT1, t.getT2)),
        (e: Throwable) => observer.onError(e),
        () => observer.onComplete(),
        (s: JSubscription) => observer.onSubscribe(s)
      )
}
