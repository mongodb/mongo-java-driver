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

import org.mongodb.scala.{ Observable, Observer, SingleObservable }
import reactor.core.publisher.Flux

private[scala] case class FoldLeftObservable[T, S](observable: Observable[T], initialValue: S, accumulator: (S, T) => S)
    extends SingleObservable[S] {

  override def subscribe(observer: Observer[_ >: S]): Unit =
    Flux.from(observable).reduce(initialValue, (s: S, t: T) => accumulator(s, t)).subscribe(observer)
}
