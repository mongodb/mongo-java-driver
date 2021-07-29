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

import org.mongodb.scala.{ BaseSpec, Observable, Observer }
import org.scalatest.concurrent.{ Eventually, Futures }

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ Future, Promise }

class FlatMapObservableTest extends BaseSpec with Futures with Eventually {
  "FlatMapObservable" should "only complete once" in {
    val p = Promise[Unit]()
    val completedCounter = new AtomicInteger(0)
    Observable(1 to 100)
      .flatMap(
        x => createObservable(x)
      )
      .subscribe(
        _ => (),
        e => p.failure(e),
        () => {
          completedCounter.incrementAndGet()
          Thread.sleep(100)
          p.trySuccess(())
        }
      )
    eventually(assert(completedCounter.get() == 1, s"${completedCounter.get()}"))
    Thread.sleep(200)
    assert(completedCounter.get() == 1, s"${completedCounter.get()}")
    Thread.sleep(1000)
  }

  private def createObservable(x: Int): Observable[Int] = new Observable[Int] {
    override def subscribe(observer: Observer[_ >: Int]): Unit = {
      Future(()).onComplete(_ => {
        observer.onNext(x)
        observer.onComplete()
      })
    }
  }
}
