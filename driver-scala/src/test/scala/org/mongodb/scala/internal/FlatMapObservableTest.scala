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
import org.scalatest.Assertions
import org.scalatest.concurrent.{ Eventually, Futures }

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ Future, Promise }

class FlatMapObservableTest extends BaseSpec with Futures with Eventually {
  "FlatMapObservable" should "only complete once" in {
    Assertions.cancel("Temporarily skipping this test")

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
          p.trySuccess(())
          completedCounter.incrementAndGet()
        }
      )
    eventually(assert(completedCounter.get() == 1, s"${completedCounter.get()}"))
  }

  it should "call onError if the mapper fails" in {
    val p = Promise[Unit]()
    val errorCounter = new AtomicInteger(0)
    Observable(1 to 100)
      .flatMap(
        x =>
          if (x > 10) {
            throw new IllegalStateException("Fail")
          } else {
            createObservable(x)
          }
      )
      .subscribe(
        _ => (),
        _ => {
          p.trySuccess()
          errorCounter.incrementAndGet()
        },
        () => {
          p.failure(new IllegalStateException("Should not complete"))
        }
      )
    eventually(assert(errorCounter.get() == 1, s"${errorCounter.get()}"))
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
