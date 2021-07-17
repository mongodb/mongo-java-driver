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
        x =>
          (observer: Observer[_ >: Int]) => {
            Future(()).onComplete(_ => {
              observer.onNext(x)
              observer.onComplete()
            })
          }
      )
      .subscribe(
        _ => (),
        p.failure,
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
}
