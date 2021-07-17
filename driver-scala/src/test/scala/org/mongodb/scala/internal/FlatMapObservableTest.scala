package org.mongodb.scala.internal

import org.mongodb.scala.{ BaseSpec, Observable, Observer }
import org.scalatest.concurrent.{ Eventually, Futures }
import org.scalatest.prop.TableDrivenPropertyChecks

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ Future, Promise }

class FlatMapObservableTest extends BaseSpec with TableDrivenPropertyChecks with Futures with Eventually {
  "FlatMapObservable" should "only complete once" in {
    val p = Promise[Unit]()
    val completedCounter = new AtomicInteger(0)
    Observable(1 to 100)
      .flatMap(
        x =>
          new Observable[Int] {
            override def subscribe(observer: Observer[_ >: Int]): Unit = {
              Future(()).onComplete(_ => {
                observer.onNext(x)
                observer.onComplete()
              })

            }
          }
      )
      .subscribe(
        _ => (),
        p.failure,
        () => {
          completedCounter.incrementAndGet()
          Thread.sleep(100)
          p.success(())
        }
      )
    eventually(assert(completedCounter.get() == 1, s"${completedCounter.get()}"))
    Thread.sleep(200)
    assert(completedCounter.get() == 1, s"${completedCounter.get()}")

  }
}
