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
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.util.{ Failure, Success }

class ObservableImplementationSpec extends BaseSpec with TableDrivenPropertyChecks {

  "Observables" should "call onCompleted once all results are consumed" in {
    forAll(happyObservables) { (observable: Observable[Int], observer: TestObserver[Int]) =>
      {
        observable.subscribe(observer)

        val subscription = observer.subscription.get
        subscription.request(1)
        subscription.request(1000)

        subscription.isUnsubscribed should equal(false)
        observer.error should equal(None)
        observer.completed should equal(true)
      }
    }
  }

  it should "Consuming observables should handle over requesting observables as expected" in {
    forAll(overRequestingObservables) { (observable: Observable[Int], observer: TestObserver[Int], expected: Int) =>
      {
        observable.subscribe(observer)

        val subscription = observer.subscription.get
        subscription.request(1000)

        subscription.isUnsubscribed should equal(false)
        observer.error should equal(None)
        observer.completed should equal(true)
        observer.results.size should equal(expected)
      }
    }
  }

  it should "be well behaved when and call onError if the Observable errors" in {
    forAll(failingObservables) { (observable: Observable[Int]) =>
      {
        var thrown = false
        observable.subscribe(_ => (), _ => thrown = true)
        thrown should equal(true)
      }
    }
  }

  it should "be well behaved when errors are caused by passed in function and call onError" in {
    forAll(failingFunctionsObservables) { (observable: Observable[Int]) =>
      {
        var thrown = false
        observable.subscribe(_ => (), _ => thrown = true)
        thrown should equal(true)
      }
    }
  }

  it should "honor subscriptions and isUnsubscribed" in {
    forAll(happyObservables) { (observable: Observable[Int], observer: TestObserver[Int]) =>
      {
        observable.subscribe(observer)

        val expectedCompleted = observable.isInstanceOf[FoldLeftObservable[_, _]]
        val subscription = observer.subscription.get
        subscription.request(1)
        subscription.request(2)
        subscription.request(3)
        subscription.request(4)
        subscription.isUnsubscribed should equal(false)

        subscription.unsubscribe()
        subscription.isUnsubscribed should equal(true)

        observer.error should equal(None)
        observer.results.length should be <= 10
        observer.completed should equal(expectedCompleted)
      }
    }
  }

  it should "honor subscriptions and isUnsubscribed without requesting data" in {
    forAll(happyObservables) { (observable: Observable[Int], observer: TestObserver[Int]) =>
      {
        observable.subscribe(observer)

        val subscription = observer.subscription.get
        subscription.isUnsubscribed should equal(false)

        subscription.unsubscribe()
        subscription.isUnsubscribed should equal(true)

        observer.error should equal(None)
        observer.results shouldBe empty
        observer.completed should equal(false)

        subscription.request(1000)
        observer.results shouldBe empty
        observer.completed should equal(false)
      }
    }
  }

  it should "propagate errors from the observer" in {
    forAll(happyObservables) { (observable: Observable[Int], observer: TestObserver[Int]) =>
      {
        testObserver[Int](observable, observer)
      }
    }
  }

  def testObserver[I](observable: Observable[I], observer: TestObserver[I]): Unit = {
    val failObserver = TestObserver[I](new Observer[I] {
      override def onError(throwable: Throwable): Unit = {}

      override def onSubscribe(subscription: Subscription): Unit = {}

      override def onComplete(): Unit = {}

      override def onNext(tResult: I): Unit = throw new Throwable("Failed action")
    })

    observable.subscribe(failObserver)
    intercept[Throwable] {
      observer.subscription.get.request(10)
    }
  }

  it should "allow multiple subscriptions" in {
    forAll(happyObservables) { (observable: Observable[Int], observer: TestObserver[Int]) =>
      {
        val observer1 = TestObserver[Int]()

        observable.subscribe(observer)
        observable.subscribe(observer1)
        observer.subscription.get.request(Long.MaxValue)
        observer1.subscription.get.request(Long.MaxValue)

        observer.error.isEmpty should equal(true)
        observer1.error.isEmpty should equal(true)
        observer.completed should equal(true)
        observer1.completed should equal(true)

        observer.results.length should equal(observer.results.length)
      }
    }
  }

  it should "return the length of the smallest Observable from ZipObservable" in {
    forAll(zippedObservables) { (observable: Observable[(Int, Int)]) =>
      {
        val observer = TestObserver[(Int, Int)]()
        observable.subscribe(observer)

        observer.subscription.foreach(_.request(100))

        observer.results should equal((1 to 50).map(i => (i, i)))
        observer.completed should equal(true)
      }
    }

    forAll(zippedObservablesWithEmptyObservable) { (observable: Observable[(Int, Int)]) =>
      {
        val observer = TestObserver[(Int, Int)]()
        observable.subscribe(observer)

        observer.subscription.foreach(_.request(100))

        observer.results should equal(List())
        observer.completed should equal(true)
      }
    }
  }

  it should "error if requested amount is less than 1" in {
    forAll(happyObservables) { (observable: Observable[Int], observer: TestObserver[Int]) =>
      {
        observable.subscribe(observer)
        intercept[IllegalArgumentException] {
          observer.subscription.get.request(0)
        }
      }
    }
  }

  it should "handle multiple requests where request rolls over Long.MaxValue" in {
    forAll(happyObservables) { (observable: Observable[Int], observer: TestObserver[Int]) =>
      {
        observable.subscribe(observer)
        observer.subscription.get.request(Long.MaxValue - 1)
        observer.subscription.get.request(Long.MaxValue)

        observer.error should equal(None)
        observer.results should not be empty
        observer.completed should equal(true)
      }
    }
  }

  val failOn = 30

  private def failingObservables =
    Table(
      "observable",
      TestObservable(failOn = failOn),
      AndThenObservable[Int, Int](TestObservable(failOn = failOn), {
        case Success(r)  => 1000
        case Failure(ex) => 0
      }),
      FilterObservable[Int](TestObservable(failOn = failOn), (i: Int) => i % 2 != 0),
      FlatMapObservable[Int, Int](TestObservable(), (i: Int) => TestObservable(failOn = failOn)),
      FoldLeftObservable(TestObservable(1 to 100, failOn = failOn), 0, (v: Int, i: Int) => v + i),
      MapObservable[Int, Int](TestObservable(failOn = failOn), (i: Int) => i * 100),
      RecoverObservable[Int, Int](TestObservable(failOn = failOn), { case e: ArithmeticException => 999 }),
      RecoverWithObservable[Int, Int](TestObservable(failOn = failOn), {
        case e: ArithmeticException => TestObservable()
      }),
      RecoverWithObservable[Int, Int](TestObservable(failOn = failOn), {
        case e => TestObservable(failOn = failOn)
      }),
      ZipObservable[Int, Int](TestObservable(), TestObservable(failOn = failOn)).map[Int](a => a._1),
      ZipObservable[Int, Int](TestObservable(failOn = failOn), TestObservable()).map[Int](a => a._1)
    )

  private def failingFunctionsObservables =
    Table(
      "observable",
      FilterObservable[Int](TestObservable(), (i: Int) => {
        if (i > 10) {
          throw new RuntimeException("Error")
        }
        i % 2 == 0
      }),
      FlatMapObservable[Int, Int](TestObservable(), (i: Int) => {
        if (i > 10) {
          throw new RuntimeException("Error")
        }
        TestObservable(1 to 2)
      }),
      FoldLeftObservable(TestObservable(1 to 100), 0, (v: Int, i: Int) => {
        if (i > 10) {
          throw new RuntimeException("Error")
        }
        v + i
      }),
      MapObservable[Int, Int](TestObservable(), (i: Int) => {
        if (i > 10) {
          throw new RuntimeException("Error")
        }
        i * 100
      })
    )

  private def happyObservables =
    Table(
      ("observable", "observer"),
      (TestObservable(), TestObserver[Int]()),
      (AndThenObservable[Int, Int](TestObservable(), {
        case Success(r)  => 1000
        case Failure(ex) => 0
      }), TestObserver[Int]()),
      (FilterObservable[Int](TestObservable(), (i: Int) => i % 2 != 0), TestObserver[Int]()),
      (
        FlatMapObservable[Int, Int](TestObservable(), (i: Int) => TestObservable(1 to 1)),
        TestObserver[Int]()
      ),
      (
        FlatMapObservable[Int, Int](TestObservable(1 to 1), (i: Int) => TestObservable()),
        TestObserver[Int]()
      ),
      (FoldLeftObservable(TestObservable(1 to 100), 0, (v: Int, i: Int) => v + i), TestObserver[Int]()),
      (MapObservable[Int, Int](TestObservable(), (i: Int) => i * 100), TestObserver[Int]()),
      (RecoverObservable[Int, Int](TestObservable(), { case e: ArithmeticException => 999 }), TestObserver[Int]()),
      (
        RecoverWithObservable[Int, Int](TestObservable(), { case t => TestObservable() }),
        TestObserver[Int]()
      ),
      (
        RecoverWithObservable[Int, Int](TestObservable(1 to 10, failOn = 1), { case t => TestObservable() }),
        TestObserver[Int]()
      ),
      (IterableObservable((1 to 100).toStream), TestObserver[Int]()),
      (ZipObservable[Int, Int](TestObservable(), TestObservable()).map[Int](a => a._1), TestObserver[Int]())
    )

  private def zippedObservables =
    Table[Observable[(Int, Int)]](
      "observable",
      ZipObservable[Int, Int](TestObservable(1 to 50), TestObservable()),
      ZipObservable[Int, Int](TestObservable(), TestObservable(1 to 50))
    )

  private def zippedObservablesWithEmptyObservable =
    Table[Observable[(Int, Int)]](
      "observable",
      ZipObservable[Int, Int](TestObservable(1 to 50), TestObservable(List())),
      ZipObservable[Int, Int](TestObservable(List()), TestObservable(1 to 50))
    )

  private def overRequestingObservables =
    Table(
      ("observable", "observer", "expected"),
      (
        FlatMapObservable[Int, Int](
          OverRequestedObservable(TestObservable(1 to 10)),
          (i: Int) => TestObservable(1 to 10)
        ),
        TestObserver[Int](),
        100
      ),
      (
        RecoverWithObservable[Int, Int](
          TestObservable(1 to 10, failOn = 1),
          { case t => OverRequestedObservable(TestObservable(1 to 10)) }
        ),
        TestObserver[Int](),
        10
      )
    )

  case class OverRequestedObservable(observable: TestObservable = TestObservable()) extends Observable[Int] {

    var totalRequested = 0L
    override def subscribe(observer: Observer[_ >: Int]): Unit = {
      observable.subscribe(
        new Observer[Int] {

          var completed = false
          override def onError(throwable: Throwable): Unit = observer.onError(throwable)

          override def onSubscribe(subscription: Subscription): Unit = {
            val masterSub = new Subscription() {
              override def isUnsubscribed: Boolean = subscription.isUnsubscribed

              override def request(n: Long): Unit = {
                if (!completed) {
                  var demand = n + 1
                  if (demand < 0) demand = Long.MaxValue
                  totalRequested += demand
                  subscription.request(demand)
                }
              }
              override def unsubscribe(): Unit = subscription.unsubscribe()
            }
            observer.onSubscribe(masterSub)
          }

          override def onComplete(): Unit = {
            completed = true
            observer.onComplete()
          }

          override def onNext(tResult: Int): Unit = {
            observer.onNext(tResult)
          }
        }
      )
    }
  }

}
