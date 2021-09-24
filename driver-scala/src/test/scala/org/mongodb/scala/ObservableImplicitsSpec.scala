package org.mongodb.scala

import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.concurrent.ScalaFutures

class ObservableImplicitsSpec extends BaseSpec with MockFactory with ScalaFutures {

  "ToSingleObservableVoid" should "subscribe to an Observable[Unit], rather than Observable[Void], so that it is composable using monad operations" in {
    case class CustomVoidMongoResult(namespace: MongoNamespace, additionalMetadata: Map[String, String])
    val expected = CustomVoidMongoResult(MongoNamespace("database", "collection"), Map.empty)
    val voidObservable = SingleObservable.apply[Void](null).publisher
    val stringObservable = voidObservable
    // show that you can map, filter, etc. over an Observable that's implicitly converted from Publisher[Void]
      .map(_ => expected)
      .filter(_ => true)
    val actual = stringObservable.head().futureValue

    assert(actual === expected)
  }

}
