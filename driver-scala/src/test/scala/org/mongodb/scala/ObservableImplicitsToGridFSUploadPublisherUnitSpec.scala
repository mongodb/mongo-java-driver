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
package org.mongodb.scala

import com.mongodb.reactivestreams.client.gridfs.GridFSUploadPublisher
import org.mongodb.scala.bson.{ BsonInt32, BsonValue, ObjectId }
import org.reactivestreams.Subscriber
import reactor.core.publisher.Mono

class ObservableImplicitsToGridFSUploadPublisherUnitSpec extends BaseSpec {
  it should "emit exactly one element" in {
    var onNextCounter = 0
    VoidGridFSUploadPublisher().toObservable().subscribe((_: Void) => onNextCounter += 1)
    onNextCounter shouldBe 0

    onNextCounter = 0
    var errorActual: Option[Throwable] = None
    var completed = false
    toGridFSUploadPublisherUnit().subscribe(
      (_: Unit) => onNextCounter += 1,
      (error: Throwable) => errorActual = Some(error),
      () => completed = true
    )
    onNextCounter shouldBe 1
    errorActual shouldBe None
    completed shouldBe true
  }

  it should "signal the underlying error" in {
    var onNextCounter = 0
    val errorExpected = Some(new Exception())
    var errorActual: Option[Throwable] = None
    var completed = false
    toGridFSUploadPublisherUnit(errorExpected).subscribe(
      (_: Unit) => onNextCounter += 1,
      (error: Throwable) => errorActual = Some(error),
      () => completed = true
    )
    onNextCounter shouldBe 0
    errorActual shouldBe errorExpected
    completed shouldBe false
  }

  it should "work with explicit request" in {
    var onNextCounter = 0
    var errorActual: Option[Throwable] = None
    var completed = false
    toGridFSUploadPublisherUnit().subscribe(new Observer[Unit] {
      override def onSubscribe(subscription: Subscription): Unit = subscription.request(1)

      override def onNext(result: Unit): Unit = onNextCounter += 1

      override def onError(error: Throwable): Unit = errorActual = Some(error)

      override def onComplete(): Unit = completed = true
    })
    onNextCounter shouldBe 1
    errorActual shouldBe None
    completed shouldBe true
  }

  def toGridFSUploadPublisherUnit(error: Option[Exception] = Option.empty): Observable[Unit] = {
    gridfs.ToGridFSUploadPublisherUnit(VoidGridFSUploadPublisher(error)).toObservable()
  }

  /**
   * A [[GridFSUploadPublisher]] that emits no items.
   */
  case class VoidGridFSUploadPublisher(error: Option[Exception] = Option.empty) extends GridFSUploadPublisher[Void] {
    private val objectId = new ObjectId()
    private val id = BsonInt32(0)

    override def getObjectId: ObjectId = objectId

    override def getId: BsonValue = id

    override def subscribe(subscriber: Subscriber[_ >: Void]): Unit = {
      val mono = error match {
        case Some(error) => Mono.error(error)
        case None        => Mono.empty()
      }
      mono.subscribe(subscriber)
    }
  }
}
