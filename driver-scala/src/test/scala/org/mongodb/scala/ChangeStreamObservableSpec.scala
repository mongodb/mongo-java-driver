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

import java.util.concurrent.TimeUnit

import com.mongodb.reactivestreams.client.ChangeStreamPublisher
import org.mockito.Mockito.{ verify, verifyNoMoreInteractions }
import org.mongodb.scala.bson.BsonTimestamp
import org.mongodb.scala.model.Collation
import org.mongodb.scala.model.changestream.FullDocument
import org.reactivestreams.Publisher
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.duration.Duration
import scala.util.Success

class ChangeStreamObservableSpec extends BaseSpec with MockitoSugar {

  "ChangeStreamObservable" should "have the same methods as the wrapped ChangeStreamObservable" in {
    val mongoPublisher: Set[String] = classOf[Publisher[Document]].getMethods.map(_.getName).toSet
    val wrapped: Set[String] = classOf[ChangeStreamPublisher[Document]].getMethods
      .map(_.getName)
      .toSet -- mongoPublisher
    val local = classOf[ChangeStreamObservable[Document]].getMethods.map(_.getName).toSet

    wrapped.foreach((name: String) => {
      val cleanedName = name.stripPrefix("get")
      assert(local.contains(name) | local.contains(cleanedName.head.toLower + cleanedName.tail), s"Missing: $name")
    })
  }

  it should "call the underlying methods" in {
    val wrapper = mock[ChangeStreamPublisher[Document]]
    val observable = ChangeStreamObservable[Document](wrapper)

    val duration = Duration(1, TimeUnit.SECONDS)
    val resumeToken = Document()
    val fullDocument = FullDocument.DEFAULT
    val startAtTime = BsonTimestamp()
    val collation = Collation.builder().locale("en").build()
    val batchSize = 10

    observable.batchSize(batchSize)
    observable.fullDocument(fullDocument)
    observable.resumeAfter(resumeToken)
    observable.startAfter(resumeToken)
    observable.startAtOperationTime(startAtTime)
    observable.maxAwaitTime(duration)
    observable.collation(collation)
    observable.withDocumentClass(classOf[Int])

    verify(wrapper).batchSize(batchSize)
    verify(wrapper).fullDocument(fullDocument)
    verify(wrapper).resumeAfter(resumeToken.underlying)
    verify(wrapper).startAfter(resumeToken.underlying)
    verify(wrapper).startAtOperationTime(startAtTime)
    verify(wrapper).maxAwaitTime(duration.toMillis, TimeUnit.MILLISECONDS)
    verify(wrapper).collation(collation)
    verify(wrapper).withDocumentClass(classOf[Int])

    verifyNoMoreInteractions(wrapper)
  }

  it should "mirror FullDocument" in {
    FullDocument.fromString("default") shouldBe Success(FullDocument.DEFAULT)
    FullDocument.fromString("madeUp").isFailure shouldBe true
  }
}
