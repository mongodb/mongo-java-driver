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

import com.mongodb.reactivestreams.client.DistinctPublisher
import org.mongodb.scala.model.Collation
import org.reactivestreams.Publisher
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration.Duration

class DistinctObservableSpec extends FlatSpec with Matchers with MockFactory {

  "DistinctObservable" should "have the same methods as the wrapped DistinctObservable" in {
    val mongoPublisher: Set[String] = classOf[Publisher[Document]].getMethods.map(_.getName).toSet
    val wrapped = classOf[DistinctPublisher[Document]].getMethods.map(_.getName).toSet -- mongoPublisher
    val local = classOf[DistinctObservable[Document]].getMethods.map(_.getName).toSet

    wrapped.foreach((name: String) => {
      val cleanedName = name.stripPrefix("get")
      assert(local.contains(name) | local.contains(cleanedName.head.toLower + cleanedName.tail), s"Missing: $name")
    })
  }

  it should "call the underlying methods" in {
    val wrapper = mock[DistinctPublisher[Document]]
    val observable = DistinctObservable(wrapper)

    val filter = Document("a" -> 1)
    val duration = Duration(1, TimeUnit.SECONDS)
    val collation = Collation.builder().locale("en").build()
    val batchSize = 10

    wrapper.expects(Symbol("filter"))(filter).once()
    wrapper.expects(Symbol("maxTime"))(duration.toMillis, TimeUnit.MILLISECONDS).once()
    wrapper.expects(Symbol("collation"))(collation).once()
    wrapper.expects(Symbol("batchSize"))(batchSize).once()

    observable.filter(filter)
    observable.maxTime(duration)
    observable.collation(collation)
    observable.batchSize(batchSize)
  }
}
