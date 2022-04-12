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

import com.mongodb.ExplainVerbosity

import java.util.concurrent.TimeUnit
import com.mongodb.reactivestreams.client.AggregatePublisher
import org.mockito.Mockito.{ verify, verifyNoMoreInteractions }
import org.mongodb.scala.model.Collation
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.duration.Duration

class AggregateObservableSpec extends BaseSpec with MockitoSugar {

  "AggregateObservable" should "have the same methods as the wrapped AggregateObservable" in {
    val wrapped: Set[String] = classOf[AggregatePublisher[Document]].getMethods.map(_.getName).toSet
    val local: Set[String] = classOf[AggregateObservable[Document]].getMethods.map(_.getName).toSet

    wrapped.foreach((name: String) => {
      val cleanedName = name.stripPrefix("get")
      assert(local.contains(name) | local.contains(cleanedName.head.toLower + cleanedName.tail), s"Missing: $name")
    })
  }

  it should "call the underlying methods" in {
    val wrapper = mock[AggregatePublisher[Document]]
    val observable = AggregateObservable(wrapper)

    val duration = Duration(1, TimeUnit.SECONDS)
    val collation = Collation.builder().locale("en").build()
    val hint = Document("{hint: 1}")
    val batchSize = 10
    val ct = classOf[Document]
    val verbosity = ExplainVerbosity.QUERY_PLANNER

    observable.allowDiskUse(true)
    observable.maxTime(duration)
    observable.maxAwaitTime(duration)
    observable.bypassDocumentValidation(true)
    observable.collation(collation)
    observable.comment("comment")
    observable.hint(hint)
    observable.batchSize(batchSize)
    observable.explain[Document]()
    observable.explain[Document](verbosity)

    verify(wrapper).allowDiskUse(true)
    verify(wrapper).maxTime(duration.toMillis, TimeUnit.MILLISECONDS)
    verify(wrapper).maxAwaitTime(duration.toMillis, TimeUnit.MILLISECONDS)
    verify(wrapper).bypassDocumentValidation(true)
    verify(wrapper).collation(collation)
    verify(wrapper).comment("comment")
    verify(wrapper).hint(hint)
    verify(wrapper).batchSize(batchSize)
    verify(wrapper).explain(ct)
    verify(wrapper).explain(ct, verbosity)

    observable.toCollection()
    verify(wrapper).toCollection

    verifyNoMoreInteractions(wrapper)
  }
}
