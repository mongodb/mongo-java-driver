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

import com.mongodb.client.cursor.TimeoutMode
import com.mongodb.reactivestreams.client.FindPublisher
import com.mongodb.{ CursorType, ExplainVerbosity }
import org.mockito.Mockito.{ verify, verifyNoMoreInteractions }
import org.mongodb.scala.model.Collation
import org.reactivestreams.Publisher
import org.scalatestplus.mockito.MockitoSugar

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

class FindObservableSpec extends BaseSpec with MockitoSugar {

  "FindObservable" should "have the same methods as the wrapped FindPublisher" in {
    val mongoPublisher: Set[String] = classOf[Publisher[Document]].getMethods.map(_.getName).toSet
    val wrapped = classOf[FindPublisher[Document]].getMethods.map(_.getName).toSet -- mongoPublisher
    val local = classOf[FindObservable[Document]].getMethods.map(_.getName).toSet

    wrapped.foreach((name: String) => {
      val cleanedName = name.stripPrefix("get")
      assert(local.contains(name) || local.contains(cleanedName.head.toLower + cleanedName.tail), s"Missing: $name")
    })
  }

  it should "call the underlying methods" in {
    val wrapper = mock[FindPublisher[Document]]
    val observable = FindObservable(wrapper)

    val filter = Document("a" -> 1)
    val hint = Document("a" -> 1)
    val hintString = "a_1"
    val duration = Duration(1, TimeUnit.SECONDS)
    val maxDuration = Duration(10, TimeUnit.SECONDS)
    val projection = Document("proj" -> 1)
    val sort = Document("sort" -> 1)
    val collation = Collation.builder().locale("en").build()
    val batchSize = 10
    val ct = classOf[Document]
    val verbosity = ExplainVerbosity.QUERY_PLANNER

    observable.first()
    verify(wrapper).first()

    observable.collation(collation)
    observable.cursorType(CursorType.NonTailable)
    observable.filter(filter)
    observable.hint(hint)
    observable.hintString(hintString)
    observable.limit(1)
    observable.maxAwaitTime(maxDuration)
    observable.maxTime(duration)
    observable.noCursorTimeout(true)
    observable.partial(true)
    observable.projection(projection)
    observable.skip(1)
    observable.sort(sort)
    observable.batchSize(batchSize)
    observable.allowDiskUse(true)
    observable.explain[Document]()
    observable.explain[Document](verbosity)
    observable.timeoutMode(TimeoutMode.ITERATION)

    verify(wrapper).collation(collation)
    verify(wrapper).cursorType(CursorType.NonTailable)
    verify(wrapper).filter(filter)
    verify(wrapper).limit(1)
    verify(wrapper).hint(hint)
    verify(wrapper).hintString(hintString)
    verify(wrapper).maxAwaitTime(maxDuration.toMillis, TimeUnit.MILLISECONDS)
    verify(wrapper).maxTime(duration.toMillis, TimeUnit.MILLISECONDS)
    verify(wrapper).noCursorTimeout(true)
    verify(wrapper).partial(true)
    verify(wrapper).projection(projection)
    verify(wrapper).skip(1)
    verify(wrapper).sort(sort)
    verify(wrapper).batchSize(batchSize)
    verify(wrapper).allowDiskUse(true)
    verify(wrapper).explain(ct)
    verify(wrapper).explain(ct, verbosity)
    verify(wrapper).timeoutMode(TimeoutMode.ITERATION)

    verifyNoMoreInteractions(wrapper)
  }
}
