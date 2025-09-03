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
import com.mongodb.client.model.MapReduceAction
import com.mongodb.reactivestreams.client.MapReducePublisher
import org.mockito.Mockito.{ verify, verifyNoMoreInteractions }
import org.mongodb.scala.model.Collation
import org.scalatestplus.mockito.MockitoSugar

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

class MapReduceObservableSpec extends BaseSpec with MockitoSugar {

  "MapReduceObservable" should "have the same methods as the wrapped MapReduceObservable" in {
    val wrapped = classOf[MapReducePublisher[Document]].getMethods.map(_.getName).toSet
    val local = classOf[MapReduceObservable[Document]].getMethods.map(_.getName).toSet

    wrapped.foreach((name: String) => {
      val cleanedName = name.stripPrefix("get")
      assert(local.contains(name) || local.contains(cleanedName.head.toLower + cleanedName.tail), s"Missing: $name")
    })
  }

  it should "call the underlying methods" in {
    val wrapper = mock[MapReducePublisher[Document]]
    val observable = MapReduceObservable(wrapper)

    val filter = Document("a" -> 1)
    val duration = Duration(1, TimeUnit.SECONDS)
    val sort = Document("sort" -> 1)
    val scope = Document("mod" -> 1)
    val collation = Collation.builder().locale("en").build()
    val batchSize = 10

    observable.filter(filter)
    observable.scope(scope)
    observable.sort(sort)
    observable.limit(1)
    observable.maxTime(duration)
    observable.collectionName("collectionName")
    observable.databaseName("databaseName")
    observable.finalizeFunction("final")
    observable.action(MapReduceAction.REPLACE)
    observable.jsMode(true)
    observable.verbose(true)
    observable.bypassDocumentValidation(true)
    observable.collation(collation)
    observable.batchSize(batchSize)
    observable.timeoutMode(TimeoutMode.ITERATION)

    verify(wrapper).filter(filter)
    verify(wrapper).scope(scope)
    verify(wrapper).sort(sort)
    verify(wrapper).limit(1)
    verify(wrapper).maxTime(duration.toMillis, TimeUnit.MILLISECONDS)
    verify(wrapper).collectionName("collectionName")
    verify(wrapper).databaseName("databaseName")
    verify(wrapper).finalizeFunction("final")
    verify(wrapper).action(MapReduceAction.REPLACE)
    verify(wrapper).jsMode(true)
    verify(wrapper).verbose(true)
    verify(wrapper).bypassDocumentValidation(true)
    verify(wrapper).collation(collation)
    verify(wrapper).batchSize(batchSize)
    verify(wrapper).timeoutMode(TimeoutMode.ITERATION)
    verifyNoMoreInteractions(wrapper)

    observable.toCollection()
    verify(wrapper).toCollection
    verifyNoMoreInteractions(wrapper)
  }
}
