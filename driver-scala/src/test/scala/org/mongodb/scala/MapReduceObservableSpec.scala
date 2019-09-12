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

import com.mongodb.client.model.MapReduceAction
import com.mongodb.reactivestreams.client.MapReducePublisher
import org.mongodb.scala.model.Collation
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration.Duration

class MapReduceObservableSpec extends BaseSpec with MockFactory {

  "MapReduceObservable" should "have the same methods as the wrapped MapReduceObservable" in {
    val wrapped = classOf[MapReducePublisher[Document]].getMethods.map(_.getName).toSet
    val local = classOf[MapReduceObservable[Document]].getMethods.map(_.getName).toSet

    wrapped.foreach((name: String) => {
      val cleanedName = name.stripPrefix("get")
      assert(local.contains(name) | local.contains(cleanedName.head.toLower + cleanedName.tail), s"Missing: $name")
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

    wrapper.expects(Symbol("filter"))(filter).once()
    wrapper.expects(Symbol("scope"))(scope).once()
    wrapper.expects(Symbol("sort"))(sort).once()
    wrapper.expects(Symbol("limit"))(1).once()
    wrapper.expects(Symbol("maxTime"))(duration.toMillis, TimeUnit.MILLISECONDS).once()
    wrapper.expects(Symbol("collectionName"))("collectionName").once()
    wrapper.expects(Symbol("databaseName"))("databaseName").once()
    wrapper.expects(Symbol("finalizeFunction"))("final").once()
    wrapper.expects(Symbol("action"))(MapReduceAction.REPLACE).once()
    wrapper.expects(Symbol("jsMode"))(true).once()
    wrapper.expects(Symbol("verbose"))(true).once()
    wrapper.expects(Symbol("sharded"))(true).once()
    wrapper.expects(Symbol("nonAtomic"))(true).once()
    wrapper.expects(Symbol("bypassDocumentValidation"))(true).once()
    wrapper.expects(Symbol("collation"))(collation).once()
    wrapper.expects(Symbol("batchSize"))(batchSize).once()

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
    observable.sharded(true)
    observable.nonAtomic(true)
    observable.bypassDocumentValidation(true)
    observable.collation(collation)
    observable.batchSize(batchSize)

    wrapper.expects(Symbol("toCollection"))().once()
    observable.toCollection()
  }
}
