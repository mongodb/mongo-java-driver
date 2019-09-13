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

package org.mongodb.scala.gridfs

import java.util.concurrent.TimeUnit

import com.mongodb.reactivestreams.client.gridfs.GridFSFindPublisher
import org.mongodb.scala.{ BaseSpec, Document }
import org.reactivestreams.Publisher
import org.scalamock.scalatest.proxy.MockFactory

import scala.concurrent.duration.Duration

class GridFSFindObservableSpec extends BaseSpec with MockFactory {
  val wrapper = mock[GridFSFindPublisher]
  val gridFSFindObservable = GridFSFindObservable(wrapper)

  "GridFSFindObservable" should "have the same methods as the wrapped GridFSFindPublisher" in {
    val mongoPublisher: Set[String] = classOf[Publisher[Document]].getMethods.map(_.getName).toSet
    val wrapped = classOf[GridFSFindPublisher].getMethods.map(_.getName).toSet -- mongoPublisher - "collation"
    val local = classOf[GridFSFindObservable].getMethods.map(_.getName).toSet

    wrapped.foreach((name: String) => {
      val cleanedName = name.stripPrefix("get")
      assert(local.contains(name) | local.contains(cleanedName.head.toLower + cleanedName.tail), s"Missing: $name")
    })
  }

  it should "call the underlying methods" in {
    val batchSize = 20
    val filter = Document("{a: 1}")
    val limit = 10
    val maxTime = Duration(10, "second") //scalatyle:ignore
    val skip = 5
    val sort = Document("{_id: 1}")

    wrapper.expects(Symbol("batchSize"))(batchSize).once()
    wrapper.expects(Symbol("filter"))(filter).once()
    wrapper.expects(Symbol("limit"))(limit).once()
    wrapper.expects(Symbol("maxTime"))(maxTime.toMillis, TimeUnit.MILLISECONDS).once()
    wrapper.expects(Symbol("noCursorTimeout"))(true).once()
    wrapper.expects(Symbol("skip"))(skip).once()
    wrapper.expects(Symbol("sort"))(sort).once()

    gridFSFindObservable.batchSize(batchSize)
    gridFSFindObservable.filter(filter)
    gridFSFindObservable.limit(limit)
    gridFSFindObservable.maxTime(maxTime)
    gridFSFindObservable.noCursorTimeout(true)
    gridFSFindObservable.skip(skip)
    gridFSFindObservable.sort(sort)
  }

}
