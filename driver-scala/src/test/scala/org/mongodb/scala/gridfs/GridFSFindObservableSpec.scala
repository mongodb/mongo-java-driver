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
import org.mockito.Mockito.verify
import org.mongodb.scala.{ BaseSpec, Document }
import org.reactivestreams.Publisher
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.duration.Duration

class GridFSFindObservableSpec extends BaseSpec with MockitoSugar {
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
    val maxTime = Duration(10, "second") // scalatyle:ignore
    val skip = 5
    val sort = Document("{_id: 1}")

    gridFSFindObservable.batchSize(batchSize)
    gridFSFindObservable.filter(filter)
    gridFSFindObservable.limit(limit)
    gridFSFindObservable.maxTime(maxTime)
    gridFSFindObservable.noCursorTimeout(true)
    gridFSFindObservable.skip(skip)
    gridFSFindObservable.sort(sort)

    verify(wrapper).batchSize(batchSize)
    verify(wrapper).filter(filter)
    verify(wrapper).limit(limit)
    verify(wrapper).maxTime(maxTime.toMillis, TimeUnit.MILLISECONDS)
    verify(wrapper).noCursorTimeout(true)
    verify(wrapper).skip(skip)
    verify(wrapper).sort(sort)
  }

}
