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
import com.mongodb.reactivestreams.client.ListIndexesPublisher
import org.mockito.Mockito.{ verify, verifyNoMoreInteractions }
import org.reactivestreams.Publisher
import org.scalatestplus.mockito.MockitoSugar

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

class ListIndexesObservableSpec extends BaseSpec with MockitoSugar {

  "ListIndexesObservable" should "have the same methods as the wrapped ListIndexesObservable" in {
    val mongoPublisher: Set[String] = classOf[Publisher[Document]].getMethods.map(_.getName).toSet
    val wrapped = classOf[ListIndexesPublisher[Document]].getMethods.map(_.getName).toSet -- mongoPublisher
    val local = classOf[ListIndexesObservable[Document]].getMethods.map(_.getName).toSet

    wrapped.foreach((name: String) => {
      val cleanedName = name.stripPrefix("get")
      assert(local.contains(name) || local.contains(cleanedName.head.toLower + cleanedName.tail), s"Missing: $name")
    })
  }

  it should "call the underlying methods" in {
    val wrapper = mock[ListIndexesPublisher[Document]]
    val observable = ListIndexesObservable(wrapper)
    val duration = Duration(1, TimeUnit.SECONDS)
    val batchSize = 10

    observable.maxTime(duration)
    observable.batchSize(batchSize)
    observable.timeoutMode(TimeoutMode.ITERATION)

    verify(wrapper).maxTime(duration.toMillis, TimeUnit.MILLISECONDS)
    verify(wrapper).batchSize(batchSize)
    verify(wrapper).timeoutMode(TimeoutMode.ITERATION)

    verifyNoMoreInteractions(wrapper)
  }
}
