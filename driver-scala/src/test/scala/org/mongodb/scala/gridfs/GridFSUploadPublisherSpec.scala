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

import com.mongodb.reactivestreams.client.gridfs.GridFSUploadPublisher
import org.mockito.Mockito.verify
import org.mongodb.scala.BaseSpec
import org.scalatestplus.mockito.MockitoSugar

class GridFSUploadPublisherSpec extends BaseSpec with MockitoSugar {
  val wrapper = mock[GridFSUploadPublisher[Unit]]
  val gridFSUploadObservable = GridFSUploadObservable(wrapper)

  "GridFSBucket" should "have the same methods as the wrapped GridFSUploadStream" in {
    val wrapped = classOf[GridFSUploadPublisher[Unit]].getMethods.map(_.getName).toSet
    val local = classOf[GridFSUploadObservable[Unit]].getMethods.map(_.getName).toSet

    wrapped.foreach((name: String) => {
      val cleanedName = name.stripPrefix("get")
      assert(local.contains(name) || local.contains(cleanedName.head.toLower + cleanedName.tail), s"Missing: $name")
    })
  }

  it should "call the underlying methods" in {

    gridFSUploadObservable.objectId
    gridFSUploadObservable.id

    verify(wrapper).getObjectId
    verify(wrapper).getId
  }

}
