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

import java.nio.ByteBuffer

import com.mongodb.reactivestreams.client.gridfs.{ GridFSUploadStream => JGridFSUploadStream }
import org.mongodb.scala.BaseSpec
import org.scalamock.scalatest.proxy.MockFactory

class GridFSUploadStreamSpec extends BaseSpec with MockFactory {
  val wrapper = mock[JGridFSUploadStream]
  val gridFSUploadStream = GridFSUploadStream(wrapper)

  "GridFSBucket" should "have the same methods as the wrapped GridFSUploadStream" in {
    val wrapped = classOf[JGridFSUploadStream].getMethods.map(_.getName).toSet
    val local = classOf[GridFSUploadStream].getMethods.map(_.getName).toSet

    wrapped.foreach((name: String) => {
      val cleanedName = name.stripPrefix("get")
      assert(local.contains(name) | local.contains(cleanedName.head.toLower + cleanedName.tail), s"Missing: $name")
    })
  }

  it should "call the underlying methods" in {
    val src = ByteBuffer.allocate(2)

    wrapper.expects(Symbol("getObjectId"))().once()
    wrapper.expects(Symbol("getId"))().once()
    wrapper.expects(Symbol("abort"))().once()
    wrapper.expects(Symbol("write"))(src).once()
    wrapper.expects(Symbol("close"))().once()

    gridFSUploadStream.objectId
    gridFSUploadStream.id
    gridFSUploadStream.abort()
    gridFSUploadStream.write(src)
    gridFSUploadStream.close()
  }

}
