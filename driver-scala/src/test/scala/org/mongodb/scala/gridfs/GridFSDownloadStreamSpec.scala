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

import com.mongodb.reactivestreams.client.gridfs.{GridFSDownloadStream => JGridFSDownloadStream}
import org.mongodb.scala.BaseSpec
import org.scalamock.scalatest.proxy.MockFactory

class GridFSDownloadStreamSpec extends BaseSpec with MockFactory {
  val wrapper = mock[JGridFSDownloadStream]
  val gridFSDownloadStream = GridFSDownloadStream(wrapper)

  "GridFSDownloadStream" should "have the same methods as the wrapped GridFSDownloadStream" in {
    val wrapped = classOf[JGridFSDownloadStream].getMethods.map(_.getName).toSet
    val local = classOf[GridFSDownloadStream].getMethods.map(_.getName).toSet

    wrapped.foreach((name: String) => {
      val cleanedName = name.stripPrefix("get")
      assert(local.contains(name) | local.contains(cleanedName.head.toLower + cleanedName.tail), s"Missing: $name")
    })
  }

  it should "call the underlying methods" in {
    val dst = ByteBuffer.allocate(2)
    val batchSize = 10

    wrapper.expects(Symbol("batchSize"))(batchSize).once()
    wrapper.expects(Symbol("getGridFSFile"))().once()
    wrapper.expects(Symbol("read"))(dst).once()
    wrapper.expects(Symbol("skip"))(10L).once()
    wrapper.expects(Symbol("close"))().once()

    gridFSDownloadStream.batchSize(batchSize)
    gridFSDownloadStream.gridFSFile()
    gridFSDownloadStream.read(dst)
    gridFSDownloadStream.skip(10L)
    gridFSDownloadStream.close()
  }

}
