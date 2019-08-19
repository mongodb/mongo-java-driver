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

package com.mongodb.reactivestreams.client.internal

import com.mongodb.async.client.gridfs.GridFSDownloadStream as WrappedGridFSDownloadStream
import org.reactivestreams.Subscriber
import spock.lang.Specification

import java.nio.ByteBuffer

class GridFSDownloadStreamImplSpecification extends Specification {

    def subscriber = Stub(Subscriber) {
        onSubscribe(_) >> { args -> args[0].request(100) }
    }

    def 'should call the underlying getGridFSFile'() {
        when:
        def wrapped = Mock(WrappedGridFSDownloadStream) {
            1 * getGridFSFile(_)
        }
        def downloadStream = new GridFSDownloadStreamImpl(wrapped)

        then:
        downloadStream.getGridFSFile().subscribe(subscriber)
    }

    def 'should call the underlying batchSize'() {
        when:
        def wrapped = Mock(WrappedGridFSDownloadStream) {
            1 * batchSize(10)
        }
        def downloadStream = new GridFSDownloadStreamImpl(wrapped)

        then:
        downloadStream.batchSize(10)
    }

    def 'should call the underlying read'() {
        when:
        def wrapped = Mock(WrappedGridFSDownloadStream) {
            1 * read(ByteBuffer.allocate(2), _)
        }
        def downloadStream = new GridFSDownloadStreamImpl(wrapped)

        then:
        downloadStream.read(ByteBuffer.allocate(2)).subscribe(subscriber)
    }

    def 'should call the underlying close'() {
        when:
        def wrapped = Mock(WrappedGridFSDownloadStream) {
            1 * close(_)
        }
        def downloadStream = new GridFSDownloadStreamImpl(wrapped)

        then:
        downloadStream.close().subscribe(subscriber)
    }

}
