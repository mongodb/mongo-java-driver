/*
 * Copyright 2016 MongoDB, Inc.
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

import com.mongodb.async.client.gridfs.GridFSUploadStream as WrappedGridFSUploadStream
import org.bson.BsonObjectId
import org.reactivestreams.Subscriber
import spock.lang.Specification

import java.nio.ByteBuffer

class GridFSUploadStreamImplSpecification extends Specification {

    def subscriber = Stub(Subscriber) {
        onSubscribe(_) >> { args -> args[0].request(100) }
    }
    def fileId = new BsonObjectId()
    def content = 'file content ' as byte[]

    def 'should call the underlying getId'() {
        when:
        def wrapped = Mock(WrappedGridFSUploadStream) {
            1 * getId() >> { fileId }
        }
        def uploadStream = new GridFSUploadStreamImpl(wrapped)

        then:
        uploadStream.getId() == fileId
    }

    def 'should call the underlying getObjectId'() {
        when:
        def wrapped = Mock(WrappedGridFSUploadStream) {
            1 * getObjectId() >> { fileId.getValue() }
        }
        def uploadStream = new GridFSUploadStreamImpl(wrapped)

        then:
        uploadStream.getObjectId() == fileId.getValue()
    }

    def 'should call the underlying write'() {
        when:
        def wrapped = Mock(WrappedGridFSUploadStream) {
            1 * write(ByteBuffer.wrap(content), _)
        }
        def uploadStream = new GridFSUploadStreamImpl(wrapped)

        then:
        uploadStream.write(ByteBuffer.wrap(content)).subscribe(subscriber)
    }

    def 'should call the underlying abort'() {
        when:
        def wrapped = Mock(WrappedGridFSUploadStream) {
            1 * abort(_)
        }
        def uploadStream = new GridFSUploadStreamImpl(wrapped)

        then:
        uploadStream.abort().subscribe(subscriber)
    }

    def 'should call the underlying close'() {
        when:
        def wrapped = Mock(WrappedGridFSUploadStream) {
            1 * close(_)
        }
        def uploadStream = new GridFSUploadStreamImpl(wrapped)

        then:
        uploadStream.close().subscribe(subscriber)
    }

}
