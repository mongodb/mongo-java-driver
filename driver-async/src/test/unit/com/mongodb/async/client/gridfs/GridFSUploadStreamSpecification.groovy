/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.async.client.gridfs

import com.mongodb.MongoException
import com.mongodb.MongoGridFSException
import com.mongodb.async.FutureResultCallback
import com.mongodb.async.SingleResultCallback
import com.mongodb.async.client.MongoCollection
import com.mongodb.session.ClientSession
import org.bson.BsonObjectId
import org.bson.BsonString
import org.bson.Document
import org.bson.types.Binary
import spock.lang.Specification

import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.concurrent.CountDownLatch

class GridFSUploadStreamSpecification extends Specification {
    def fileId = new BsonObjectId()
    def filename = 'filename'
    def metadata = new Document()
    def content = 'file content ' as byte[]

    def 'should return the file id'() {
        when:
        def uploadStream = new GridFSUploadStreamImpl(clientSession, Stub(MongoCollection), Stub(MongoCollection), fileId, filename, 255,
                metadata, NOOP_INDEXCHECK)

        then:
        uploadStream.getId() == fileId

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should write the buffer it reaches the chunk size'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def callback = Stub(SingleResultCallback)
        def uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection, chunksCollection, fileId, filename, 2, metadata,
                NOOP_INDEXCHECK)

        when:
        uploadStream.write(ByteBuffer.wrap(new byte[1]), callback)

        then:
        0 * chunksCollection.insertOne(*_)

        when:
        uploadStream.write(ByteBuffer.wrap(new byte[1]), callback)

        then:
        if (clientSession != null) {
            1 * chunksCollection.insertOne(clientSession, _, _)
        } else {
            1 * chunksCollection.insertOne(_, _)
        }

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should write to the files collection on close'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def callback = Stub(SingleResultCallback)
        def uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection, chunksCollection, fileId, filename, 255, null,
                NOOP_INDEXCHECK)
        def byteBuffer = ByteBuffer.wrap(new byte[10])

        when:
        uploadStream.write(byteBuffer, callback)

        then:
        0 * chunksCollection.insertOne(*_)

        when:
        uploadStream.close(callback)

        then:
        if (clientSession != null) {
            1 * chunksCollection.insertOne(clientSession, _, _) >> { it.last().onResult(null, null) }
            1 * filesCollection.insertOne(clientSession, _, _)
        } else {
            1 * chunksCollection.insertOne(_, _) >> { it.last().onResult(null, null) }
            1 * filesCollection.insertOne(_, _)
        }

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should write to the files and chunks collection as expected on close'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def callback = Stub(SingleResultCallback)
        def metadata = new Document('contentType', 'text/txt')
        def uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection, chunksCollection, fileId, filename, 255, metadata,
                NOOP_INDEXCHECK)
        def chunksData
        def fileData

        when:
        uploadStream.write(ByteBuffer.wrap(content), callback)
        uploadStream.close(callback)

        then:
        if (clientSession != null) {
            1 * chunksCollection.insertOne(clientSession, _, _) >> {
                chunksData = it[1]
                it.last().onResult(null, null)
            }

            1 * filesCollection.insertOne(clientSession, _, _) >> {
                fileData = it[1]
                it.last().onResult(null, null)
            }
        } else {
            1 * chunksCollection.insertOne(_, _) >> {
                chunksData = it[0]
                it.last().onResult(null, null)
            }

            1 * filesCollection.insertOne(_, _) >> {
                fileData = it[0]
                it.last().onResult(null, null)
            }
        }

        then:
        chunksData.get('files_id') == fileId
        chunksData.getInteger('n') == 0
        chunksData.get('data', Binary).getData() == content

        fileData.getId() == fileId
        fileData.getFilename() == filename
        fileData.getLength() == content.length as Long
        fileData.getChunkSize() == 255
        fileData.getMD5() == MessageDigest.getInstance('MD5').digest(content).encodeHex().toString()
        fileData.getMetadata() == metadata

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should not write an empty chunk'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def callback = Stub(SingleResultCallback)
        def uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection, chunksCollection, fileId, filename, 255, metadata,
                NOOP_INDEXCHECK)

        when:
        uploadStream.close(callback)

        then:
        0 * chunksCollection.insertOne(*_)
        if (clientSession != null) {
            1 * filesCollection.insertOne(clientSession, _, _)
        } else {
            1 * filesCollection.insertOne(_, _)
        }

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should delete any chunks when calling abort'() {
        given:
        def chunksCollection = Mock(MongoCollection)
        def callback = Stub(SingleResultCallback)
        def uploadStream = new GridFSUploadStreamImpl(clientSession, Stub(MongoCollection), chunksCollection, fileId, filename, 255,
                metadata, NOOP_INDEXCHECK)

        when:
        uploadStream.write(ByteBuffer.wrap(content), callback)
        uploadStream.abort(callback)

        then:
        if (clientSession != null) {
            1 * chunksCollection.deleteMany(clientSession, new Document('files_id', fileId), _)
        } else {
            1 * chunksCollection.deleteMany(new Document('files_id', fileId), _)
        }

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should close the stream on abort'() {
        given:
        def callback = Stub(SingleResultCallback)
        def uploadStream = new GridFSUploadStreamImpl(clientSession, Stub(MongoCollection), Stub(MongoCollection), fileId, filename, 255,
                metadata, NOOP_INDEXCHECK)
        uploadStream.write(ByteBuffer.wrap(content), callback)
        uploadStream.abort(callback)

        when:
        def futureResults = new FutureResultCallback()
        uploadStream.write(ByteBuffer.wrap(content), futureResults)
        futureResults.get()

        then:
        thrown(MongoGridFSException)

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should throw an exception when trying to action post close'() {
        given:
        def callback = Stub(SingleResultCallback)
        def uploadStream = new GridFSUploadStreamImpl(clientSession, Stub(MongoCollection), Stub(MongoCollection), fileId, filename, 255,
                metadata, NOOP_INDEXCHECK)
        uploadStream.close(callback)

        when:
        def futureResults = new FutureResultCallback()
        uploadStream.write(ByteBuffer.wrap(content), futureResults)
        futureResults.get()

        then:
        thrown(MongoGridFSException)

        when:
        futureResults = new FutureResultCallback()
        uploadStream.abort(futureResults)
        futureResults.get()

        then:
        thrown(MongoGridFSException)

        when: 'Multiple calls to close are ok'
        futureResults = new FutureResultCallback()
        uploadStream.close(futureResults)
        futureResults.get()

        then:
        notThrown(MongoGridFSException)

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should throw an exception when calling getObjectId and the fileId is not an ObjectId'() {
        given:
        def fileId = new BsonString('myFile')
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection, chunksCollection, fileId, filename, 255, metadata,
                NOOP_INDEXCHECK)

        when:
        uploadStream.getObjectId()

        then:
        thrown(MongoGridFSException)

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should not allow concurrent writes'() {
        given:
        def latchA = new CountDownLatch(1)
        def latchB = new CountDownLatch(1)
        def chunksCollection = Mock(MongoCollection)
        def uploadStream = new GridFSUploadStreamImpl(clientSession, Stub(MongoCollection), chunksCollection, fileId, filename, 255,
                metadata, NOOP_INDEXCHECK)

        when:
        def futureResult = new FutureResultCallback()

        Thread.start { uploadStream.write(ByteBuffer.allocate(255), Stub(SingleResultCallback)) }
        Thread.start {
            latchA.await()
            uploadStream.write(ByteBuffer.allocate(100), new SingleResultCallback<Integer>() {
                @Override
                void onResult(final Integer result, final Throwable t) {
                    latchB.countDown()
                    futureResult.onResult(result, t)
                }
            })
        }
        futureResult.get()

        then:
        if (clientSession != null ) {
            1 * chunksCollection.insertOne(clientSession, _, _) >> {
                latchA.countDown()
                latchB.await()
                it.last().onResult(null, null)
            }
        } else {
            1 * chunksCollection.insertOne(_, _) >> {
                latchA.countDown()
                latchB.await()
                it.last().onResult(null, null)
            }
        }

        then:
        def exception = thrown(MongoGridFSException)
        exception.getMessage() == 'The AsyncOutputStream does not support concurrent writing.'

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should not allow a concurrent write and close'() {
        given:
        def latchA = new CountDownLatch(1)
        def latchB = new CountDownLatch(1)
        def chunksCollection = Mock(MongoCollection)
        def uploadStream = new GridFSUploadStreamImpl(clientSession, Stub(MongoCollection), chunksCollection, fileId, filename, 255,
                metadata, NOOP_INDEXCHECK)

        when:
        def futureResult = new FutureResultCallback()

        Thread.start { uploadStream.write(ByteBuffer.allocate(255), Stub(SingleResultCallback)) }
        Thread.start {
            latchA.await()
            uploadStream.close(new SingleResultCallback<Integer>() {
                @Override
                void onResult(final Integer result, final Throwable t) {
                    latchB.countDown()
                    futureResult.onResult(result, t)
                }
            })
        }
        futureResult.get()

        then:
        if (clientSession != null ) {
            1 * chunksCollection.insertOne(clientSession, _, _) >> {
                latchA.countDown()
                latchB.await()
                it.last().onResult(null, null)
            }
        } else {
            1 * chunksCollection.insertOne(_, _) >> {
                latchA.countDown()
                latchB.await()
                it.last().onResult(null, null)
            }
        }

        then:
        def exception = thrown(MongoGridFSException)
        exception.getMessage() == 'The AsyncOutputStream does not support concurrent writing.'

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should not allow concurrent write then abort'() {
        given:
        def latchA = new CountDownLatch(1)
        def latchB = new CountDownLatch(1)
        def chunksCollection = Mock(MongoCollection)
        def uploadStream = new GridFSUploadStreamImpl(clientSession, Stub(MongoCollection), chunksCollection, fileId, filename, 255,
                metadata, NOOP_INDEXCHECK)

        when:
        def futureResult = new FutureResultCallback()

        Thread.start { uploadStream.write(ByteBuffer.allocate(255), Stub(SingleResultCallback)) }
        Thread.start {
            latchA.await()
            uploadStream.abort(new SingleResultCallback<Integer>() {
                @Override
                void onResult(final Integer result, final Throwable t) {
                    latchB.countDown()
                    futureResult.onResult(result, t)
                }
            })
        }
        futureResult.get()

        then:
        if (clientSession != null ) {
            1 * chunksCollection.insertOne(clientSession, _, _) >> {
                latchA.countDown()
                latchB.await()
                it.last().onResult(null, null)
            }
        } else {
            1 * chunksCollection.insertOne(_, _) >> {
                latchA.countDown()
                latchB.await()
                it.last().onResult(null, null)
            }
        }

        then:
        def exception = thrown(MongoGridFSException)
        exception.getMessage() == 'The AsyncOutputStream does not support concurrent writing.'

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should propagate exceptions when writing'() {
        given:
        def chunksCollection = Mock(MongoCollection)
        def alternativeException = new MongoException('Alternative failure')
        def uploadStream = new GridFSUploadStreamImpl(clientSession, Stub(MongoCollection), chunksCollection, fileId, filename, 255,
                metadata, NOOP_INDEXCHECK)

        when:
        def futureResult = new FutureResultCallback()
        uploadStream.write(ByteBuffer.allocate(255), futureResult)

        then:
        if (clientSession != null) {
            1 * chunksCollection.insertOne(clientSession, _, _) >> { it.last().onResult(null, alternativeException) }
        } else {
            1 * chunksCollection.insertOne(_, _) >> { it.last().onResult(null, alternativeException) }
        }

        when:
        futureResult.get()

        then:
        def exception = thrown(MongoException)
        exception == alternativeException

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should propagate exceptions when closing'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def closeException = new MongoException('Alternative failure')

        when: 'The insert to the chunks collection fails'
        def futureResult = new FutureResultCallback()
        def uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection, chunksCollection, fileId, filename, 255, metadata,
                NOOP_INDEXCHECK)
        uploadStream.write(ByteBuffer.wrap(content), Stub(SingleResultCallback))
        uploadStream.close(futureResult)

        then:
        if (clientSession != null) {
            1 * chunksCollection.insertOne(clientSession, _, _) >> { it.last().onResult(null, closeException) }
        } else {
            1 * chunksCollection.insertOne(_, _) >> { it.last().onResult(null, closeException) }
        }

        when:
        futureResult.get()

        then:
        def exception = thrown(MongoException)
        exception == closeException

        when: 'The insert to the files collection fails'
        futureResult = new FutureResultCallback()
        uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection, chunksCollection, fileId, filename, 255, metadata,
                NOOP_INDEXCHECK)
        uploadStream.write(ByteBuffer.wrap(content), Stub(SingleResultCallback))
        uploadStream.close(futureResult)

        then:
        if (clientSession != null) {
            1 * chunksCollection.insertOne(clientSession, _, _) >> { it.last().onResult(null, null) }
            1 * filesCollection.insertOne(clientSession, _, _) >> { it.last().onResult(null, closeException) }
        } else {
            1 * chunksCollection.insertOne(_, _) >> { it.last().onResult(null, null) }
            1 * filesCollection.insertOne(_, _) >> { it.last().onResult(null, closeException) }
        }

        when:
        futureResult.get()

        then:
        exception = thrown(MongoException)
        exception == closeException

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should propagate exceptions when aborting'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def abortException = new MongoException('Alternative failure')
        def uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection, chunksCollection, fileId, filename, 255, metadata,
                NOOP_INDEXCHECK)
        def futureResult = new FutureResultCallback()

        when:
        uploadStream.abort(futureResult)

        then:
        if (clientSession != null) {
            1 * chunksCollection.deleteMany(clientSession, _, _) >> { it.last().onResult(null, abortException) }
        } else {
            1 * chunksCollection.deleteMany(_, _) >> { it.last().onResult(null, abortException) }
        }

        when:
        futureResult.get()

        then:
        def exception = thrown(MongoException)
        exception == abortException

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should propagate exceptions when creating indexes'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def indexException = new MongoException('Alternative failure')
        def indexCheck = Mock(GridFSIndexCheck) {
            1 * checkAndCreateIndex(_) >> { it.last().onResult(null, indexException) }
        }
        def uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection, chunksCollection, fileId, filename, 255, metadata,
                indexCheck)
        def futureResult = new FutureResultCallback()

        when:
        uploadStream.write(ByteBuffer.allocate(10), futureResult)
        futureResult.get()

        then:
        def exception = thrown(MongoException)
        exception == indexException

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    private final static GridFSIndexCheck NOOP_INDEXCHECK = new GridFSIndexCheck() {
        @Override
        void checkAndCreateIndex(final SingleResultCallback<Void> callback) {
            callback.onResult(null, null)
        }
    }
}
