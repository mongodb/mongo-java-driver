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
import com.mongodb.async.AsyncBatchCursor
import com.mongodb.async.FutureResultCallback
import com.mongodb.async.SingleResultCallback
import com.mongodb.async.client.FindIterable
import com.mongodb.async.client.MongoCollection
import com.mongodb.client.gridfs.model.GridFSFile
import org.bson.BsonObjectId
import org.bson.Document
import org.bson.types.Binary
import org.bson.types.ObjectId
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.ByteBuffer
import java.util.concurrent.CountDownLatch

class GridFSDownloadStreamSpecification extends Specification {
    private final static GridFSFile FILE_INFO = new GridFSFile(new BsonObjectId(new ObjectId()), 'filename', 3L, 2, new Date(), 'abc',
            new Document())

    def 'should return the file info'() {
        given:
        def gridFSFindIterable = Mock(GridFSFindIterable)
        def downloadStream = new GridFSDownloadStreamImpl(gridFSFindIterable, Stub(MongoCollection))

        when:
        def futureResult = new FutureResultCallback()
        downloadStream.getGridFSFile(futureResult)

        then:
        1 * gridFSFindIterable.first(_) >> { it[0].onResult(FILE_INFO, null) }
        futureResult.get() == FILE_INFO

        when: 'Ensure that the fileInfo is cached'
        futureResult = new FutureResultCallback()
        downloadStream.getGridFSFile(futureResult)

        then:
        0 * gridFSFindIterable.first(_)
        futureResult.get() == FILE_INFO
    }

    @Unroll
    def 'should return handle errors getting the file info when #description'() {
        given:
        def gridFSFindIterable = Mock(GridFSFindIterable)
        def downloadStream = new GridFSDownloadStreamImpl(gridFSFindIterable, Stub(MongoCollection))

        when:
        def futureResult = new FutureResultCallback()
        downloadStream.getGridFSFile(futureResult)

        then:
        1 * gridFSFindIterable.first(_) >> { it[0].onResult(result, error) }

        when:
        futureResult.get()

        then:
        thrown(MongoException)

        where:
        description                     | result    | error
        'the file info was not found'   | null      | null
        'there was an error'            | null      | new MongoException('failure')
    }

    def 'should query the chunks collection as expected'() {
        given:
        def twoBytes = new byte[2]
        def oneByte = new byte[1]
        def findQuery = new Document('files_id', FILE_INFO.getId()).append('n', new Document('$gte', 0))
        def sort = new Document('n', 1)
        def chunkDocument = new Document('files_id', FILE_INFO.getId())
                .append('n', 0)
                .append('data', new Binary(twoBytes))

        def secondChunkDocument = new Document('files_id', FILE_INFO.getId())
                .append('n', 1)
                .append('data', new Binary(oneByte))

        def gridFSFindIterable = Mock(GridFSFindIterable)
        def batchCursor = Mock(AsyncBatchCursor)
        def findIterable = Mock(FindIterable)
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(gridFSFindIterable, chunksCollection)

        when:
        def firstByteBuffer = ByteBuffer.allocate(2)
        def futureResult = new FutureResultCallback()
        downloadStream.read(firstByteBuffer, futureResult)

        then:
        1 * gridFSFindIterable.first(_) >> { it[0].onResult(FILE_INFO, null) }
        1 * chunksCollection.find(findQuery) >> findIterable
        1 * findIterable.sort(sort) >> findIterable
        1 * findIterable.batchSize(0) >> findIterable
        1 * findIterable.batchCursor(_) >> { it[0].onResult(batchCursor, null) }
        1 * batchCursor.next(_) >> { it[0].onResult([chunkDocument, secondChunkDocument], null) }

        then:
        futureResult.get() == 2
        firstByteBuffer.flip() == ByteBuffer.wrap(twoBytes)

        when:
        def secondByteBuffer = ByteBuffer.allocate(1)
        futureResult = new FutureResultCallback()
        downloadStream.read(secondByteBuffer, futureResult)

        then:
        futureResult.get() == 1
        0 * batchCursor.next(_)
        secondByteBuffer.flip() == ByteBuffer.wrap(oneByte)

        when:
        def thirdByteBuffer = ByteBuffer.allocate(1)
        futureResult = new FutureResultCallback()
        downloadStream.read(thirdByteBuffer, futureResult)

        then:
        futureResult.get() == -1
        0 * batchCursor.next(_)
        thirdByteBuffer == ByteBuffer.allocate(1)
    }

    def 'should create a new cursor each time when using batchSize 1'() {
        given:
        def twoBytes = new byte[2]
        def oneByte = new byte[1]
        def findQuery = new Document('files_id', FILE_INFO.getId()).append('n', new Document('$gte', 0))
        def sort = new Document('n', 1)
        def chunkDocument = new Document('files_id', FILE_INFO.getId())
                .append('n', 0)
                .append('data', new Binary(twoBytes))

        def secondChunkDocument = new Document('files_id', FILE_INFO.getId())
                .append('n', 1)
                .append('data', new Binary(oneByte))

        def gridFSFindIterable = Mock(GridFSFindIterable)
        def batchCursor = Mock(AsyncBatchCursor)
        def findIterable = Mock(FindIterable)
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(gridFSFindIterable, chunksCollection).batchSize(1)

        when:
        def firstByteBuffer = ByteBuffer.allocate(2)
        def futureResult = new FutureResultCallback()
        downloadStream.read(firstByteBuffer, futureResult)

        then:
        1 * gridFSFindIterable.first(_) >> { it[0].onResult(FILE_INFO, null) }
        1 * chunksCollection.find(findQuery) >> findIterable
        1 * findIterable.sort(sort) >> findIterable
        1 * findIterable.batchSize(1) >> findIterable
        1 * findIterable.batchCursor(_) >> { it[0].onResult(batchCursor, null) }
        1 * batchCursor.next(_) >> { it[0].onResult([chunkDocument], null) }

        then:
        futureResult.get() == 2
        firstByteBuffer.flip() == ByteBuffer.wrap(twoBytes)

        when:
        def secondByteBuffer = ByteBuffer.allocate(1)
        futureResult = new FutureResultCallback()
        findQuery.put('n', new Document('$gte', 1))
        downloadStream.read(secondByteBuffer, futureResult)

        then:
        1 * chunksCollection.find(findQuery) >> findIterable
        1 * findIterable.batchSize(1) >> findIterable
        1 * findIterable.sort(sort) >> findIterable
        1 * findIterable.batchCursor(_) >> { it[0].onResult(batchCursor, null) }
        1 * batchCursor.next(_) >> { it[0].onResult([secondChunkDocument], null) }

        then:
        futureResult.get() == 1
        secondByteBuffer.flip() == ByteBuffer.wrap(oneByte)

        when:
        def thirdByteBuffer = ByteBuffer.allocate(1)
        futureResult = new FutureResultCallback()
        downloadStream.read(thirdByteBuffer, futureResult)

        then:
        0 * chunksCollection.find(_) >> findIterable

        then:
        futureResult.get() == -1
        thirdByteBuffer == ByteBuffer.allocate(1)
    }

    def 'should throw if trying to pass negative batchSize'() {
        given:
        def downloadStream = new GridFSDownloadStreamImpl(Stub(GridFSFindIterable), Stub(MongoCollection))

        when:
        downloadStream.batchSize(0)

        then:
        notThrown(IllegalArgumentException)


        when:
        downloadStream.batchSize(-1)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should throw if no chunks found when data is expected'() {
        given:
        def gridFSFindIterable = Mock(GridFSFindIterable)
        def batchCursor = Mock(AsyncBatchCursor)
        def findIterable = Mock(FindIterable)
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(gridFSFindIterable, chunksCollection).batchSize(1)

        when:
        def firstByteBuffer = ByteBuffer.allocate(2)
        def futureResult = new FutureResultCallback()
        downloadStream.read(firstByteBuffer, futureResult)

        then:
        1 * gridFSFindIterable.first(_) >> { it[0].onResult(FILE_INFO, null) }
        1 * chunksCollection.find(_) >> findIterable
        1 * findIterable.sort(_) >> findIterable
        1 * findIterable.batchSize(1) >> findIterable
        1 * findIterable.batchCursor(_) >> { it[0].onResult(batchCursor, null) }
        1 * batchCursor.next(_) >> { it[0].onResult([], null) }

        when:
        futureResult.get()

        then:
        thrown(MongoGridFSException)
    }

    @Unroll
    def 'should propagate any errors getting the batch cursor #description'() {
        given:
        def gridFSFindIterable = Mock(GridFSFindIterable)
        def findIterable = Mock(FindIterable)
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(gridFSFindIterable, chunksCollection)

        when:
        def firstByteBuffer = ByteBuffer.allocate(2)
        def futureResult = new FutureResultCallback()
        downloadStream.read(firstByteBuffer, futureResult)

        then:
        1 * gridFSFindIterable.first(_) >> { it[0].onResult(FILE_INFO, null) }
        1 * chunksCollection.find(_) >> findIterable
        1 * findIterable.sort(_) >> findIterable
        1 * findIterable.batchSize(0) >> findIterable
        1 * findIterable.batchCursor(_) >> { it[0].onResult(result, error) }

        when:
        futureResult.get()

        then:
        thrown(MongoException)

        where:
        description                 | result    | error
        'when there is an error'    | null      | new MongoException('failure')
    }

    @Unroll
    def 'should throw if chunk data #description'() {
        given:
        def gridFSFindIterable = Mock(GridFSFindIterable)
        def batchCursor = Mock(AsyncBatchCursor)
        def findIterable = Mock(FindIterable)
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(gridFSFindIterable, chunksCollection)

        when:
        def firstByteBuffer = ByteBuffer.allocate(2)
        def futureResult = new FutureResultCallback()
        downloadStream.read(firstByteBuffer, futureResult)

        then:
        1 * gridFSFindIterable.first(_) >> { it[0].onResult(FILE_INFO, null) }
        1 * chunksCollection.find(_) >> findIterable
        1 * findIterable.sort(_) >> findIterable
        1 * findIterable.batchSize(0) >> findIterable
        1 * findIterable.batchCursor(_) >> { it[0].onResult(batchCursor, null) }
        1 * batchCursor.next(_) >> { it[0].onResult(result, error) }

        when:
        futureResult.get()

        then:
        thrown(MongoException)

        where:
        description                 | result                                                | error
        'is smaller than expected'  | [Document.parse('{ files_id: 1, n: 0}')
                                               .append('data', new Binary(new byte[1]))]    | null
        'is bigger than expected'   | [Document.parse('{ files_id: 1, n: 0}')
                                               .append('data', new Binary(new byte[100]))]  | null
        'has the wrong n index'     | [Document.parse('{ files_id: 1, n: 1}')
                                               .append('data', new Binary(new byte[3]))]    | null
        'has the wrong data type'   | [Document.parse('{ files_id: 1, n: 0}')
                                               .append('data', 'hello')]                    | null
        'is empty'                  | []                                                    | null
        'is null'                   | null                                                  | null
        'has an error'              | null                                                  | new MongoException('failure')
    }

    def 'should throw an exception when trying to action post close'() {
        given:
        def downloadStream = new GridFSDownloadStreamImpl(Stub(GridFSFindIterable), Stub(MongoCollection))
        def futureResult = new FutureResultCallback()
        downloadStream.close(futureResult)
        futureResult.get()

        when:
        futureResult = new FutureResultCallback()
        downloadStream.read(ByteBuffer.allocate(1), futureResult)
        futureResult.get()

        then:
        thrown(MongoGridFSException)

        when:
        futureResult = new FutureResultCallback()
        downloadStream.getGridFSFile(futureResult)
        futureResult.get()

        then:
        thrown(MongoGridFSException)

        when:
        futureResult = new FutureResultCallback()
        downloadStream.close(futureResult)
        futureResult.get()

        then:
        notThrown(MongoGridFSException)
    }

    def 'should not allow concurrent reads'() {
        given:
        def latchA = new CountDownLatch(1)
        def latchB = new CountDownLatch(1)
        def gridFSFindIterable = Mock(GridFSFindIterable) {
            1 * first(_) >> {
                latchA.countDown()
                latchB.await()
                it[0].onResult(null, null)
            }
        }
        def downloadStream = new GridFSDownloadStreamImpl(gridFSFindIterable, Stub(MongoCollection))
        def futureResult = new FutureResultCallback()

        when:
        Thread.start { downloadStream.read(ByteBuffer.allocate(100), Stub(SingleResultCallback)) }
        Thread.start {
            latchA.await()
            downloadStream.read(ByteBuffer.allocate(100), new SingleResultCallback<Integer>() {
                @Override
                void onResult(final Integer result, final Throwable t) {
                    latchB.countDown()
                    futureResult.onResult(result, t)
                }
            })
        }
        futureResult.get()

        then:
        def exception = thrown(MongoGridFSException)
        exception.getMessage() == 'The AsyncInputStream does not support concurrent reading.'
    }

    def 'should not allow a concurrent read and close'() {
        given:
        def latchA = new CountDownLatch(1)
        def latchB = new CountDownLatch(1)
        def gridFSFindIterable = Mock(GridFSFindIterable) {
            1 * first(_) >> {
                latchA.countDown()
                latchB.await()
                it[0].onResult(null, null)
            }
        }
        def downloadStream = new GridFSDownloadStreamImpl(gridFSFindIterable, Stub(MongoCollection))
        def futureResult = new FutureResultCallback()

        when:
        Thread.start { downloadStream.read(ByteBuffer.allocate(100), Stub(SingleResultCallback)) }
        Thread.start {
            latchA.await()
            downloadStream.close(new SingleResultCallback<Integer>() {
                @Override
                void onResult(final Integer result, final Throwable t) {
                    latchB.countDown()
                    futureResult.onResult(result, t)
                }
            })
        }
        futureResult.get()

        then:
        def exception = thrown(MongoGridFSException)
        exception.getMessage() == 'The AsyncInputStream does not support concurrent reading.'
    }
}
