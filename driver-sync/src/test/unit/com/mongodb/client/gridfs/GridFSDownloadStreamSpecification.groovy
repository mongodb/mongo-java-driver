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

package com.mongodb.client.gridfs

import com.mongodb.MongoGridFSException
import com.mongodb.client.FindIterable
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoCursor
import com.mongodb.client.gridfs.model.GridFSFile
import com.mongodb.client.ClientSession
import org.bson.BsonObjectId
import org.bson.Document
import org.bson.types.Binary
import org.bson.types.ObjectId
import spock.lang.Specification

class GridFSDownloadStreamSpecification extends Specification {
    def fileInfo = new GridFSFile(new BsonObjectId(new ObjectId()), 'filename', 3L, 2, new Date(), 'abc', new Document())

    def 'should return the file info'() {
        when:
        def downloadStream = new GridFSDownloadStreamImpl(null, fileInfo, Stub(MongoCollection))

        then:
        downloadStream.getGridFSFile() == fileInfo
    }

    def 'should query the chunks collection as expected'() {
        when:
        def twoBytes = new byte[2]
        def oneByte = new byte[1]
        def findQuery = new Document('files_id', fileInfo.getId()).append('n', new Document('$gte', 0))
        def sort = new Document('n', 1)
        def chunkDocument = new Document('files_id', fileInfo.getId())
                .append('n', 0)
                .append('data', new Binary(twoBytes))

        def secondChunkDocument = new Document('files_id', fileInfo.getId())
                .append('n', 1)
                .append('data', new Binary(oneByte))

        def mongoCursor = Mock(MongoCursor)
        def findIterable = Mock(FindIterable)
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(clientSession, fileInfo, chunksCollection)

        then:
        downloadStream.available() == 0

        when:
        def result = downloadStream.read()

        then:
        result == (twoBytes[0] & 0xFF)
        if (clientSession != null) {
            1 * chunksCollection.find(clientSession, findQuery) >> findIterable
        } else {
            1 * chunksCollection.find(findQuery) >> findIterable
        }
        1 * findIterable.sort(sort) >> findIterable
        1 * findIterable.batchSize(0) >> findIterable
        1 * findIterable.iterator() >> mongoCursor

        then:
        1 * mongoCursor.hasNext() >> true
        1 * mongoCursor.next() >> chunkDocument

        downloadStream.available() == 1

        when:
        result = downloadStream.read()

        then:
        result == (twoBytes[1] & 0xFF)
        0 * mongoCursor.hasNext()
        0 * mongoCursor.next()
        downloadStream.available() == 0

        when:
        result = downloadStream.read()

        then:
        result == (oneByte[0] & 0xFF)
        1 * mongoCursor.hasNext() >> true
        1 * mongoCursor.next() >> secondChunkDocument

        when:
        result = downloadStream.read()

        then:
        result == -1

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should create a new cursor each time when using batchSize 1'() {
        when:
        def twoBytes = new byte[2]
        def oneByte = new byte[1]
        def findQuery = new Document('files_id', fileInfo.getId()).append('n', new Document('$gte', 0))
        def secondFindQuery = new Document('files_id', fileInfo.getId()).append('n', new Document('$gte', 1))
        def sort = new Document('n', 1)
        def chunkDocument = new Document('files_id', fileInfo.getId())
                .append('n', 0)
                .append('data', new Binary(twoBytes))

        def secondChunkDocument = new Document('files_id', fileInfo.getId())
                .append('n', 1)
                .append('data', new Binary(oneByte))

        def mongoCursor = Mock(MongoCursor)
        def findIterable = Mock(FindIterable)
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(clientSession, fileInfo, chunksCollection).batchSize(1)

        then:
        downloadStream.available() == 0

        when:
        def result = downloadStream.read()

        then:
        result == (twoBytes[0] & 0xFF)
        if (clientSession != null) {
            1 * chunksCollection.find(clientSession, findQuery) >> findIterable
        } else {
            1 * chunksCollection.find(findQuery) >> findIterable
        }
        1 * findIterable.sort(sort) >> findIterable
        1 * findIterable.batchSize(1) >> findIterable
        1 * findIterable.iterator() >> mongoCursor

        then:
        1 * mongoCursor.hasNext() >> true
        1 * mongoCursor.next() >> chunkDocument

        downloadStream.available() == 1

        when:
        result = downloadStream.read()

        then:
        result == (twoBytes[1] & 0xFF)
        0 * mongoCursor.hasNext()
        0 * mongoCursor.next()
        downloadStream.available() == 0

        when:
        result = downloadStream.read()

        then:
        result == (oneByte[0] & 0xFF)
        if (clientSession != null) {
            1 * chunksCollection.find(clientSession, secondFindQuery) >> findIterable
        } else {
            1 * chunksCollection.find(secondFindQuery) >> findIterable
        }
        1 * findIterable.sort(sort) >> findIterable
        1 * findIterable.batchSize(1) >> findIterable
        1 * findIterable.iterator() >> mongoCursor
        1 * mongoCursor.hasNext() >> true
        1 * mongoCursor.next() >> secondChunkDocument

        when:
        result = downloadStream.read()

        then:
        result == -1

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should skip to the correct point'() {
        given:
        def fileInfo = new GridFSFile(new BsonObjectId(new ObjectId()), 'filename', 4194297L, 32,
                new Date(), 'abc', new Document())

        def firstChunkBytes = 1..32 as byte[]
        def lastChunkBytes = 33 .. 57 as byte[]

        def sort = new Document('n', 1)

        def findQueries = [new Document('files_id', fileInfo.getId()).append('n', new Document('$gte', 0)),
                           new Document('files_id', fileInfo.getId()).append('n', new Document('$gte', 131071))]
        def chunkDocuments =
                [new Document('files_id', fileInfo.getId()).append('n', 0).append('data', new Binary(firstChunkBytes)),
                 new Document('files_id', fileInfo.getId()).append('n', 131071).append('data', new Binary(lastChunkBytes))]

        def mongoCursor = Mock(MongoCursor)
        def findIterable = Mock(FindIterable)
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(clientSession, fileInfo, chunksCollection)

        when:
        def skipResult = downloadStream.skip(15)

        then:
        skipResult == 15L
        0 * chunksCollection.find(*_)

        when:
        def readByte = new byte[5]
        downloadStream.read(readByte)

        then:
        if (clientSession != null) {
            1 * chunksCollection.find(clientSession, findQueries[0]) >> findIterable
        } else {
            1 * chunksCollection.find(findQueries[0]) >> findIterable
        }
        1 * findIterable.sort(sort) >> findIterable
        1 * findIterable.batchSize(0) >> findIterable
        1 * findIterable.iterator() >> mongoCursor
        1 * mongoCursor.hasNext() >> true
        1 * mongoCursor.next() >> chunkDocuments[0]

        then:
        readByte == [16, 17, 18, 19, 20] as byte[]

        when:
        skipResult = downloadStream.skip(4194272)

        then:
        skipResult == 4194272L
        0 * chunksCollection.find(*_)

        when:
        downloadStream.read(readByte)

        then:
        if (clientSession != null) {
            1 * chunksCollection.find(clientSession, findQueries[1]) >> findIterable
        } else {
            1 * chunksCollection.find(findQueries[1]) >> findIterable
        }
        1 * findIterable.sort(sort) >> findIterable
        1 * findIterable.batchSize(0) >> findIterable
        1 * findIterable.iterator() >> mongoCursor
        1 * mongoCursor.hasNext() >> true
        1 * mongoCursor.next() >> chunkDocuments[1]

        then:
        readByte == [53, 54, 55, 56, 57] as byte[]

        when:
        skipResult = downloadStream.skip(1)

        then:
        skipResult == 0L
        0 * chunksCollection.find(*_)

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should mark and reset to the correct point'() {
        given:
        def fileInfo = new GridFSFile(new BsonObjectId(new ObjectId()), 'filename', 25L, 25, new Date(), 'abc', new Document())

        def expected10Bytes = 11 .. 20 as byte[]
        def firstChunkBytes = 1..25 as byte[]

        def chunkDocument = new Document('files_id', fileInfo.getId()).append('n', 0).append('data', new Binary(firstChunkBytes))

        def mongoCursor = Mock(MongoCursor)
        def findIterable = Mock(FindIterable)
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(clientSession, fileInfo, chunksCollection)

        when:
        def readByte = new byte[10]
        downloadStream.read(readByte)

        then:
        if (clientSession != null) {
            1 * chunksCollection.find(clientSession, _) >> findIterable
        } else {
            1 * chunksCollection.find(_) >> findIterable
        }
        1 * findIterable.sort(_) >> findIterable
        1 * findIterable.batchSize(0) >> findIterable
        1 * findIterable.iterator() >> mongoCursor
        1 * mongoCursor.hasNext() >> true
        1 * mongoCursor.next() >> chunkDocument

        then:
        readByte == 1 .. 10 as byte[]

        when:
        downloadStream.mark()

        then:
        0 * chunksCollection.find(*_)

        when:
        downloadStream.read(readByte)

        then:
        readByte == expected10Bytes

        when:
        downloadStream.reset()

        then:
        0 * chunksCollection.find(*_)

        when:
        downloadStream.read(readByte)

        then:
        0 * chunksCollection.find(*_)
        readByte == expected10Bytes

        where:
        clientSession << [null, Stub(ClientSession)]
    }


    def 'should mark and reset across chunks'() {
        given:
        def fileInfo = new GridFSFile(new BsonObjectId(new ObjectId()), 'filename', 50L, 25, new Date(), 'abc', new Document())

        def firstChunkBytes = 1..25 as byte[]
        def secondChunkBytes = 26 .. 50 as byte[]

        def chunkDocuments =
                [new Document('files_id', fileInfo.getId()).append('n', 0).append('data', new Binary(firstChunkBytes)),
                 new Document('files_id', fileInfo.getId()).append('n', 1).append('data', new Binary(secondChunkBytes))]

        def mongoCursor = Mock(MongoCursor)
        def findIterable = Mock(FindIterable)
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(clientSession, fileInfo, chunksCollection)

        when:
        downloadStream.mark()
        def readByte = new byte[25]
        downloadStream.read(readByte)

        then:
        if (clientSession != null) {
            1 * chunksCollection.find(clientSession, _) >> findIterable
        } else {
            1 * chunksCollection.find(_) >> findIterable
        }
        1 * findIterable.sort(_) >> findIterable
        1 * findIterable.batchSize(0) >> findIterable
        1 * findIterable.iterator() >> mongoCursor
        1 * mongoCursor.hasNext() >> true
        1 * mongoCursor.next() >> chunkDocuments[0]

        then:
        readByte == firstChunkBytes

        then:
        0 * chunksCollection.find(*_)

        when:
        downloadStream.read(readByte)

        then:
        readByte == secondChunkBytes
        1 * mongoCursor.hasNext() >> true
        1 * mongoCursor.next() >> chunkDocuments[1]

        when: 'check read to EOF'
        def result = downloadStream.read(readByte)

        then:
        result == -1

        when:
        downloadStream.reset()

        then:
        0 * chunksCollection.find(*_)

        when:
        downloadStream.read(readByte)

        then:
        readByte == firstChunkBytes
        if (clientSession != null) {
            1 * chunksCollection.find(clientSession, _) >> findIterable
        } else {
            1 * chunksCollection.find(_) >> findIterable
        }
        1 * findIterable.sort(_) >> findIterable
        1 * findIterable.batchSize(0) >> findIterable
        1 * findIterable.iterator() >> mongoCursor
        1 * mongoCursor.hasNext() >> true
        1 * mongoCursor.next() >> chunkDocuments[0]

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should validate next chunk when marked and reset at eof'() {
        given:
        def fileInfo = new GridFSFile(new BsonObjectId(new ObjectId()), 'filename', 25L, 25, new Date(), 'abc', new Document())

        def chunkBytes = 1..25 as byte[]
        def chunkDocument = new Document('files_id', fileInfo.getId()).append('n', 0).append('data', new Binary(chunkBytes))

        def mongoCursor = Mock(MongoCursor)
        def findIterable = Mock(FindIterable)
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(clientSession, fileInfo, chunksCollection)

        when:
        def readByte = new byte[25]
        downloadStream.read(readByte)

        then:
        if (clientSession != null) {
            1 * chunksCollection.find(clientSession, _) >> findIterable
        } else {
            1 * chunksCollection.find(_) >> findIterable
        }
        1 * findIterable.sort(_) >> findIterable
        1 * findIterable.batchSize(0) >> findIterable
        1 * findIterable.iterator() >> mongoCursor
        1 * mongoCursor.hasNext() >> true
        1 * mongoCursor.next() >> chunkDocument

        then:
        readByte == chunkBytes

        when:
        downloadStream.mark()

        then:
        0 * chunksCollection.find(*_)

        when:
        downloadStream.reset()

        then:
        0 * chunksCollection.find(*_)

        when: 'Trying to read past eof'
        def result = downloadStream.read(readByte)

        then:
        result == -1

        when: 'Resets back to eof'
        downloadStream.reset()

        then:
        0 * chunksCollection.find(*_)

        when:
        result = downloadStream.read(readByte)

        then:
        result == -1

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should not throw an exception when trying to mark post close'() {
        given:
        def downloadStream = new GridFSDownloadStreamImpl(clientSession, fileInfo, Stub(MongoCollection))
        downloadStream.close()

        when:
        downloadStream.mark()

        then:
        notThrown(MongoGridFSException)

        when:
        downloadStream.mark(1)

        then:
        notThrown(MongoGridFSException)

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should handle negative skip value correctly '() {
        given:
        def downloadStream = new GridFSDownloadStreamImpl(clientSession, fileInfo, Stub(MongoCollection))

        when:
        def result = downloadStream.skip(-1)

        then:
        result == 0L

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should handle skip that is larger or equal to the file length'() {
        given:
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(clientSession, fileInfo, chunksCollection)

        when:
        def result = downloadStream.skip(skipValue)

        then:
        result == 3L
        0 * chunksCollection.find(*_)

        when:
        result = downloadStream.read()

        then:
        result == -1

        where:
        [skipValue, clientSession] << [[3, 100], [null, Stub(ClientSession)]].combinations()
    }

    def 'should throw if trying to pass negative batchSize'() {
        given:
        def downloadStream = new GridFSDownloadStreamImpl(clientSession, fileInfo, Stub(MongoCollection))

        when:
        downloadStream.batchSize(0)

        then:
        notThrown(IllegalArgumentException)


        when:
        downloadStream.batchSize(-1)

        then:
        thrown(IllegalArgumentException)

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should throw if no chunks found when data is expected'() {
        given:
        def mongoCursor = Mock(MongoCursor)
        def findIterable = Mock(FindIterable)
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(clientSession, fileInfo, chunksCollection)

        when:
        downloadStream.read()

        then:
        if (clientSession != null) {
            1 * chunksCollection.find(clientSession, _) >> findIterable
        } else {
            1 * chunksCollection.find(_) >> findIterable
        }
        1 * findIterable.sort(_) >> findIterable
        1 * findIterable.batchSize(0) >> findIterable
        1 * findIterable.iterator() >> mongoCursor
        1 * mongoCursor.hasNext() >> false

        then:
        thrown(MongoGridFSException)

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should throw if chunk data differs from the expected'() {
        given:
        def chunkDocument = new Document('files_id', fileInfo.getId())
                .append('n', 0)
                .append('data', new Binary(data))

        def mongoCursor = Mock(MongoCursor)
        def findIterable = Mock(FindIterable)
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(clientSession, fileInfo, chunksCollection)

        when:
        downloadStream.read()

        then:
        if (clientSession != null) {
            1 * chunksCollection.find(clientSession, _) >> findIterable
        } else {
            1 * chunksCollection.find(_) >> findIterable
        }
        1 * findIterable.sort(_) >> findIterable
        1 * findIterable.batchSize(0) >> findIterable
        1 * findIterable.iterator() >> mongoCursor
        1 * mongoCursor.hasNext() >> true
        1 * mongoCursor.next() >> chunkDocument

        then:
        thrown(MongoGridFSException)

        where:
        [data, clientSession] << [[new byte[1], new byte[100]], [null, Stub(ClientSession)]].combinations()
    }

    def 'should throw an exception when trying to action post close'() {
        given:
        def downloadStream = new GridFSDownloadStreamImpl(clientSession, fileInfo, Stub(MongoCollection))
        downloadStream.close()

        when:
        downloadStream.read()

        then:
        thrown(MongoGridFSException)

        when:
        downloadStream.skip(10)

        then:
        thrown(MongoGridFSException)

        when:
        downloadStream.reset()

        then:
        thrown(MongoGridFSException)

        when:
        downloadStream.read(new byte[10])

        then:
        thrown(MongoGridFSException)

        when:
        downloadStream.read(new byte[10], 0, 10)

        then:
        thrown(MongoGridFSException)

        where:
        clientSession << [null, Stub(ClientSession)]
    }
}
