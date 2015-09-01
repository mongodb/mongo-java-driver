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

package com.mongodb.client.gridfs

import com.mongodb.MongoGridFSException
import com.mongodb.client.FindIterable
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoCursor
import com.mongodb.client.gridfs.model.GridFSFile
import org.bson.BsonObjectId
import org.bson.Document
import org.bson.types.Binary
import org.bson.types.ObjectId
import spock.lang.Specification

class GridFSDownloadStreamSpecification extends Specification {
    def fileInfo = new GridFSFile(new BsonObjectId(new ObjectId()), 'filename', 3L, 2, new Date(), 'abc', new Document())

    def 'should return the file info'() {
        when:
        def downloadStream = new GridFSDownloadStreamImpl(fileInfo, Stub(MongoCollection))

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
        def downloadStream = new GridFSDownloadStreamImpl(fileInfo, chunksCollection)

        then:
        downloadStream.available() == 0

        when:
        def result = downloadStream.read()

        then:
        result == (twoBytes[0] & 0xFF)
        1 * chunksCollection.find(findQuery) >> findIterable
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

        then: 'extra chunk check'
        result == -1
        1 * mongoCursor.hasNext() >> false
        0 * mongoCursor.next()

        when:
        result = downloadStream.read()

        then:
        result == -1
        0 * chunksCollection.find(_)
    }

    def 'should create a new cursor each time when using batchSize 1'() {
        when:
        def twoBytes = new byte[2]
        def oneByte = new byte[1]
        def findQuery = new Document('files_id', fileInfo.getId()).append('n', new Document('$gte', 0))
        def secondFindQuery = new Document('files_id', fileInfo.getId()).append('n', new Document('$gte', 1))
        def thirdFindQuery = new Document('files_id', fileInfo.getId()).append('n', new Document('$gte', 2))
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
        def downloadStream = new GridFSDownloadStreamImpl(fileInfo, chunksCollection).batchSize(1)

        then:
        downloadStream.available() == 0

        when:
        def result = downloadStream.read()

        then:
        result == (twoBytes[0] & 0xFF)
        1 * chunksCollection.find(findQuery) >> findIterable
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
        1 * chunksCollection.find(secondFindQuery) >> findIterable
        1 * findIterable.sort(sort) >> findIterable
        1 * findIterable.batchSize(1) >> findIterable
        1 * findIterable.iterator() >> mongoCursor
        1 * mongoCursor.hasNext() >> true
        1 * mongoCursor.next() >> secondChunkDocument

        when:
        result = downloadStream.read()

        then: 'extra chunk check'
        result == -1
        1 * chunksCollection.find(thirdFindQuery) >> findIterable
        1 * findIterable.sort(sort) >> findIterable
        1 * findIterable.batchSize(1) >> findIterable
        1 * findIterable.iterator() >> mongoCursor
        1 * mongoCursor.hasNext() >> false
        0 * mongoCursor.next()

        when:
        result = downloadStream.read()

        then:
        result == -1
        0 * chunksCollection.find(_)
    }

    def 'should skip to the correct point'() {
        given:
        def fileInfo = new GridFSFile(new BsonObjectId(new ObjectId()), 'filename', 60L, 25, new Date(), 'abc', new Document())

        def firstChunkBytes = 1..25 as byte[]
        def thirdChunkBytes = 51 .. 60 as byte[]

        def sort = new Document('n', 1)

        def findQueries = [new Document('files_id', fileInfo.getId()).append('n', new Document('$gte', 0)),
                           new Document('files_id', fileInfo.getId()).append('n', new Document('$gte', 2))]
        def chunkDocuments =
                [new Document('files_id', fileInfo.getId()).append('n', 0).append('data', new Binary(firstChunkBytes)),
                 new Document('files_id', fileInfo.getId()).append('n', 2).append('data', new Binary(thirdChunkBytes))]

        def mongoCursor = Mock(MongoCursor)
        def findIterable = Mock(FindIterable)
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(fileInfo, chunksCollection)

        when:
        def skipResult = downloadStream.skip(15)

        then:
        skipResult == 15L
        0 * chunksCollection.find(_)

        when:
        def readByte = new byte[5]
        downloadStream.read(readByte)

        then:
        1 * chunksCollection.find(findQueries[0]) >> findIterable
        1 * findIterable.sort(sort) >> findIterable
        1 * findIterable.batchSize(0) >> findIterable
        1 * findIterable.iterator() >> mongoCursor
        1 * mongoCursor.hasNext() >> true
        1 * mongoCursor.next() >> chunkDocuments[0]

        then:
        readByte == [16, 17, 18, 19, 20] as byte[]

        when:
        skipResult = downloadStream.skip(35)

        then:
        skipResult == 35L
        0 * chunksCollection.find(_)

        when:
        downloadStream.read(readByte)

        then:
        1 * chunksCollection.find(findQueries[1]) >> findIterable
        1 * findIterable.sort(sort) >> findIterable
        1 * findIterable.batchSize(0) >> findIterable
        1 * findIterable.iterator() >> mongoCursor
        1 * mongoCursor.hasNext() >> true
        1 * mongoCursor.next() >> chunkDocuments[1]

        then:
        readByte == [56, 57, 58, 59, 60] as byte[]

        when:
        skipResult = downloadStream.skip(1)

        then:
        skipResult == 0L
        0 * chunksCollection.find(_)
    }

    def 'should handle negative skip value correctly '() {
        given:
        def downloadStream = new GridFSDownloadStreamImpl(fileInfo, Stub(MongoCollection))

        when:
        def result = downloadStream.skip(-1)

        then:
        result == 0L
    }

    def 'should handle skip that is larger or equal to the file length'() {
        given:
        def chunksCollection = Mock(MongoCollection)
        def mongoCursor = Mock(MongoCursor)
        def findIterable = Mock(FindIterable)
        def downloadStream = new GridFSDownloadStreamImpl(fileInfo, chunksCollection)

        when:
        def result = downloadStream.skip(skipValue)

        then:
        result == 3L
        0 * chunksCollection.find(_)

        when:
        result = downloadStream.read()

        then:
        result == -1
        1 * chunksCollection.find(_) >> findIterable
        1 * findIterable.sort(_) >> findIterable
        1 * findIterable.batchSize(0) >> findIterable
        1 * findIterable.iterator() >> mongoCursor
        1 * mongoCursor.hasNext() >> false

        when:
        result = downloadStream.read()

        then:
        result == -1
        0 * chunksCollection.find(_)

        where:
        skipValue << [3, 100]
    }

    def 'should allow extra empty chunk'() {
        given:
        def fileInfo = new GridFSFile(new BsonObjectId(new ObjectId()), 'filename', 10L, 10, new Date(), 'abc', new Document())
        def chunksCollection = Mock(MongoCollection)
        def mongoCursor = Mock(MongoCursor)
        def findIterable = Mock(FindIterable)

        def findQuery = new Document('files_id', fileInfo.getId()).append('n', new Document('$gte', 0))
        def sort = new Document('n', 1)
        def downloadStream = new GridFSDownloadStreamImpl(fileInfo, chunksCollection)

        def tenBytes = new byte[10]
        def chunkDocument = new Document('files_id', fileInfo.getId())
                .append('n', 0)
                .append('data', new Binary(tenBytes))

        def emptyChunkDocument = new Document('files_id', fileInfo.getId())
                .append('n', 1)
                .append('data', new Binary(new byte[0]))

        when:
        downloadStream.read(new byte[10])

        then:
        1 * chunksCollection.find(findQuery) >> findIterable
        1 * findIterable.sort(sort) >> findIterable
        1 * findIterable.batchSize(0) >> findIterable
        1 * findIterable.iterator() >> mongoCursor
        1 * mongoCursor.hasNext() >> true
        1 * mongoCursor.next() >> chunkDocument

        when:
        downloadStream.read()

        then: 'extra chunk check'
        1 * mongoCursor.hasNext() >> true
        1 * mongoCursor.next() >> emptyChunkDocument

        when:
        downloadStream.read()

        then:
        0 * chunksCollection.find(_)
    }

    def 'should throw if trying to pass negative batchSize'() {
        given:
        def downloadStream = new GridFSDownloadStreamImpl(fileInfo, Stub(MongoCollection))

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
        def mongoCursor = Mock(MongoCursor)
        def findIterable = Mock(FindIterable)
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(fileInfo, chunksCollection)

        when:
        downloadStream.read()

        then:
        1 * chunksCollection.find(_) >> findIterable
        1 * findIterable.sort(_) >> findIterable
        1 * findIterable.batchSize(0) >> findIterable
        1 * findIterable.iterator() >> mongoCursor
        1 * mongoCursor.hasNext() >> false

        then:
        thrown(MongoGridFSException)
    }

    def 'should throw if chunk data differs from the expected'() {
        given:
        def chunkDocument = new Document('files_id', fileInfo.getId())
                .append('n', 0)
                .append('data', new Binary(data))

        def mongoCursor = Mock(MongoCursor)
        def findIterable = Mock(FindIterable)
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(fileInfo, chunksCollection)

        when:
        downloadStream.read()

        then:
        1 * chunksCollection.find(_) >> findIterable
        1 * findIterable.sort(_) >> findIterable
        1 * findIterable.batchSize(0) >> findIterable
        1 * findIterable.iterator() >> mongoCursor
        1 * mongoCursor.hasNext() >> true
        1 * mongoCursor.next() >> chunkDocument

        then:
        thrown(MongoGridFSException)

        where:
        data << [new byte[1], new byte[100]]
    }

    def 'should throw if extra chunk contains data'() {
        def fileInfo = new GridFSFile(new BsonObjectId(new ObjectId()), 'filename', 10L, 10, new Date(), 'abc', new Document())
        def chunksCollection = Mock(MongoCollection)
        def mongoCursor = Mock(MongoCursor)
        def findIterable = Mock(FindIterable)
        def downloadStream = new GridFSDownloadStreamImpl(fileInfo, chunksCollection)

        def tenBytes = new byte[10]
        def chunkDocument = new Document('files_id', fileInfo.getId()).append('n', 0).append('data', new Binary(tenBytes))
        def badChunkDocument = new Document('files_id', fileInfo.getId()).append('n', 1).append('data', new Binary(new byte[1]))

        when:
        downloadStream.read(new byte[10])

        then:
        1 * chunksCollection.find(_) >> findIterable
        1 * findIterable.sort(_) >> findIterable
        1 * findIterable.batchSize(0) >> findIterable
        1 * findIterable.iterator() >> mongoCursor
        1 * mongoCursor.hasNext() >> true
        1 * mongoCursor.next() >> chunkDocument

        when:
        downloadStream.read()

        then:
        1 * mongoCursor.hasNext() >> true
        1 * mongoCursor.next() >> badChunkDocument

        then:
        thrown(MongoGridFSException)
    }

    def 'should throw if empty chunk contains data'() {
        def fileInfo = new GridFSFile(new BsonObjectId(new ObjectId()), 'filename', 0L, 10, new Date(), 'abc', new Document())
        def chunksCollection = Mock(MongoCollection)
        def mongoCursor = Mock(MongoCursor)
        def findIterable = Mock(FindIterable)
        def downloadStream = new GridFSDownloadStreamImpl(fileInfo, chunksCollection)

        def badChunkDocument = new Document('files_id', fileInfo.getId()).append('n', 0).append('data', new Binary(new byte[1]))

        when:
        downloadStream.read(new byte[10])

        then:
        1 * chunksCollection.find(_) >> findIterable
        1 * findIterable.sort(_) >> findIterable
        1 * findIterable.batchSize(0) >> findIterable
        1 * findIterable.iterator() >> mongoCursor
        1 * mongoCursor.hasNext() >> true
        1 * mongoCursor.next() >> badChunkDocument

        then:
        thrown(MongoGridFSException)
    }

    def 'should throw an exception when trying to action post close'() {
        given:
        def downloadStream = new GridFSDownloadStreamImpl(fileInfo, Stub(MongoCollection))
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
        downloadStream.read(new byte[10])

        then:
        thrown(MongoGridFSException)

        when:
        downloadStream.read(new byte[10], 0, 10)

        then:
        thrown(MongoGridFSException)
    }
}
