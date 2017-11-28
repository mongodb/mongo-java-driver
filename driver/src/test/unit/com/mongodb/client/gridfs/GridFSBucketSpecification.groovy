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

import com.mongodb.MongoClient
import com.mongodb.MongoDatabaseImpl
import com.mongodb.MongoGridFSException
import com.mongodb.MongoNamespace
import com.mongodb.OperationExecutor
import com.mongodb.ReadConcern
import com.mongodb.TestOperationExecutor
import com.mongodb.WriteConcern
import com.mongodb.client.FindIterable
import com.mongodb.client.ListIndexesIterable
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoCursor
import com.mongodb.client.gridfs.model.GridFSDownloadOptions
import com.mongodb.client.gridfs.model.GridFSFile
import com.mongodb.client.result.DeleteResult
import com.mongodb.client.result.UpdateResult
import com.mongodb.operation.BatchCursor
import com.mongodb.operation.FindOperation
import com.mongodb.session.ClientSession
import org.bson.BsonDocument
import org.bson.BsonObjectId
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.DocumentCodecProvider
import org.bson.types.Binary
import org.bson.types.ObjectId
import spock.lang.Specification
import spock.lang.Unroll

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.secondary
import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static spock.util.matcher.HamcrestSupport.expect

@SuppressWarnings('ClosureAsLastMethodParameter')
class GridFSBucketSpecification extends Specification {

    def readConcern = ReadConcern.DEFAULT
    def registry = MongoClient.getDefaultCodecRegistry()
    def database = databaseWithExecutor(Stub(OperationExecutor))
    def databaseWithExecutor(OperationExecutor executor) {
        new MongoDatabaseImpl('test', registry, primary(), WriteConcern.ACKNOWLEDGED, false, readConcern, executor)
    }

    def 'should return the correct bucket name'() {
        when:
        def bucketName = new GridFSBucketImpl(database).getBucketName()

        then:
        bucketName == 'fs'

        when:
        bucketName = new GridFSBucketImpl(database, 'custom').getBucketName()

        then:
        bucketName == 'custom'
    }

    def 'should behave correctly when using withChunkSizeBytes'() {
        given:
        def newChunkSize = 200

        when:
        def gridFSBucket = new GridFSBucketImpl(database).withChunkSizeBytes(newChunkSize)

        then:
        gridFSBucket.getChunkSizeBytes() == newChunkSize
    }

    def 'should behave correctly when using withReadPreference'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def newReadPreference = secondary()

        when:
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection).withReadPreference(newReadPreference)

        then:
        1 * filesCollection.withReadPreference(newReadPreference) >> filesCollection
        1 * chunksCollection.withReadPreference(newReadPreference) >> chunksCollection

        when:
        gridFSBucket.getReadConcern()

        then:
        1 * filesCollection.getReadConcern()
    }

    def 'should behave correctly when using withWriteConcern'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def newWriteConcern = WriteConcern.MAJORITY

        when:
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection).withWriteConcern(newWriteConcern)

        then:
        1 * filesCollection.withWriteConcern(newWriteConcern) >> filesCollection
        1 * chunksCollection.withWriteConcern(newWriteConcern) >> chunksCollection

        when:
        gridFSBucket.getWriteConcern()

        then:
        1 * filesCollection.getWriteConcern()
    }

    def 'should behave correctly when using withReadConcern'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def newReadConcern = ReadConcern.MAJORITY

        when:
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection).withReadConcern(newReadConcern)

        then:
        1 * filesCollection.withReadConcern(newReadConcern) >> filesCollection
        1 * chunksCollection.withReadConcern(newReadConcern) >> chunksCollection

        when:
        gridFSBucket.getReadConcern()

        then:
        1 * filesCollection.getReadConcern() >> newReadConcern
    }

    def 'should get defaults from MongoDatabase'() {
        given:
        def defaultChunkSizeBytes = 255 * 1024
        def database = new MongoDatabaseImpl('test', fromProviders(new DocumentCodecProvider()), secondary(), WriteConcern.ACKNOWLEDGED,
                false, readConcern, new TestOperationExecutor([]))

        when:
        def gridFSBucket = new GridFSBucketImpl(database)

        then:
        gridFSBucket.getChunkSizeBytes() == defaultChunkSizeBytes
        gridFSBucket.getReadPreference() == database.getReadPreference()
        gridFSBucket.getWriteConcern() == database.getWriteConcern()
        gridFSBucket.getReadConcern() == database.getReadConcern()
    }

    def 'should create the expected GridFSUploadStream'() {
        given:
        def filesCollection = Stub(MongoCollection)
        def chunksCollection = Stub(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection)

        when:
        def stream
        if (clientSession != null) {
            stream = gridFSBucket.openUploadStream(clientSession, 'filename')
        } else {
            stream = gridFSBucket.openUploadStream('filename')
        }

        then:
        expect stream, isTheSameAs(new GridFSUploadStreamImpl(clientSession, filesCollection, chunksCollection, stream.getId(), 'filename',
                255, null), ['md5', 'closeLock'])

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should upload from stream'() {
        given:
        def findIterable = Mock(FindIterable)
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection)
        def contentBytes = 'content' as byte[]
        def inputStream = new ByteArrayInputStream(contentBytes)

        when:
        gridFSBucket.uploadFromStream('filename', inputStream)

        then: 'index check'
        1 * filesCollection.withDocumentClass(Document) >> filesCollection
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        1 * filesCollection.find() >> findIterable
        1 * findIterable.projection(new Document('_id', 1)) >> findIterable
        1 * findIterable.first() >> new Document()

        then:
        1 * chunksCollection.insertOne(_)

        then:
        1 * filesCollection.insertOne(_)
    }

    def 'should clean up any chunks when upload from stream throws an IOException'() {
        given:
        def findIterable = Mock(FindIterable)
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection)
        def inputStream = Mock(InputStream) {
            2 * read(_) >> 255 >> { throw new IOException('stream failure') }
        }

        when:
        gridFSBucket.uploadFromStream('filename', inputStream)

        then: 'index check'
        1 * filesCollection.withDocumentClass(Document) >> filesCollection
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        1 * filesCollection.find() >> findIterable
        1 * findIterable.projection(new Document('_id', 1)) >> findIterable
        1 * findIterable.first() >> new Document()

        then:
        1 * chunksCollection.insertOne(_)

        then:
        1 * chunksCollection.deleteMany(_)

        then:
        0 * filesCollection.insertOne(_)

        then:
        def exception = thrown(MongoGridFSException)
        exception.getMessage() == 'IOException when reading from the InputStream'
    }

    def 'should not clean up any chunks when upload throws an exception'() {
        given:
        def findIterable = Mock(FindIterable)
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def alternativeException = new MongoGridFSException('Alternative failure')
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection)
        def inputStream = Mock(InputStream) {
            2 * read(_) >> 255 >> { throw alternativeException }
        }

        when:
        gridFSBucket.uploadFromStream('filename', inputStream)

        then: 'index check'
        1 * filesCollection.withDocumentClass(Document) >> filesCollection
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        1 * filesCollection.find() >> findIterable
        1 * findIterable.projection(new Document('_id', 1)) >> findIterable
        1 * findIterable.first() >> new Document()

        then:
        1 * chunksCollection.insertOne(_)

        then:
        0 * chunksCollection.deleteMany(_)

        then:
        0 * filesCollection.insertOne(_)

        then:
        def exception = thrown(MongoGridFSException)
        exception == alternativeException
    }

    def 'should create the expected GridFSDownloadStream'() {
        given:
        def fileId = new BsonObjectId(new ObjectId())
        def fileInfo = new GridFSFile(fileId, 'File 1', 10, 255, new Date(), '1234', new Document())
        def findIterable =  Mock(FindIterable)
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Stub(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection)

        when:
        def stream
        if (clientSession != null) {
            stream = gridFSBucket.openDownloadStream(clientSession, fileId.getValue())
        } else {
            stream = gridFSBucket.openDownloadStream(fileId.getValue())
        }

        then:
        if (clientSession != null) {
            1 * filesCollection.find(clientSession) >> findIterable
        } else {
            1 * filesCollection.find() >> findIterable
        }
        1 * findIterable.filter(_) >> findIterable
        1 * findIterable.first() >> fileInfo

        then:
        expect stream, isTheSameAs(new GridFSDownloadStreamImpl(clientSession, fileInfo, chunksCollection), ['closeLock', 'cursorLock'])


        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should download to stream'() {
        given:
        def fileId = new ObjectId()
        def bsonFileId = new BsonObjectId(fileId)
        def fileInfo = new GridFSFile(bsonFileId, 'filename', 10, 255, new Date(), '1234', new Document())
        def mongoCursor = Mock(MongoCursor)
        def findIterable =  Mock(FindIterable)
        def filesCollection = Mock(MongoCollection)
        def tenBytes = new byte[10]
        def chunkDocument = new Document('files_id', fileInfo.getId()).append('n', 0).append('data', new Binary(tenBytes))
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection)
        def outputStream = new ByteArrayOutputStream(10)

        when:
        if (clientSession != null) {
            gridFSBucket.downloadToStream(clientSession, fileId, outputStream)
        } else {
            gridFSBucket.downloadToStream(fileId, outputStream)
        }
        outputStream.close()

        then:
        if (clientSession != null) {
            1 * filesCollection.find(clientSession) >> findIterable
        } else {
            1 * filesCollection.find() >> findIterable
        }
        1 * findIterable.filter(new Document('_id', bsonFileId)) >> findIterable
        1 * findIterable.first() >> fileInfo

        then:
        if (clientSession != null) {
            1 * chunksCollection.find(clientSession, _) >> findIterable
        } else {
            1 * chunksCollection.find(_) >> findIterable
        }
        1 * findIterable.sort(_) >> findIterable
        1 * findIterable.batchSize(_) >> findIterable
        1 * findIterable.iterator() >> mongoCursor
        1 * mongoCursor.hasNext() >> true
        1 * mongoCursor.next() >> chunkDocument

        then:
        outputStream.toByteArray() == tenBytes

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should download to stream using BsonValue'() {
        given:
        def bsonFileId = new BsonString('1')
        def fileInfo = new GridFSFile(bsonFileId, 'filename', 10L, 255, new Date(), '1234', new Document())
        def mongoCursor =  Mock(MongoCursor)
        def findIterable =  Mock(FindIterable)
        def filesCollection = Mock(MongoCollection)
        def tenBytes = new byte[10]
        def chunkDocument = new Document('files_id', fileInfo.getId()).append('n', 0).append('data', new Binary(tenBytes))
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection)
        def outputStream = new ByteArrayOutputStream(10)

        when:
        if (clientSession != null) {
            gridFSBucket.downloadToStream(clientSession, bsonFileId, outputStream)
        } else {
            gridFSBucket.downloadToStream(bsonFileId, outputStream)
        }
        outputStream.close()

        then:
        if (clientSession != null) {
            1 * filesCollection.find(clientSession) >> findIterable
        } else {
            1 * filesCollection.find() >> findIterable
        }
        1 * findIterable.filter(new Document('_id', bsonFileId)) >> findIterable
        1 * findIterable.first() >> fileInfo

        then:
        if (clientSession != null) {
            1 * chunksCollection.find(clientSession, _) >> findIterable
        } else {
            1 * chunksCollection.find(_) >> findIterable
        }
        1 * findIterable.sort(_) >> findIterable
        1 * findIterable.batchSize(_) >> findIterable
        1 * findIterable.iterator() >> mongoCursor
        1 * mongoCursor.hasNext() >> true
        1 * mongoCursor.next() >> chunkDocument

        then:
        outputStream.toByteArray() == tenBytes

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should download to stream by name'() {
        given:
        def filename = 'filename'
        def fileId = new ObjectId()
        def bsonFileId = new BsonObjectId(fileId)
        def fileInfo = new GridFSFile(bsonFileId, filename, 10, 255, new Date(), '1234', new Document())
        def mongoCursor =  Mock(MongoCursor)
        def findIterable =  Mock(FindIterable)
        def findChunkIterable =  Mock(FindIterable)
        def filesCollection = Mock(MongoCollection)
        def tenBytes = new byte[10]
        def chunkDocument = new Document('files_id', fileInfo.getId()).append('n', 0).append('data', new Binary(tenBytes))
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection)
        def outputStream = new ByteArrayOutputStream(10)

        when:
        if (clientSession != null) {
            gridFSBucket.downloadToStream(clientSession, filename, outputStream)
        } else {
            gridFSBucket.downloadToStream(filename, outputStream)
        }
        outputStream.close()

        then:
        if (clientSession != null) {
            1 * filesCollection.find(clientSession) >> findIterable
        } else {
            1 * filesCollection.find() >> findIterable
        }
        1 * findIterable.filter(new Document('filename', filename)) >> findIterable
        1 * findIterable.skip(_) >> findIterable
        1 * findIterable.sort(_) >> findIterable
        1 * findIterable.first() >> fileInfo

        if (clientSession != null) {
            1 * chunksCollection.find(clientSession, _) >> findChunkIterable
        } else {
            1 * chunksCollection.find(_) >> findChunkIterable
        }
        1 * findChunkIterable.sort(_) >> findChunkIterable
        1 * findChunkIterable.batchSize(_) >> findChunkIterable
        1 * findChunkIterable.iterator() >> mongoCursor
        1 * mongoCursor.hasNext() >> true
        1 * mongoCursor.next() >> chunkDocument

        then:
        outputStream.toByteArray() == tenBytes

        then:
        1 * mongoCursor.close()

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should throw an exception if file not found'() {
        given:
        def fileId = new ObjectId()
        def bsonFileId = new BsonObjectId(fileId)
        def findIterable =  Mock(FindIterable)
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Stub(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection)

        when:
        if (clientSession != null) {
            gridFSBucket.openDownloadStream(clientSession, fileId)
        } else {
            gridFSBucket.openDownloadStream(fileId)
        }

        then:
        if (clientSession != null) {
            1 * filesCollection.find(clientSession) >> findIterable
        } else {
            1 * filesCollection.find() >> findIterable
        }

        1 * findIterable.filter(new Document('_id', bsonFileId)) >> findIterable
        1 * findIterable.first() >> null

        then:
        thrown(MongoGridFSException)

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    @Unroll
    def 'should create the expected GridFSDownloadStream when opening by name with version: #version'() {
        given:
        def filename = 'filename'
        def fileId = new ObjectId()
        def bsonFileId = new BsonObjectId(fileId)
        def fileInfo = new GridFSFile(bsonFileId, filename, 10, 255, new Date(), '1234', new Document())
        def findIterable =  Mock(FindIterable)
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Stub(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection)

        when:
        def stream = gridFSBucket.openDownloadStream(filename, new GridFSDownloadOptions().revision(version))

        then:
        1 * filesCollection.find() >> findIterable
        1 * findIterable.filter(new Document('filename', filename)) >> findIterable

        then:
        1 * findIterable.skip(skip) >> findIterable

        then:
        1 * findIterable.sort(new Document('uploadDate', sortOrder)) >> findIterable
        1 * findIterable.first() >> fileInfo

        then:
        expect stream, isTheSameAs(new GridFSDownloadStreamImpl(null, fileInfo, chunksCollection), ['closeLock', 'cursorLock'])

        where:
        version | skip | sortOrder
          0     |  0   | 1
          1     |  1   | 1
          2     |  2   | 1
          3     |  3   | 1
          -1    |  0   | -1
          -2    |  1   | -1
          -3    |  2   | -1
    }

    def 'should create the expected GridFSFindIterable'() {
        given:
        def collection = Mock(MongoCollection)
        def findIterable = Mock(FindIterable)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, collection, Stub(MongoCollection))

        when:
        def result
        if (clientSession != null) {
            result = gridFSBucket.find(clientSession)
        } else {
            result = gridFSBucket.find()
        }
        then:
        if (clientSession != null) {
            1 * collection.find(clientSession) >> findIterable
        } else {
            1 * collection.find() >> findIterable
        }
        expect result, isTheSameAs(new GridFSFindIterableImpl(findIterable))

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should execute the expected FindOperation when finding a file'() {
        given:
        def executor = new TestOperationExecutor([Stub(BatchCursor), Stub(BatchCursor)])
        def database = databaseWithExecutor(executor)
        def gridFSBucket = new GridFSBucketImpl(database)
        def decoder = registry.get(GridFSFile)

        when:
        gridFSBucket.find().iterator()

        then:
        executor.getReadPreference() == primary()
        expect executor.getReadOperation(), isTheSameAs(new FindOperation<GridFSFile>(new MongoNamespace('test.fs.files'), decoder)
                .filter(new BsonDocument()))

        when:
        def filter = new BsonDocument('filename', new BsonString('filename'))
        def readConcern = ReadConcern.MAJORITY
        gridFSBucket.withReadPreference(secondary()).withReadConcern(readConcern).find(filter).iterator()

        then:
        executor.getReadPreference() == secondary()
        expect executor.getReadOperation(), isTheSameAs(new FindOperation<GridFSFile>(new MongoNamespace('test.fs.files'), decoder)
                .readConcern(readConcern).filter(filter).slaveOk(true))
    }

    def 'should throw an exception if file not found when opening by name'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def findIterable = Mock(FindIterable)
        def chunksCollection = Stub(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection)

        when:
        if (clientSession != null) {
            gridFSBucket.openDownloadStream(clientSession, 'filename')
        } else {
            gridFSBucket.openDownloadStream('filename')
        }

        then:
        if (clientSession != null) {
            1 * filesCollection.find(clientSession) >> findIterable
        } else {
            1 * filesCollection.find() >> findIterable
        }

        1 * findIterable.filter(new Document('filename', 'filename')) >> findIterable
        1 * findIterable.skip(0) >> findIterable
        1 * findIterable.sort(new Document('uploadDate', -1)) >> findIterable
        1 * findIterable.first() >> null

        then:
        thrown(MongoGridFSException)

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should create indexes on write'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def listIndexesIterable = Mock(ListIndexesIterable)
        def findIterable = Mock(FindIterable)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection)

        when:
        if (clientSession != null) {
            gridFSBucket.openUploadStream(clientSession, 'filename')
        } else {
            gridFSBucket.openUploadStream('filename')
        }

        then:
        1 * filesCollection.withDocumentClass(Document) >> filesCollection
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        if (clientSession != null) {
            1 * filesCollection.find(clientSession) >> findIterable
        } else {
            1 * filesCollection.find() >> findIterable
        }
        1 * findIterable.projection(new Document('_id', 1)) >> findIterable
        1 * findIterable.first() >> null

        then:
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        if (clientSession != null) {
            1 * filesCollection.listIndexes(clientSession) >> listIndexesIterable
        } else {
            1 * filesCollection.listIndexes() >> listIndexesIterable
        }
        1 * listIndexesIterable.into(_) >> []

        then:
        if (clientSession != null) {
            1 * filesCollection.createIndex(clientSession, { index -> index == Document.parse('{"filename": 1, "uploadDate": 1 }') },
                    { indexOptions -> !indexOptions.isUnique() })
        } else {
            1 * filesCollection.createIndex({ index -> index == Document.parse('{"filename": 1, "uploadDate": 1 }') },
                    { indexOptions -> !indexOptions.isUnique() })
        }

        then:
        1 * chunksCollection.withReadPreference(primary()) >> chunksCollection
        if (clientSession != null) {
            1 * chunksCollection.listIndexes(clientSession) >> listIndexesIterable
        } else {
            1 * chunksCollection.listIndexes() >> listIndexesIterable
        }
        1 * listIndexesIterable.into(_) >> []

        then:
        if (clientSession != null) {
            1 * chunksCollection.createIndex(clientSession, { index -> index == Document.parse('{"files_id": 1, "n": 1}') },
                    { indexOptions -> indexOptions.isUnique() })
        } else {
            1 * chunksCollection.createIndex({ index -> index == Document.parse('{"files_id": 1, "n": 1}') },
                    { indexOptions -> indexOptions.isUnique() })
        }

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should not create indexes if they already exist'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def listIndexesIterable = Mock(ListIndexesIterable)
        def findIterable = Mock(FindIterable)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection)

        when:
        gridFSBucket.openUploadStream('filename')

        then:
        1 * filesCollection.withDocumentClass(Document) >> filesCollection
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        1 * filesCollection.find() >> findIterable
        1 * findIterable.projection(new Document('_id', 1)) >> findIterable
        1 * findIterable.first() >> null

        then:
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        1 * filesCollection.listIndexes() >> listIndexesIterable
        1 * listIndexesIterable.into(_) >> [Document.parse('{"key": {"_id": 1}}'),
                                            Document.parse('{"key": {"filename": 1, "uploadDate": 1 }}')]

        then:
        0 * filesCollection.createIndex(_)

        then:
        1 * chunksCollection.withReadPreference(primary()) >> chunksCollection
        1 * chunksCollection.listIndexes() >> listIndexesIterable
        1 * listIndexesIterable.into(_) >> [Document.parse('{"key": {"_id": 1}}'),
                                            Document.parse('{"key": {"files_id": 1, "n": 1 }}')]

        then:
        0 * chunksCollection.createIndex(_)
    }

    def 'should delete from files collection then chunks collection'() {
        given:
        def fileId = new ObjectId()
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection)

        when:
        gridFSBucket.delete(fileId)

        then: 'Delete from the files collection first'
        1 * filesCollection.deleteOne(new Document('_id', new BsonObjectId(fileId))) >> DeleteResult.acknowledged(1)

        then:
        1 * chunksCollection.deleteMany(new Document('files_id', new BsonObjectId(fileId)))
    }

    def 'should throw an exception when deleting if no record in the files collection'() {
        given:
        def fileId = new ObjectId()
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection)

        when:
        gridFSBucket.delete(fileId)

        then: 'Delete from the files collection first'
        1 * filesCollection.deleteOne(new Document('_id', new BsonObjectId(fileId))) >> DeleteResult.acknowledged(0)

        then: 'Should still delete any orphan chunks'
        1 * chunksCollection.deleteMany(new Document('files_id', new BsonObjectId(fileId)))

        then:
        thrown(MongoGridFSException)
    }

    def 'should rename a file'() {
        given:
        def id = new ObjectId()
        def fileId = new BsonObjectId(id)
        def filesCollection = Mock(MongoCollection)
        def newFilename = 'newFilename'
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, Stub(MongoCollection))

        when:
        gridFSBucket.rename(id, newFilename)

        then:
        1 * filesCollection.updateOne(new BsonDocument('_id', fileId),
                new BsonDocument('$set',
                        new BsonDocument('filename', new BsonString(newFilename)))) >> new UpdateResult.UnacknowledgedUpdateResult()

        when:
        gridFSBucket.rename(fileId, newFilename)

        then:
        1 * filesCollection.updateOne(new BsonDocument('_id', fileId),
                new BsonDocument('$set',
                        new BsonDocument('filename', new BsonString(newFilename)))) >> new UpdateResult.UnacknowledgedUpdateResult()
    }

    def 'should throw an exception renaming non existent file'() {
        given:
        def fileId = new ObjectId()
        def filesCollection = Mock(MongoCollection) {
            1 * updateOne(_, _) >> new UpdateResult.AcknowledgedUpdateResult(0, 0, null)
        }
        def newFilename = 'newFilename'
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, Stub(MongoCollection))

        when:
        gridFSBucket.rename(fileId, newFilename)

        then:
        thrown(MongoGridFSException)
    }

    def 'should be able to drop the bucket'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection)

        when:
        gridFSBucket.drop()

        then: 'drop the files collection first'
        1 * filesCollection.drop()

        then:
        1 * chunksCollection.drop()
    }

    def 'should validate the clientSession is not null'() {
        given:
        def objectId = new ObjectId()
        def bsonValue = new BsonObjectId(objectId)
        def filename = 'filename'
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection)

        when:
        gridFSBucket.delete(null, objectId)
        then:
        thrown(IllegalArgumentException)

        when:
        gridFSBucket.downloadToStream(null, filename, Stub(OutputStream))
        then:
        thrown(IllegalArgumentException)

        when:
        gridFSBucket.downloadToStream(null, objectId, Stub(OutputStream))
        then:
        thrown(IllegalArgumentException)

        when:
        gridFSBucket.drop(null)
        then:
        thrown(IllegalArgumentException)

        when:
        gridFSBucket.find((ClientSession) null)
        then:
        thrown(IllegalArgumentException)

        when:
        gridFSBucket.openDownloadStream(null, filename)
        then:
        thrown(IllegalArgumentException)

        when:
        gridFSBucket.openDownloadStream(null, objectId)
        then:
        thrown(IllegalArgumentException)

        when:
        gridFSBucket.openUploadStream(null, filename)
        then:
        thrown(IllegalArgumentException)

        when:
        gridFSBucket.openUploadStream(null, bsonValue, filename)
        then:
        thrown(IllegalArgumentException)

        when:
        gridFSBucket.rename(null, objectId, filename)
        then:
        thrown(IllegalArgumentException)

        when:
        gridFSBucket.uploadFromStream(null, filename, Stub(InputStream))
        then:
        thrown(IllegalArgumentException)

        when:
        gridFSBucket.uploadFromStream(null, bsonValue, filename, Stub(InputStream))
        then:
        thrown(IllegalArgumentException)
    }
}
