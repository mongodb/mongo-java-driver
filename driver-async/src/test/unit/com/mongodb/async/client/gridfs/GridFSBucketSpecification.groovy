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

package com.mongodb.async.client.gridfs

import com.mongodb.MongoException
import com.mongodb.MongoGridFSException
import com.mongodb.MongoNamespace
import com.mongodb.ReadConcern
import com.mongodb.WriteConcern
import com.mongodb.async.AsyncBatchCursor
import com.mongodb.async.FutureResultCallback
import com.mongodb.async.SingleResultCallback
import com.mongodb.async.client.ClientSession
import com.mongodb.async.client.FindIterable
import com.mongodb.async.client.MongoClients
import com.mongodb.async.client.MongoCollection
import com.mongodb.async.client.MongoDatabaseImpl
import com.mongodb.async.client.OperationExecutor
import com.mongodb.async.client.TestOperationExecutor
import com.mongodb.client.gridfs.model.GridFSDownloadOptions
import com.mongodb.client.gridfs.model.GridFSFile
import com.mongodb.client.result.DeleteResult
import com.mongodb.client.result.UpdateResult
import com.mongodb.operation.FindOperation
import org.bson.BsonDocument
import org.bson.BsonObjectId
import org.bson.BsonString
import org.bson.Document
import org.bson.UuidRepresentation
import org.bson.codecs.DocumentCodecProvider
import org.bson.types.Binary
import org.bson.types.ObjectId
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.ByteBuffer

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.secondary
import static com.mongodb.async.client.gridfs.helpers.AsyncStreamHelper.toAsyncInputStream
import static com.mongodb.async.client.gridfs.helpers.AsyncStreamHelper.toAsyncOutputStream
import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static spock.util.matcher.HamcrestSupport.expect

@SuppressWarnings(['ClosureAsLastMethodParameter', 'ClassSize'])
class GridFSBucketSpecification extends Specification {

    def readConcern = ReadConcern.DEFAULT
    def uuidRepresentation = UuidRepresentation.STANDARD
    def registry = MongoClients.defaultCodecRegistry
    def database = databaseWithExecutor(Stub(OperationExecutor))
    def databaseWithExecutor(OperationExecutor executor) {
        new MongoDatabaseImpl('test', registry, primary(), WriteConcern.ACKNOWLEDGED, true, true, readConcern, uuidRepresentation, executor)
    }
    def disableMD5 = false

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
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection, disableMD5)
                .withReadPreference(newReadPreference)

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
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection, disableMD5)
                .withWriteConcern(newWriteConcern)

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
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection, disableMD5)
                .withReadConcern(newReadConcern)

        then:
        1 * filesCollection.withReadConcern(newReadConcern) >> filesCollection
        1 * chunksCollection.withReadConcern(newReadConcern) >> chunksCollection

        when:
        gridFSBucket.getReadConcern()

        then:
        1 * filesCollection.getReadConcern() >> newReadConcern
    }

    def 'should behave correctly when using withDisableMD5'() {
        when:
        def gridFSBucket = new GridFSBucketImpl('fs', 255, Stub(MongoCollection), Stub(MongoCollection), disableMD5)

        then:
        !gridFSBucket.getDisableMD5()

        when:
        gridFSBucket = gridFSBucket.withDisableMD5(true)

        then:
        gridFSBucket.getDisableMD5()
    }

    def 'should get defaults from MongoDatabase'() {
        given:
        def defaultChunkSizeBytes = 255 * 1024
        def database = new MongoDatabaseImpl('test', fromProviders(new DocumentCodecProvider()), secondary(), WriteConcern.ACKNOWLEDGED,
                true, true, readConcern, uuidRepresentation, new TestOperationExecutor([]))

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
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection, disableMD5)

        when:
        def stream
        if (clientSession != null){
            stream = gridFSBucket.openUploadStream(clientSession, 'filename')
        } else {
            stream = gridFSBucket.openUploadStream('filename')
        }

        then:
        expect stream, isTheSameAs(new GridFSUploadStreamImpl(clientSession, filesCollection, chunksCollection, stream.getId(),
                'filename', 255, disableMD5, null, new GridFSIndexCheckImpl(clientSession, filesCollection, chunksCollection)),
                ['md5', 'closeAndWritingLock'])

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should upload from stream'() {
        given:
        def findIterable = Mock(FindIterable)
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection, disableMD5)
        def contentBytes = 'content' as byte[]
        def inputStream = toAsyncInputStream(new ByteArrayInputStream(contentBytes))

        when:
        if (clientSession != null){
            gridFSBucket.uploadFromStream(clientSession, 'filename', inputStream, Stub(SingleResultCallback))
        } else {
            gridFSBucket.uploadFromStream('filename', inputStream, Stub(SingleResultCallback))
        }

        then:
        1 * filesCollection.withDocumentClass(Document) >> filesCollection
        1 * filesCollection.withReadPreference(_) >> filesCollection
        if (clientSession != null){
            1 * filesCollection.find(clientSession) >> findIterable
        } else {
            1 * filesCollection.find() >> findIterable
        }
        1 * findIterable.projection(new Document('_id', 1)) >> findIterable
        1 * findIterable.first(_) >> { it.last().onResult(new Document(), null) }

        if (clientSession != null){
            1 * chunksCollection.insertOne(clientSession, _, _) >> { it.last().onResult(null, null) }
            1 * filesCollection.insertOne(clientSession, _, _)
        } else {
            1 * chunksCollection.insertOne(_, _) >> { it.last().onResult(null, null) }
            1 * filesCollection.insertOne(_, _)
        }

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should clean up any chunks when upload from stream throws an IOException'() {
        given:
        def findIterable = Mock(FindIterable)
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection, disableMD5)
        def inputStream = Mock(AsyncInputStream) {
            2 * read(_, _) >> { it[0].put(new byte[255]); it.last().onResult(255, null) } >> {
                it.last().onResult(null, new IOException('stream failure'))
            }
        }
        def futureResult = new FutureResultCallback()

        when:
        if (clientSession != null){
            gridFSBucket.uploadFromStream(clientSession, 'filename', inputStream, futureResult)
        } else {
            gridFSBucket.uploadFromStream('filename', inputStream, futureResult)
        }

        then:
        1 * filesCollection.withDocumentClass(Document) >> filesCollection
        1 * filesCollection.withReadPreference(_) >> filesCollection
        if (clientSession != null){
            1 * filesCollection.find(clientSession) >> findIterable
        } else {
            1 * filesCollection.find() >> findIterable
        }
        1 * findIterable.projection(new Document('_id', 1)) >> findIterable
        1 * findIterable.first(_) >> { it.last().onResult(new Document(), null) }

        if (clientSession != null) {
            1 * chunksCollection.insertOne(clientSession, _, _) >> { it.last().onResult(null, null) }
        } else {
            1 * chunksCollection.insertOne(_, _) >> { it.last().onResult(null, null) }
        }

        if (clientSession != null) {
            1 * chunksCollection.deleteMany(clientSession, _, _) >> { it.last().onResult(null, null) }
        } else {
            1 * chunksCollection.deleteMany(_, _) >> { it.last().onResult(null, null) }
        }

        then:
        0 * filesCollection.insertOne(*_)

        when:
        futureResult.get()

        then:
        def exception = thrown(MongoGridFSException)
        exception.getMessage() == 'IOException when reading from the InputStream'

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should not clean up any chunks when upload throws an exception'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def findIterable = Mock(FindIterable)
        def chunksCollection = Mock(MongoCollection)
        def alternativeException = new MongoGridFSException('Alternative failure')
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection, disableMD5)
        def inputStream = Mock(AsyncInputStream) {
            2 * read(_, _) >> {  it[0].put(new byte[255]); it.last().onResult(255, null) } >> {
                it.last().onResult(null, alternativeException)
            }
        }
        def futureResult = new FutureResultCallback()

        when:
        if (clientSession != null){
            gridFSBucket.uploadFromStream(clientSession, 'filename', inputStream, futureResult)
        } else {
            gridFSBucket.uploadFromStream('filename', inputStream, futureResult)
        }

        then:
        1 * filesCollection.withDocumentClass(Document) >> filesCollection
        1 * filesCollection.withReadPreference(_) >> filesCollection
        if (clientSession != null){
            1 * filesCollection.find(clientSession) >> findIterable
        } else {
            1 * filesCollection.find() >> findIterable
        }
        1 * findIterable.projection(new Document('_id', 1)) >> findIterable
        1 * findIterable.first(_) >> { it.last().onResult(new Document(), null) }

        if (clientSession != null){
            1 * chunksCollection.insertOne(clientSession, _, _) >> { it.last().onResult(null, null) }
        } else {
            1 * chunksCollection.insertOne(_, _) >> { it.last().onResult(null, null) }
        }

        when:
        futureResult.get()

        then:
        def exception = thrown(MongoGridFSException)
        exception == alternativeException

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should propagate errors when writing to the uploadStream'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def findIterable = Mock(FindIterable)
        def chunksCollection = Mock(MongoCollection)
        def uploadStreamException = new MongoException('Alternative failure')
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection, disableMD5)
        def inputStream = Mock(AsyncInputStream)
        def futureResult = new FutureResultCallback()

        when:
        if (clientSession != null){
            gridFSBucket.uploadFromStream(clientSession, 'filename', inputStream, futureResult)
        } else {
            gridFSBucket.uploadFromStream('filename', inputStream, futureResult)
        }

        then: 'When writing to the stream'
        1 * filesCollection.withDocumentClass(Document) >> filesCollection
        1 * filesCollection.withReadPreference(_) >> filesCollection
        if (clientSession != null){
            1 * filesCollection.find(clientSession) >> findIterable
        } else {
            1 * filesCollection.find() >> findIterable
        }
        1 * findIterable.projection(new Document('_id', 1)) >> findIterable
        1 * findIterable.first(_) >> { it.last().onResult(new Document(), null) }

        1 * inputStream.read(_, _) >> { it[0].put(new byte[255]); it.last().onResult(255, null) }

        if (clientSession != null){
            1 * chunksCollection.insertOne(clientSession, _, _) >> { it.last().onResult(null, uploadStreamException) }
        } else {
            1 * chunksCollection.insertOne(_, _) >> { it.last().onResult(null, uploadStreamException) }
        }

        when:
        futureResult.get()

        then:
        def exception = thrown(MongoException)
        exception == uploadStreamException

        when:
        futureResult = new FutureResultCallback()
        gridFSBucket.uploadFromStream('filename', inputStream, futureResult)

        then: 'When closing the stream'
        1 * filesCollection.withDocumentClass(Document) >> filesCollection
        1 * filesCollection.withReadPreference(_) >> filesCollection
        if (clientSession != null){
            1 * filesCollection.find(clientSession) >> findIterable
        } else {
            1 * filesCollection.find() >> findIterable
        }
        1 * findIterable.projection(new Document('_id', 1)) >> findIterable
        1 * findIterable.first(_) >> { it.last().onResult(new Document(), null) }

        2 * inputStream.read(_, _) >> { it[0].put(new byte[255]); it.last().onResult(255, null) } >> { it.last().onResult(-1, null) }

        if (clientSession != null){
            1 * chunksCollection.insertOne(clientSession, _, _) >> { it.last().onResult(null, null) }
            1 * filesCollection.insertOne(clientSession, _, _) >> { it.last().onResult(null, uploadStreamException) }
        } else {
            1 * chunksCollection.insertOne(_, _) >> { it.last().onResult(null, null) }
            1 * filesCollection.insertOne(_, _) >> { it.last().onResult(null, uploadStreamException) }
        }

        when:
        futureResult.get()

        then:
        exception = thrown(MongoException)
        exception == uploadStreamException

        where:
        clientSession << [null]
    }

    def 'should create the expected GridFSDownloadStream'() {
        given:
        def fileId = new BsonObjectId(new ObjectId())
        def findIterable = Mock(FindIterable)
        def gridFSFindIterable = new GridFSFindIterableImpl(findIterable)
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Stub(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection, disableMD5)

        when:
        def stream
        if (clientSession != null){
            stream = gridFSBucket.openDownloadStream(clientSession, fileId.getValue())
        } else {
            stream = gridFSBucket.openDownloadStream(fileId.getValue())
        }

        then:
        if (clientSession != null){
            1 * filesCollection.find(clientSession) >> findIterable
        } else {
            1 * filesCollection.find() >> findIterable
        }
        1 * findIterable.filter(_) >> findIterable

        then:
        expect stream, isTheSameAs(new GridFSDownloadStreamImpl(clientSession, gridFSFindIterable, chunksCollection),
                ['closeAndReadingLock', 'resultsQueue'])

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should download to stream'() {
        given:
        def fileId = new ObjectId()
        def bsonFileId = new BsonObjectId(fileId)
        def fileInfo = new GridFSFile(bsonFileId, 'filename', 10, 255, new Date(), '1234', new Document())
        def batchCursor = Mock(AsyncBatchCursor)
        def filesFindIterable = Mock(FindIterable)
        def chunksFindIterable = Mock(FindIterable)
        def filesCollection = Mock(MongoCollection)
        def sizeOfStream = 10
        def tenBytes = new byte[sizeOfStream]
        def chunkDocument = new Document('files_id', fileInfo.getId()).append('n', 0).append('data', new Binary(tenBytes))
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection, disableMD5)
        def outputStream = new ByteArrayOutputStream(1024)
        def asyncOutputStream = toAsyncOutputStream(outputStream)

        when:
        def futureResult = new FutureResultCallback()
        if (clientSession != null){
            gridFSBucket.downloadToStream(clientSession, fileId, asyncOutputStream, futureResult)
        } else {
            gridFSBucket.downloadToStream(fileId, asyncOutputStream, futureResult)
        }
        asyncOutputStream.close(Stub(SingleResultCallback))
        def size = futureResult.get()

        then:
        if (clientSession != null){
            1 * filesCollection.find(clientSession) >> filesFindIterable
        } else {
            1 * filesCollection.find() >> filesFindIterable
        }
        1 * filesFindIterable.filter(new Document('_id', fileId)) >> filesFindIterable

        then:
        1 * filesFindIterable.first(_) >> { it.last().onResult(fileInfo, null) }
        if (clientSession != null){
            1 * chunksCollection.find(clientSession, _) >> chunksFindIterable
        } else {
            1 * chunksCollection.find(_) >> chunksFindIterable
        }
        1 * chunksFindIterable.sort(_) >> chunksFindIterable
        1 * chunksFindIterable.batchSize(_) >> chunksFindIterable
        1 * chunksFindIterable.batchCursor(_) >> { it.last().onResult(batchCursor, null) }
        1 * batchCursor.next(_) >> { it.last().onResult([chunkDocument], null) }

        then:
        size == sizeOfStream
        outputStream.toByteArray() == tenBytes

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    @Unroll
    def 'should download to stream using #description'() {
        given:
        def bsonFileId = fileId instanceof ObjectId ? new BsonObjectId(fileId) : fileId
        def fileInfo = new GridFSFile(bsonFileId, 'filename', 10L, 255, new Date(), '1234', new Document())
        def batchCursor = Mock(AsyncBatchCursor)
        def filesFindIterable = Mock(FindIterable)
        def chunksFindIterable = Mock(FindIterable)
        def filesCollection = Mock(MongoCollection)
        def sizeOfStream = 10
        def tenBytes = new byte[sizeOfStream]
        def chunkDocument = new Document('files_id', fileId).append('n', 0).append('data', new Binary(tenBytes))
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection, disableMD5)
        def outputStream = new ByteArrayOutputStream(1024)
        def asyncOutputStream = toAsyncOutputStream(outputStream)

        when:
        def futureResult = new FutureResultCallback()
        if (clientSession != null){
            gridFSBucket.downloadToStream(clientSession, fileId, asyncOutputStream, futureResult)
        } else {
            gridFSBucket.downloadToStream(fileId, asyncOutputStream, futureResult)
        }
        asyncOutputStream.close(Stub(SingleResultCallback))
        def size = futureResult.get()

        then:
        if (clientSession != null){
            1 * filesCollection.find(clientSession) >> filesFindIterable
        } else {
            1 * filesCollection.find() >> filesFindIterable
        }
        1 * filesFindIterable.filter(new Document('_id', fileId)) >> filesFindIterable

        then:
        1 * filesFindIterable.first(_) >> { it.last().onResult(fileInfo, null) }
        if (clientSession != null){
            1 * chunksCollection.find(clientSession, _) >> chunksFindIterable
        } else {
            1 * chunksCollection.find(_) >> chunksFindIterable
        }
        1 * chunksFindIterable.sort(_) >> chunksFindIterable
        1 * chunksFindIterable.batchSize(_) >> chunksFindIterable
        1 * chunksFindIterable.batchCursor(_) >> { it.last().onResult(batchCursor, null) }
        1 * batchCursor.next(_) >> { it.last().onResult([chunkDocument], null) }

        then:
        size == sizeOfStream
        outputStream.toByteArray() == tenBytes

        where:
        description                    | fileId              | clientSession
        'using objectId'               | new ObjectId()      | null
        'using bsonValue'              | new BsonString('1') | null
        'using objectId with session'  | new ObjectId()      | Stub(ClientSession)
        'using bsonValue with session' | new BsonString('1') | Stub(ClientSession)
    }

    def 'should download to stream by name'() {
        given:
        def filename = 'filename'
        def fileId = new ObjectId()
        def fileInfo = new GridFSFile(new BsonObjectId(fileId), filename, 10L, 255, new Date(), '1234', new Document())
        def batchCursor = Mock(AsyncBatchCursor)
        def filesFindIterable = Mock(FindIterable)
        def chunksFindIterable = Mock(FindIterable)
        def filesCollection = Mock(MongoCollection)
        def sizeOfStream = 10
        def tenBytes = new byte[sizeOfStream]
        def chunkDocument = new Document('files_id', fileId).append('n', 0).append('data', new Binary(tenBytes))
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection, disableMD5)
        def outputStream = new ByteArrayOutputStream(1024)
        def asyncOutputStream = toAsyncOutputStream(outputStream)

        when:
        def futureResult = new FutureResultCallback()
        if (clientSession != null){
            gridFSBucket.downloadToStream(clientSession, filename, asyncOutputStream, futureResult)
        } else {
            gridFSBucket.downloadToStream(filename, asyncOutputStream, futureResult)
        }
        asyncOutputStream.close(Stub(SingleResultCallback))
        def size = futureResult.get()

        then:
        if (clientSession != null){
            1 * filesCollection.find(clientSession) >> filesFindIterable
        } else {
            1 * filesCollection.find() >> filesFindIterable
        }
        1 * filesFindIterable.filter(new Document('filename', filename)) >> filesFindIterable
        1 * filesFindIterable.sort(_) >> filesFindIterable
        1 * filesFindIterable.skip(_) >> filesFindIterable

        then:
        1 * filesFindIterable.first(_) >> { it.last().onResult(fileInfo, null) }
        if (clientSession != null){
            1 * chunksCollection.find(clientSession, _) >> chunksFindIterable
        } else {
            1 * chunksCollection.find(_) >> chunksFindIterable
        }
        1 * chunksFindIterable.sort(_) >> chunksFindIterable
        1 * chunksFindIterable.batchSize(_) >> chunksFindIterable
        1 * chunksFindIterable.batchCursor(_) >> { it.last().onResult(batchCursor, null) }
        1 * batchCursor.next(_) >> { it.last().onResult([chunkDocument], null) }

        then:
        size == sizeOfStream
        outputStream.toByteArray() == tenBytes

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should throw an exception if file not found'() {
        given:
        def fileId = new ObjectId()
        def findIterable = Mock(FindIterable)
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Stub(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection, disableMD5)

        when:
        def futureResult = new FutureResultCallback()

        def stream
        if (clientSession != null){
            stream = gridFSBucket.openDownloadStream(clientSession, fileId)
        } else {
            stream = gridFSBucket.openDownloadStream(fileId)
        }
        stream.read(ByteBuffer.wrap(new byte[10]), futureResult)
        futureResult.get()

        then:

        if (clientSession != null){
            1 * filesCollection.find(clientSession) >> findIterable
        } else {
            1 * filesCollection.find() >> findIterable
        }
        1 * findIterable.filter(new Document('_id', fileId)) >> findIterable
        1 * findIterable.first(_) >> { it.last().onResult(null, null) }
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
        def findIterable = Mock(FindIterable)
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Stub(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection, disableMD5)

        when:
        def futureResult = new FutureResultCallback()
        def stream
        if (clientSession != null) {
            stream = gridFSBucket.openDownloadStream(clientSession, filename, new GridFSDownloadOptions().revision(version))
        } else {
            stream = gridFSBucket.openDownloadStream(filename, new GridFSDownloadOptions().revision(version))
        }
        stream.getGridFSFile(futureResult)
        futureResult.get()

        then:
        if (clientSession != null) {
            1 * filesCollection.find(clientSession) >> findIterable
        } else {
            1 * filesCollection.find() >> findIterable
        }
        1 * findIterable.filter(new Document('filename', filename)) >> findIterable
        1 * findIterable.skip(skip) >> findIterable
        1 * findIterable.sort(new Document('uploadDate', sortOrder)) >> findIterable
        1 * findIterable.first(_) >> { it.last().onResult(fileInfo, null) }

        where:
        version | skip | sortOrder  | clientSession
        0       | 0    | 1          | null
        1       | 1    | 1          | null
        2       | 2    | 1          | null
        3       | 3    | 1          | null
        -3      | 2    | -1         | null
        -1      | 0    | -1         | null
        -2      | 1    | -1         | null
        0       | 0    | 1          | Stub(ClientSession)
        1       | 1    | 1          | Stub(ClientSession)
        2       | 2    | 1          | Stub(ClientSession)
        3       | 3    | 1          | Stub(ClientSession)
        -3      | 2    | -1         | Stub(ClientSession)
        -1      | 0    | -1         | Stub(ClientSession)
        -2      | 1    | -1         | Stub(ClientSession)

        // todo
    }

    def 'should create the expected GridFSFindIterable'() {
        given:
        def collection = Mock(MongoCollection)
        def findIterable = Mock(FindIterable)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, collection, Stub(MongoCollection), disableMD5)


        when:
        def result = gridFSBucket.find()

        then:
        1 * collection.find() >> findIterable
        expect result, isTheSameAs(new GridFSFindIterableImpl(findIterable))

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should execute the expected FindOperation when finding a file'() {
        given:
        def executor = new TestOperationExecutor([Stub(AsyncBatchCursor), Stub(AsyncBatchCursor)])
        def database = databaseWithExecutor(executor)
        def gridFSBucket = new GridFSBucketImpl(database)
        def decoder = registry.get(GridFSFile)
        def callback = Stub(SingleResultCallback)

        when:
        gridFSBucket.find().batchCursor(callback)

        then:
        executor.getReadPreference() == primary()
        expect executor.getReadOperation(), isTheSameAs(new FindOperation<GridFSFile>(new MongoNamespace('test.fs.files'), decoder)
                .filter(new BsonDocument()).retryReads(true))

        when:
        def filter = new BsonDocument('filename', new BsonString('filename'))
        def readConcern = ReadConcern.MAJORITY
        gridFSBucket.withReadPreference(secondary()).withReadConcern(readConcern).find(filter).batchCursor(callback)

        then:
        executor.getReadPreference() == secondary()
        expect executor.getReadOperation(), isTheSameAs(new FindOperation<GridFSFile>(new MongoNamespace('test.fs.files'), decoder)
                .filter(filter).slaveOk(true).retryReads(true))

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should throw an exception if file not found when opening by name'() {
        given:
        def findIterable = Mock(FindIterable)
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Stub(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection, disableMD5)
        when:
        def futureResult = new FutureResultCallback()
        def stream
        if (clientSession != null) {
            stream = gridFSBucket.openDownloadStream(clientSession, 'filename')
        } else {
            stream = gridFSBucket.openDownloadStream('filename')
        }
        stream.read(ByteBuffer.wrap(new byte[10]), futureResult)
        futureResult.get()

        then:
        if (clientSession != null) {
            1 * filesCollection.find(clientSession) >> findIterable
        } else {
            1 * filesCollection.find() >> findIterable
        }
        1 * findIterable.filter(new Document('filename', 'filename')) >> findIterable
        1 * findIterable.skip(0) >> findIterable
        1 * findIterable.sort(new Document('uploadDate', -1)) >> findIterable
        1 * findIterable.first(_) >> { it.last().onResult(null, null) }

        then:
        thrown(MongoGridFSException)

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should delete from files collection then chunks collection'() {
        given:
        def fileId = new ObjectId()
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection, disableMD5)

        when:
        def futureResult = new FutureResultCallback()
        gridFSBucket.delete(fileId, futureResult)
        futureResult.get()

        then:
        1 * filesCollection.deleteOne(new Document('_id', new BsonObjectId(fileId)), _) >> {
            it.last().onResult(DeleteResult.acknowledged(1), null)
        }
        1 * chunksCollection.deleteMany(new Document('files_id', new BsonObjectId(fileId)), _) >> {
            it.last().onResult(DeleteResult.acknowledged(1), null)
        }

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should throw an exception when deleting if no record in the files collection'() {
        given:
        def fileId = new ObjectId()
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection, disableMD5)

        when:
        def futureResult = new FutureResultCallback()
        gridFSBucket.delete(fileId, futureResult)
        futureResult.get()

        then:
        1 * filesCollection.deleteOne(new Document('_id', new BsonObjectId(fileId)), _) >> {
            it.last().onResult(DeleteResult.acknowledged(0), null)
        }
        1 * chunksCollection.deleteMany(new Document('files_id', new BsonObjectId(fileId)), _) >> {
            it.last().onResult(DeleteResult.acknowledged(1), null)
        }

        then:
        thrown(MongoGridFSException)

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should propagate exceptions when deleting'() {
        given:
        def fileId = new ObjectId()
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def deleteException = new MongoException('delete failed')
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection, disableMD5)

        when:
        def futureResult = new FutureResultCallback()
        gridFSBucket.delete(fileId, futureResult)

        then:
        1 * filesCollection.deleteOne(new Document('_id', new BsonObjectId(fileId)), _) >> {
            it.last().onResult(null, deleteException)
        }

        when:
        futureResult.get()

        then:
        def exception = thrown(MongoException)
        exception == deleteException

        when:
        futureResult = new FutureResultCallback()
        gridFSBucket.delete(fileId, futureResult)

        then:
        1 * filesCollection.deleteOne(new Document('_id', new BsonObjectId(fileId)), _) >> {
            it.last().onResult(DeleteResult.acknowledged(0), null)
        }
        1 * chunksCollection.deleteMany(new Document('files_id', new BsonObjectId(fileId)), _) >> {
            it.last().onResult(null, deleteException)
        }

        when:
        futureResult.get()

        then:
        exception = thrown(MongoException)
        exception == deleteException

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should rename a file'() {
        given:
        def fileId = new ObjectId()
        def filesCollection = Mock(MongoCollection)
        def newFilename = 'newFilename'
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, Stub(MongoCollection), disableMD5)

        when:
        def futureResult = new FutureResultCallback()
        gridFSBucket.rename(fileId, newFilename, futureResult)
        futureResult.get()

        then:
        1 * filesCollection.updateOne(new BsonDocument('_id', new BsonObjectId(fileId)),
                new BsonDocument('$set',
                        new BsonDocument('filename', new BsonString(newFilename))), _) >> {
            it.last().onResult(new UpdateResult.UnacknowledgedUpdateResult(), null)
        }

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should throw an exception renaming non existent file'() {
        given:
        def fileId = new ObjectId()
        def filesCollection = Mock(MongoCollection)
        def newFilename = 'newFilename'
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, Stub(MongoCollection), disableMD5)

        when:
        def futureResult = new FutureResultCallback()
        gridFSBucket.rename(fileId, newFilename, futureResult)
        futureResult.get()

        then:
        1 * filesCollection.updateOne(_, _, _) >> { it.last().onResult(new UpdateResult.AcknowledgedUpdateResult(0, 0, null), null) }

        then:
        thrown(MongoGridFSException)

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should handle exceptions when renaming a file'() {
        given:
        def fileId = new ObjectId()
        def filesCollection = Mock(MongoCollection)
        def newFilename = 'newFilename'
        def exception =  new MongoException('failed')
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, Stub(MongoCollection), disableMD5)

        when:
        def futureResult = new FutureResultCallback()
        gridFSBucket.rename(fileId, newFilename, futureResult)
        futureResult.get()

        then:
        1 * filesCollection.updateOne(_, _, _) >> { it.last().onResult(null, exception) }

        then:
        def e = thrown(MongoException)
        e == exception

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should be able to drop the bucket'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection, disableMD5)

        when:
        def futureResult = new FutureResultCallback()
        gridFSBucket.drop(futureResult)
        futureResult.get()

        then:
        1 * filesCollection.drop(_) >> { it.last().onResult(null, null) }
        1 * chunksCollection.drop(_) >> { it.last().onResult(null, null) }

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should handle exceptions when dropping the bucket'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def exception =  new MongoException('failed')
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection, disableMD5)

        when:
        def futureResult = new FutureResultCallback()
        gridFSBucket.drop(futureResult)
        futureResult.get()

        then:
        1 * filesCollection.drop(_) >> { it.last().onResult(null, exception) }

        then:
        def e = thrown(MongoException)
        e == exception

        when:
        futureResult = new FutureResultCallback()
        gridFSBucket.drop(futureResult)
        futureResult.get()

        then:
        1 * filesCollection.drop(_) >> { it.last().onResult(null, null) }
        1 * chunksCollection.drop(_) >> { it.last().onResult(null, exception) }

        then:
        e = thrown(MongoException)
        e == exception
    }

    def 'should validate the clientSession is not null'() {
        given:
        def objectId = new ObjectId()
        def bsonValue = new BsonObjectId(objectId)
        def filename = 'filename'
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def callback = Stub(SingleResultCallback)
        def gridFSBucket = new GridFSBucketImpl('fs', 255, filesCollection, chunksCollection, disableMD5)

        when:
        gridFSBucket.delete(null, objectId, callback)
        then:
        thrown(IllegalArgumentException)

        when:
        gridFSBucket.downloadToStream(null, filename, Stub(AsyncOutputStream), callback)
        then:
        thrown(IllegalArgumentException)

        when:
        gridFSBucket.downloadToStream(null, objectId, Stub(AsyncOutputStream), callback)
        then:
        thrown(IllegalArgumentException)

        when:
        gridFSBucket.drop(null, callback)
        then:
        thrown(IllegalArgumentException)

        when:
        gridFSBucket.find((ClientSession) null)
        then:
        thrown(IllegalArgumentException)

        when:
        gridFSBucket.find((ClientSession) null, new Document())
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
        gridFSBucket.rename(null, objectId, filename, callback)
        then:
        thrown(IllegalArgumentException)

        when:
        gridFSBucket.uploadFromStream((ClientSession) null, filename, Stub(AsyncInputStream), callback)
        then:
        thrown(IllegalArgumentException)

        when:
        gridFSBucket.uploadFromStream(null, bsonValue, filename, Stub(AsyncInputStream), callback)
        then:
        thrown(IllegalArgumentException)
    }
}
