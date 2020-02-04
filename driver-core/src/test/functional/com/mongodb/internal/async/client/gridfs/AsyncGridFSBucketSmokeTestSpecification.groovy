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

package com.mongodb.internal.async.client.gridfs

import category.Slow
import com.mongodb.MongoGridFSException
import com.mongodb.client.gridfs.model.GridFSFile
import com.mongodb.client.gridfs.model.GridFSUploadOptions
import com.mongodb.internal.async.client.AsyncMongoClients
import com.mongodb.internal.async.client.AsyncMongoCollection
import com.mongodb.internal.async.client.AsyncMongoDatabase
import com.mongodb.internal.async.client.FunctionalSpecification
import org.bson.BsonDocument
import org.bson.BsonString
import org.bson.Document
import org.bson.UuidRepresentation
import org.bson.types.ObjectId
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf
import spock.lang.Unroll

import java.nio.ByteBuffer
import java.security.SecureRandom

import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.client.model.Filters.eq
import static com.mongodb.client.model.Updates.unset
import static com.mongodb.internal.async.client.Fixture.getDefaultDatabaseName
import static com.mongodb.internal.async.client.Fixture.getMongoClient
import static com.mongodb.internal.async.client.Fixture.getMongoClientBuilderFromConnectionString
import static com.mongodb.internal.async.client.TestHelper.run
import static com.mongodb.internal.async.client.TestHelper.runSlow

class AsyncGridFSBucketSmokeTestSpecification extends FunctionalSpecification {
    protected AsyncMongoDatabase mongoDatabase;
    protected AsyncMongoCollection<GridFSFile> filesCollection;
    protected AsyncMongoCollection<Document> chunksCollection;
    protected AsyncGridFSBucket gridFSBucket;
    def singleChunkString = 'GridFS'
    def multiChunkString = singleChunkString.padLeft(1024 * 255 * 5)

    def setup() {
        mongoDatabase = getMongoClient().getDatabase(getDefaultDatabaseName())
        filesCollection = mongoDatabase.getCollection('fs.files', GridFSFile)
        chunksCollection = mongoDatabase.getCollection('fs.chunks')
        run(filesCollection.&drop)
        run(chunksCollection.&drop)
        gridFSBucket = new AsyncGridFSBucketImpl(mongoDatabase)
    }

    def cleanup() {
        if (filesCollection != null) {
            run(filesCollection.&drop)
            run(chunksCollection.&drop)
        }
    }

    @Unroll
    def 'should round trip a #description'() {
        given:
        def content = multiChunk ? multiChunkString : singleChunkString
        def contentBytes = content as byte[]
        def expectedLength = contentBytes.length
        def bucket = gridFSBucket

        when:
        def outputStream = bucket.openUploadStream('myFile')
        run(outputStream.&write, ByteBuffer.wrap(contentBytes))
        run(outputStream.&close)
        def fileId = outputStream.getObjectId()

        then:
        run(filesCollection.&countDocuments) == 1
        run(chunksCollection.&countDocuments) == chunkCount

        when:
        def fileInfo = run(bucket.find().filter(eq('_id', fileId)).&first)

        then:
        fileInfo.getId().getValue() == fileId
        fileInfo.getChunkSize() == bucket.getChunkSizeBytes()
        fileInfo.getLength() == expectedLength
        fileInfo.getMetadata() == null

        when:
        def byteBuffer = ByteBuffer.allocate(fileInfo.getLength() as int)
        run(bucket.openDownloadStream(fileId).&read, byteBuffer)

        then:
        byteBuffer.array() == contentBytes

        where:
        description                     | multiChunk | chunkCount
        'a small file directly'         | false      | 1
        'a large file directly'         | true       | 5
    }

    @Category(Slow)
    def 'should round trip with small chunks'() {
        given:
        def contentSize = 1024 * 500
        def chunkSize = 10
        def contentBytes = new byte[contentSize];
        new SecureRandom().nextBytes(contentBytes);
        def options = new GridFSUploadOptions().chunkSizeBytes(chunkSize)

        when:
        def outputStream = gridFSBucket.openUploadStream('myFile', options)
        run(outputStream.&write, ByteBuffer.wrap(contentBytes))
        run(outputStream.&close)
        def fileId = outputStream.getObjectId()

        then:
        run(filesCollection.&countDocuments) == 1
        run(chunksCollection.&countDocuments) == contentSize / chunkSize

        when:
        def downloadStream = gridFSBucket.openDownloadStream(fileId)
        def fileInfo = run(downloadStream.&getGridFSFile)
        def byteBuffer = ByteBuffer.allocate(fileInfo.getLength() as int)
        runSlow(downloadStream.&read, byteBuffer)

        then:
        byteBuffer.array() == contentBytes
    }

    @Category(Slow)
    def 'should round trip with data larger than the internal bufferSize'() {
        given:
        def contentSize = 1024 * 1024 * 5
        def chunkSize = 1024 * 1024
        def contentBytes = new byte[contentSize];
        new SecureRandom().nextBytes(contentBytes);
        def options = new GridFSUploadOptions().chunkSizeBytes(chunkSize)

        when:
        def uploadStream = gridFSBucket.openUploadStream('myFile', options)
        runSlow(uploadStream.&write, ByteBuffer.wrap(contentBytes))
        run(uploadStream.&close)
        def fileId = uploadStream.getObjectId()

        then:
        run(filesCollection.&countDocuments) == 1
        run(chunksCollection.&countDocuments) == contentSize / chunkSize

        when:
        def downloadStream = gridFSBucket.openDownloadStream(fileId)
        def fileInfo = run(downloadStream.&getGridFSFile)
        def byteBuffer = ByteBuffer.allocate(fileInfo.getLength() as int)
        runSlow(downloadStream.&read, byteBuffer)

        then:
        byteBuffer.array() == contentBytes
    }

    def 'should handle custom ids'() {
        def content = multiChunkString
        def contentBytes = content as byte[]
        def fileId = new BsonString('myFile')
        def byteBuffer = ByteBuffer.allocate(contentBytes.length)

        when:
        def uploadStream = gridFSBucket.openUploadStream(fileId, 'myFile')
        run(uploadStream.&write, ByteBuffer.wrap(contentBytes))
        run(uploadStream.&close)

        run(gridFSBucket.openDownloadStream(fileId).&read, byteBuffer)

        then:
        byteBuffer.array() == contentBytes

        when:
        run(gridFSBucket.&rename, fileId, 'newName')
        byteBuffer = ByteBuffer.allocate(contentBytes.length)
        run(gridFSBucket.openDownloadStream('newName').&read, byteBuffer)

        then:
        byteBuffer.array() == contentBytes

        when:
        run(gridFSBucket.&delete, fileId)

        then:
        run(filesCollection.&countDocuments) == 0
        run(chunksCollection.&countDocuments) == 0
    }

    def 'should throw a chunk not found error when there are no chunks'() {
        given:
        def contentSize = 1024 * 1024
        def contentBytes = new byte[contentSize]
        new SecureRandom().nextBytes(contentBytes)
        def fileId = new BsonString('myFile')

        when:
        def uploadStream = gridFSBucket.openUploadStream(fileId, 'myFile')
        run(uploadStream.&write, ByteBuffer.wrap(contentBytes))
        run(uploadStream.&close)

        run(chunksCollection.&deleteMany, eq('files_id', fileId))
        run(gridFSBucket.openDownloadStream(fileId).&read, ByteBuffer.allocate(contentSize))

        then:
        thrown(MongoGridFSException)
    }

    def 'should read across chunks'() {
        given:
        def contentBytes = new byte[9000];
        new SecureRandom().nextBytes(contentBytes);
        def bufferSize = 2000
        def options = new GridFSUploadOptions().chunkSizeBytes(4000)
        def fileId = new BsonString('myFile')

        when:
        def uploadStream = gridFSBucket.openUploadStream(fileId, 'myFile', options)
        run(uploadStream.&write, ByteBuffer.wrap(contentBytes))
        run(uploadStream.&close)

        then:
        run(filesCollection.&countDocuments) == 1
        run(chunksCollection.&countDocuments) == 3

        when:
        def totalRead = 0
        def fileBuffer = ByteBuffer.allocate(9000)
        def byteBuffer = ByteBuffer.allocate(bufferSize)
        def downloadStream = gridFSBucket.openDownloadStream(fileId)
        def read = run(downloadStream.&read, byteBuffer.clear())

        then:
        read == bufferSize

        when:
        fileBuffer.put(byteBuffer.array())
        totalRead += read
        read = run(downloadStream.&read, byteBuffer.clear())

        then:
        read == bufferSize

        when:
        fileBuffer.put(byteBuffer.array())
        totalRead += read
        read = run(downloadStream.&read, byteBuffer.clear())

        then:
        read == bufferSize

        when:
        fileBuffer.put(byteBuffer.array())
        totalRead += read
        read = run(downloadStream.&read, byteBuffer.clear())
        then:
        read == bufferSize

        when:
        fileBuffer.put(byteBuffer.array())
        totalRead += read
        read = run(downloadStream.&read, byteBuffer.clear())

        then:
        read == 1000

        when:
        def remaining = new byte[read]
        byteBuffer.flip()
        byteBuffer.get(remaining, 0, 1000)
        fileBuffer.put(remaining)
        totalRead += read
        read = run(downloadStream.&read, byteBuffer.clear())

        then:
        read == -1

        then:
        fileBuffer.array() == contentBytes

        then:
        run(downloadStream.&close) == null
    }

    def 'should round trip with a batchSize of 1'() {
        given:
        def content = multiChunkString
        def contentBytes = content as byte[]
        def expectedLength = contentBytes.length as Long
        ObjectId fileId

        when:
        def uploadStream = gridFSBucket.openUploadStream('myFile')
        run(uploadStream.&write, ByteBuffer.wrap(contentBytes))
        run(uploadStream.&close)
        fileId = uploadStream.getObjectId()

        then:
        run(filesCollection.&countDocuments) == 1
        run(chunksCollection.&countDocuments) == 5

        when:
        def fileInfo = run(gridFSBucket.find().filter(eq('_id', fileId)).&first)

        then:
        fileInfo.getObjectId() == fileId
        fileInfo.getChunkSize() == gridFSBucket.getChunkSizeBytes()
        fileInfo.getLength() == expectedLength
        fileInfo.getMetadata() == null

        when:
        def byteBuffer = ByteBuffer.allocate(fileInfo.getLength() as int)
        run(gridFSBucket.openDownloadStream(fileId).batchSize(1).&read, byteBuffer)

        then:
        byteBuffer.array() == contentBytes
    }

    def 'should use custom uploadOptions when uploading' () {
        given:
        def chunkSize = 20
        def metadata = new Document('archived', false)
        def options = new GridFSUploadOptions()
                .chunkSizeBytes(chunkSize)
                .metadata(metadata)
        def content = 'qwerty' * 1024
        def contentBytes = content as byte[]
        def expectedLength = contentBytes.length as Long
        def expectedNoChunks = Math.ceil((expectedLength as double) / chunkSize) as int

        when:
        def uploadStream = gridFSBucket.openUploadStream('myFile', options)
        run(uploadStream.&write, ByteBuffer.wrap(contentBytes))
        run(uploadStream.&close)
        def fileId = uploadStream.getObjectId()


        then:
        run(filesCollection.&countDocuments) == 1
        run(chunksCollection.&countDocuments) == expectedNoChunks

        when:
        def fileInfo = run(gridFSBucket.find().filter(eq('_id', fileId)).&first)

        then:
        fileInfo.getId().getValue() == fileId
        fileInfo.getChunkSize() == options.getChunkSizeBytes()
        fileInfo.getLength() == expectedLength
        fileInfo.getMetadata() == options.getMetadata()

        when:
        def byteBuffer = ByteBuffer.allocate(fileInfo.getLength() as int)
        run(gridFSBucket.openDownloadStream(fileId).&read, byteBuffer)

        then:
        byteBuffer.array() == contentBytes
    }

    def 'should be able to open by name'() {
        given:
        def content = 'Hello GridFS'
        def contentBytes = content as byte[]
        def filename = 'myFile'
        def uploadStream = gridFSBucket.openUploadStream('myFile')
        run(uploadStream.&write, ByteBuffer.wrap(contentBytes))
        run(uploadStream.&close)
        def fileId = uploadStream.getObjectId()
        def fileInfo = run(gridFSBucket.find(new Document('_id', fileId)).&first)

        when:
        def byteBuffer = ByteBuffer.allocate(fileInfo.getLength() as int)
        run(gridFSBucket.openDownloadStream(filename).&read, byteBuffer)

        then:
        byteBuffer.array() == contentBytes
    }

    def 'should be able to handle missing file'() {
        when:
        def filename = 'myFile'
        def byteBuffer = ByteBuffer.allocate(10)
        run(gridFSBucket.openDownloadStream(filename).&read, byteBuffer)

        then:
        thrown(MongoGridFSException)
    }

    def 'should abort and cleanup'() {
        when:
        def contentBytes = multiChunkString as byte[]

        then:
        run(filesCollection.&countDocuments) == 0

        when:
        def uploadStream = gridFSBucket.openUploadStream('myFile')
        run(uploadStream.&write, ByteBuffer.wrap(contentBytes))
        run(uploadStream.&abort)

        then:
        run(filesCollection.&countDocuments) == 0
        run(chunksCollection.&countDocuments) == 0
    }

    def 'should create the indexes as expected'() {
        when:
        def filesIndexKey = Document.parse('{ filename: 1, uploadDate: 1 }')
        def chunksIndexKey = Document.parse('{ files_id: 1, n: 1 }')

        then:
        !run(filesCollection.listIndexes().&into, [])*.get('key').contains(filesIndexKey)
        !run(chunksCollection.listIndexes().&into, [])*.get('key').contains(chunksIndexKey)

        when:
        def uploadStream = gridFSBucket.openUploadStream('myFile',)
        run(uploadStream.&write, ByteBuffer.wrap(multiChunkString.getBytes()))
        run(uploadStream.&close)

        then:
        run(filesCollection.listIndexes().&into, [])*.get('key').contains(Document.parse('{ filename: 1, uploadDate: 1 }'))
        run(chunksCollection.listIndexes().&into, [])*.get('key').contains(Document.parse('{ files_id: 1, n: 1 }'))
    }

    def 'should not create indexes if the files collection is not empty'() {
        when:
        run(filesCollection.withDocumentClass(Document).&insertOne, new Document('filename', 'bad file'))
        def contentBytes = 'Hello GridFS' as byte[]

        then:
        run(filesCollection.listIndexes().&into, []).size() == 1
        run(chunksCollection.listIndexes().&into, []).size() == 0

        when:
        def outputStream = gridFSBucket.openUploadStream('myFile')
        run(outputStream.&write, ByteBuffer.wrap(contentBytes))
        run(outputStream.&close)

        then:
        run(filesCollection.listIndexes().&into, []).size() == 1
        run(chunksCollection.listIndexes().&into, []).size() == 1
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) })
    def 'should not create if index is numerically the same'() {
        when:
        run(filesCollection.&createIndex, new Document('filename', indexValue1).append('uploadDate', indexValue2))
        run(chunksCollection.&createIndex, new Document('files_id', indexValue1).append('n', indexValue2))
        def contentBytes = 'Hello GridFS' as byte[]

        then:
        run(filesCollection.listIndexes().&into, []).size() == 2
        run(chunksCollection.listIndexes().&into, []).size() == 2

        when:
        def outputStream = gridFSBucket.openUploadStream('myFile')
        run(outputStream.&write, ByteBuffer.wrap(contentBytes))
        run(outputStream.&close)

        then:
        run(filesCollection.listIndexes().&into, []).size() == 2
        run(chunksCollection.listIndexes().&into, []).size() == 2

        where:
        [indexValue1, indexValue2] << [[1, 1.0, 1L], [1, 1.0, 1L]].combinations()
    }

    def 'should use the user provided codec registries for encoding / decoding data'() {
        given:
        def client = AsyncMongoClients.create(getMongoClientBuilderFromConnectionString()
                .uuidRepresentation(UuidRepresentation.STANDARD)
                .build())
        def database = client.getDatabase(getDefaultDatabaseName())
        def uuid = UUID.randomUUID()
        def fileMeta = new Document('uuid', uuid)
        def gridFSBucket = AsyncGridFSBuckets.create(database)
        def options = new GridFSUploadOptions().metadata(fileMeta)

        when:
        def uploadStream = gridFSBucket.openUploadStream('myFile', options)
        run(uploadStream.&write, ByteBuffer.wrap(multiChunkString.getBytes()))
        run(uploadStream.&close)
        def fileId = uploadStream.getObjectId()
        def file = run(gridFSBucket.find(new Document('_id', fileId)).&first)

        then:
        file.getMetadata() == fileMeta

        when:
        def fileAsDocument = run(filesCollection.find(BsonDocument).&first)

        then:
        fileAsDocument.getDocument('metadata').getBinary('uuid').getType() == 4 as byte

        cleanup:
        client?.close()
    }

    def 'should handle missing file name data when downloading'() {
        given:
        def content = multiChunkString
        def contentBytes = content as byte[]
        ObjectId fileId

        when:
        def uploadStream = gridFSBucket.openUploadStream('myFile')
        run(uploadStream.&write, ByteBuffer.wrap(contentBytes))
        run(uploadStream.&close)
        fileId = uploadStream.getObjectId()

        then:
        run(filesCollection.&countDocuments) == 1

        when:
        // Remove filename
        run(filesCollection.&updateOne, eq('_id', fileId), unset('filename'))

        def byteBuffer = ByteBuffer.allocate(contentBytes.length)
        run(gridFSBucket.openDownloadStream(fileId).&read, byteBuffer)

        then:
        byteBuffer.array() == contentBytes
    }
}

