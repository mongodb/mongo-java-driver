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

package com.mongodb.async.client.gridfs.helpers

import com.mongodb.MongoGridFSException
import com.mongodb.async.client.FunctionalSpecification
import com.mongodb.async.client.MongoCollection
import com.mongodb.async.client.MongoDatabase
import com.mongodb.async.client.gridfs.GridFSBucket
import com.mongodb.async.client.gridfs.GridFSBucketImpl
import org.bson.Document
import spock.lang.Shared

import java.nio.ByteBuffer

import static com.mongodb.async.client.Fixture.getDefaultDatabaseName
import static com.mongodb.async.client.Fixture.getMongoClient
import static com.mongodb.async.client.TestHelper.run
import static com.mongodb.async.client.gridfs.helpers.AsyncStreamHelper.toAsyncInputStream
import static com.mongodb.async.client.gridfs.helpers.AsyncStreamHelper.toAsyncOutputStream

class AsyncStreamHelperTestSpecification extends FunctionalSpecification {
    protected MongoDatabase mongoDatabase;
    protected MongoCollection<Document> filesCollection;
    protected MongoCollection<Document> chunksCollection;
    protected GridFSBucket gridFSBucket;
    @Shared
    def content = 'Hello GridFS Round Trip'.getBytes()

    def setup() {
        mongoDatabase = getMongoClient().getDatabase(getDefaultDatabaseName())
        filesCollection = mongoDatabase.getCollection('fs.files')
        chunksCollection = mongoDatabase.getCollection('fs.chunks')
        run(filesCollection.&drop)
        run(chunksCollection.&drop)
        gridFSBucket = new GridFSBucketImpl(mongoDatabase)
    }

    def cleanup() {
        if (filesCollection != null) {
            run(filesCollection.&drop)
            run(chunksCollection.&drop)
        }
    }

    def 'should round trip a bytes'() {
        given:
        def inputStream = toAsyncInputStream(sourceData)
        def outputStream = toAsyncOutputStream(destinationData)

        when:
        def objectId = run(gridFSBucket.&uploadFromStream, 'myFile', inputStream)
        run(inputStream.&close)

        then:
        run(filesCollection.&countDocuments) == 1
        run(chunksCollection.&countDocuments) == 1

        when:

        run(gridFSBucket.&downloadToStream, objectId, outputStream)
        run(outputStream.&close)

        then:
        if (destinationData instanceof OutputStream) {
            destinationData.toByteArray() == content
        } else {
            destinationData == sourceData
        }

        where:
        sourceData                          | destinationData
        content                             | new byte[content.length]
        ByteBuffer.wrap(content)            | ByteBuffer.allocate(content.length)
        new ByteArrayInputStream(content)   | new ByteArrayOutputStream(content.length)
    }

    def 'should accept data bigger than the chunkSize'() {
        when:
        def inputStream = toAsyncInputStream(dataSource)
        run(gridFSBucket.withChunkSizeBytes(25).&uploadFromStream, 'myFile', inputStream)
        run(inputStream.&close)

        then:
        run(filesCollection.&countDocuments) == 1
        run(chunksCollection.&countDocuments) == 2

        where:
        dataSource << [new byte[50], ByteBuffer.allocate(50), new ByteArrayInputStream(new byte[50])]
    }

    def 'should handle empty ByteBuffers'() {
        when:
        def amountRead = run(toAsyncInputStream(Stub(InputStream)).&read, ByteBuffer.allocate(0))

        then:
        amountRead == -1

        when:
        def amountWritten = run(toAsyncOutputStream(Stub(OutputStream)).&write, ByteBuffer.allocate(0))

        then:
        amountWritten == -1
    }

    def 'should handle InputStream errors'() {
        given:
        def inputStream = Stub(InputStream) {
            read(_) >> { throw new IOException('read failed') }
            close() >> { throw new IOException('close failed') }
        }

        when:
        run(toAsyncInputStream(inputStream).&read, ByteBuffer.wrap(content))

        then:
        thrown(MongoGridFSException)

        when:
        run(toAsyncInputStream(inputStream).&close)

        then:
        thrown(MongoGridFSException)
    }

    def 'should handle OutputStream errors'() {
        given:
        def outputStream = Stub(OutputStream) {
            write(_) >> { throw new IOException('write failed') }
            close() >> { throw new IOException('close failed') }
        }


        when:
        run(toAsyncOutputStream(outputStream).&write, ByteBuffer.allocate(1024))

        then:
        thrown(MongoGridFSException)

        when:
        run(toAsyncOutputStream(outputStream).&close)

        then:
        thrown(MongoGridFSException)
    }
}
