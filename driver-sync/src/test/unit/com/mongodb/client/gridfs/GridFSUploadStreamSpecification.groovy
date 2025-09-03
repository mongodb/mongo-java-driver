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
import com.mongodb.client.ClientSession
import com.mongodb.client.MongoCollection
import com.mongodb.client.gridfs.model.GridFSFile
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonObjectId
import org.bson.BsonString
import org.bson.Document
import spock.lang.Specification

class GridFSUploadStreamSpecification extends Specification {
    def fileId = new BsonObjectId()
    def filename = 'filename'
    def metadata = new Document()

    def 'should return the file id'() {
        when:
        def uploadStream = new GridFSUploadStreamImpl(null, Stub(MongoCollection), Stub(MongoCollection), fileId, filename, 255
                , metadata, null)
        then:
        uploadStream.getId() == fileId
    }

    def 'should write the buffer it reaches the chunk size'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection, chunksCollection, fileId, filename, 2
                , metadata, null)
        when:
        uploadStream.write(1)

        then:
        0 * chunksCollection.insertOne(*_)

        when:
        uploadStream.write(1)

        then:
        if (clientSession != null) {
            1 * chunksCollection.insertOne(clientSession, _)
        } else {
            1 * chunksCollection.insertOne(_)
        }

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should write to the files collection on close'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection, chunksCollection, fileId, filename, 255
                , null, null)

        when:
        uploadStream.write('file content ' as byte[])

        then:
        0 * chunksCollection.insertOne(*_)

        when:
        uploadStream.close()

        then:
        if (clientSession != null) {
            1 * chunksCollection.insertOne(clientSession, _)
            1 * filesCollection.insertOne(clientSession, _)
        } else {
            1 * chunksCollection.insertOne(_)
            1 * filesCollection.insertOne(_)
        }

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should write to the files and chunks collection as expected close'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def content = 'file content ' as byte[]
        def metadata = new Document('contentType', 'text/txt')
        def uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection, chunksCollection, fileId, filename, 255,
                metadata, null)
        def filesId = fileId

        when:
        uploadStream.write(content)
        uploadStream.close()

        then:
        if (clientSession != null) {
            1 * chunksCollection.insertOne(clientSession) {
                verifyAll(it, BsonDocument) {
                    it.get('files_id') == filesId
                    it.getInt32('n') == new BsonInt32(0)
                    it.getBinary('data').getData() == content
                }
            }
        } else {
            1 * chunksCollection.insertOne {
                verifyAll(it, BsonDocument) {
                    it.get('files_id') == filesId
                    it.getInt32('n') == new BsonInt32(0)
                    it.getBinary('data').getData() == content
                }
            }
        }

        then:
        if (clientSession != null) {
            1 * filesCollection.insertOne(clientSession) {
                verifyAll(it, GridFSFile) {
                    it.getId() == fileId
                    it.getFilename() == filename
                    it.getLength() == content.length as Long
                    it.getChunkSize() == 255
                    it.getMetadata() == metadata
                }
            }
        } else {
            1 * filesCollection.insertOne {
                verifyAll(it, GridFSFile) {
                    it.getId() == fileId
                    it.getFilename() == filename
                    it.getLength() == content.length as Long
                    it.getChunkSize() == 255
                    it.getMetadata() == metadata
                }
            }
        }

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should not write an empty chunk'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection, chunksCollection, fileId, filename, 255
                , metadata, null)
        when:
        uploadStream.close()

        then:
        0 * chunksCollection.insertOne(*_)
        if (clientSession != null) {
            1 * filesCollection.insertOne(clientSession, _)
        } else {
            1 * filesCollection.insertOne(_)
        }

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should delete any chunks when calling abort'() {
        given:
        def chunksCollection = Mock(MongoCollection)
        def uploadStream = new GridFSUploadStreamImpl(clientSession, Stub(MongoCollection), chunksCollection, fileId, filename, 255
                , metadata, null)

        when:
        uploadStream.write('file content ' as byte[])
        uploadStream.abort()

        then:
        if (clientSession != null) {
            1 * chunksCollection.deleteMany(clientSession, new Document('files_id', fileId))
        } else {
            1 * chunksCollection.deleteMany(new Document('files_id', fileId))
        }

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should close the stream on abort'() {
        given:
        def uploadStream = new GridFSUploadStreamImpl(clientSession, Stub(MongoCollection), Stub(MongoCollection), fileId, filename, 255
                , metadata, null)
        uploadStream.write('file content ' as byte[])
        uploadStream.abort()

        when:
        uploadStream.write(1)

        then:
        thrown(MongoGridFSException)

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should not do anything when calling flush'() {
        given:
        def chunksCollection = Mock(MongoCollection)
        def uploadStream = new GridFSUploadStreamImpl(clientSession, Stub(MongoCollection), chunksCollection, fileId, filename, 255
                , metadata, null)

        when:
        uploadStream.write('file content ' as byte[])
        uploadStream.flush()

        then:
        0 * chunksCollection.insertOne(*_)

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should throw an exception when trying to action post close'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection, chunksCollection, fileId, filename, 255
                , metadata, null)
        when:
        uploadStream.close()
        uploadStream.write(1)

        then:
        thrown(MongoGridFSException)

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should throw an exception when calling getObjectId and the fileId is not an ObjectId'() {
        given:
        def fileId = new BsonString('myFile')
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection, chunksCollection, fileId, filename, 255
                , metadata, null)
        when:
        uploadStream.getObjectId()

        then:
        thrown(MongoGridFSException)

        where:
        clientSession << [null, Stub(ClientSession)]
    }
}
