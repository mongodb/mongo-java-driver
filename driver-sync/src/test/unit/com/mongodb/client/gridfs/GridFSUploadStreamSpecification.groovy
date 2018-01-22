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
import com.mongodb.client.MongoCollection
import com.mongodb.client.gridfs.model.GridFSFile
import com.mongodb.session.ClientSession
import org.bson.BsonObjectId
import org.bson.BsonString
import org.bson.Document
import org.bson.types.Binary
import spock.lang.Specification

import java.security.MessageDigest

class GridFSUploadStreamSpecification extends Specification {
    def fileId = new BsonObjectId()
    def filename = 'filename'
    def metadata = new Document()

    def 'should return the file id'() {
        when:
        def uploadStream = new GridFSUploadStreamImpl(null, Stub(MongoCollection), Stub(MongoCollection), fileId, filename, 255, metadata)

        then:
        uploadStream.getId() == fileId
    }

    def 'should write the buffer it reaches the chunk size'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection, chunksCollection, fileId, filename, 2, metadata)

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
        def uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection, chunksCollection, fileId, filename, 255, null)

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
        def uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection, chunksCollection, fileId, filename, 255, metadata)
        def chunksData
        def fileData

        when:
        uploadStream.write(content)
        uploadStream.close()

        then:
        if (clientSession != null) {
            1 * chunksCollection.insertOne(clientSession) { Document data -> chunksData = data }
        } else {
            1 * chunksCollection.insertOne { Document data -> chunksData = data }
        }

        chunksData.get('files_id') == fileId
        chunksData.getInteger('n') == 0
        chunksData.get('data', Binary).getData() == content

        then:
        if (clientSession != null) {
            1 * filesCollection.insertOne(clientSession) { GridFSFile data -> fileData = data }
        } else {
            1 * filesCollection.insertOne { GridFSFile data -> fileData = data }
        }

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
        def uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection, chunksCollection, fileId, filename, 255, metadata)

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
        def uploadStream = new GridFSUploadStreamImpl(clientSession, Stub(MongoCollection), chunksCollection, fileId, filename, 255,
                metadata)

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
        def uploadStream = new GridFSUploadStreamImpl(clientSession, Stub(MongoCollection), Stub(MongoCollection), fileId, filename, 255,
                metadata)
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
        def uploadStream = new GridFSUploadStreamImpl(clientSession, Stub(MongoCollection), chunksCollection, fileId, filename, 255,
                metadata)

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
        def uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection, chunksCollection, fileId, filename, 255, metadata)

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
        def uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection, chunksCollection, fileId, filename, 255, metadata)

        when:
        uploadStream.getObjectId()

        then:
        thrown(MongoGridFSException)

        where:
        clientSession << [null, Stub(ClientSession)]
    }
}
