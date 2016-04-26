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
import org.bson.Document
import org.bson.types.Binary
import org.bson.types.ObjectId
import spock.lang.Specification

import java.security.MessageDigest

class GridFSUploadStreamSpecification extends Specification {
    def fileId = new ObjectId()
    def filename = 'filename'
    def metadata = new Document()

    def 'should return the file id'() {
        when:
        def uploadStream = new GridFSUploadStreamImpl(Stub(MongoCollection), Stub(MongoCollection), fileId, filename, 255, metadata)

        then:
        uploadStream.getFileId() == fileId
    }

    def 'should write the buffer it reaches the chunk size'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def uploadStream = new GridFSUploadStreamImpl(filesCollection, chunksCollection, fileId, filename, 2, metadata)

        when:
        uploadStream.write(1)

        then:
        0 * chunksCollection.insertOne(_)

        when:
        uploadStream.write(1)

        then:
        1 * chunksCollection.insertOne(_)
    }

    def 'should write to the files collection on close'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def uploadStream = new GridFSUploadStreamImpl(filesCollection, chunksCollection, fileId, filename, 255, null)

        when:
        uploadStream.write('file content ' as byte[])

        then:
        0 * chunksCollection.insertOne(_)

        when:
        uploadStream.close()

        then:
        1 * chunksCollection.insertOne(_)
        1 * filesCollection.insertOne(_)
    }

    def 'should write to the files and chunks collection as expected close'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def content = 'file content ' as byte[]
        def metadata = new Document('contentType', 'text/txt')
        def uploadStream = new GridFSUploadStreamImpl(filesCollection, chunksCollection, fileId, filename, 255, metadata)
        def chunksData
        def fileData

        when:
        uploadStream.write(content)
        uploadStream.close()

        then:
        1 * chunksCollection.insertOne { Document data -> chunksData = data }

        then:
        1 * filesCollection.insertOne { GridFSFile data -> fileData = data }
        then:
        chunksData.getObjectId('files_id') == fileId
        chunksData.getInteger('n') == 0
        chunksData.get('data', Binary).getData() == content

        fileData.getObjectId() == fileId
        fileData.getFilename() == filename
        fileData.getLength() == content.length as Long
        fileData.getChunkSize() == 255
        fileData.getMD5() == MessageDigest.getInstance('MD5').digest(content).encodeHex().toString()
        fileData.getMetadata() == metadata
    }

    def 'should not write an empty chunk'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def uploadStream = new GridFSUploadStreamImpl(filesCollection, chunksCollection, fileId, filename, 255, metadata)

        when:
        uploadStream.close()

        then:
        0 * chunksCollection.insertOne(_)
        1 * filesCollection.insertOne(_)
    }

    def 'should delete any chunks when calling abort'() {
        given:
        def chunksCollection = Mock(MongoCollection)
        def uploadStream = new GridFSUploadStreamImpl(Stub(MongoCollection), chunksCollection, fileId, filename, 255, metadata)

        when:
        uploadStream.write('file content ' as byte[])
        uploadStream.abort()

        then:
        1 * chunksCollection.deleteMany(new Document('files_id', fileId))
    }

    def 'should close the stream on abort'() {
        given:
        def uploadStream = new GridFSUploadStreamImpl(Stub(MongoCollection), Stub(MongoCollection), fileId, filename, 255, metadata)
        uploadStream.write('file content ' as byte[])
        uploadStream.abort()

        when:
        uploadStream.write(1)

        then:
        thrown(MongoGridFSException)
    }

    def 'should not do anything when calling flush'() {
        given:
        def chunksCollection = Mock(MongoCollection)
        def uploadStream = new GridFSUploadStreamImpl(Stub(MongoCollection), chunksCollection, fileId, filename, 255, metadata)

        when:
        uploadStream.write('file content ' as byte[])
        uploadStream.flush()

        then:
        0 * chunksCollection.insertOne(_)
    }

    def 'should throw an exception when trying to action post close'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def uploadStream = new GridFSUploadStreamImpl(filesCollection, chunksCollection, fileId, filename, 255, metadata)

        when:
        uploadStream.close()
        uploadStream.write(1)

        then:
        thrown(MongoGridFSException)
    }
}
