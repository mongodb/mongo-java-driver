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

package com.mongodb.reactivestreams.client.internal

import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.WriteConcern
import com.mongodb.internal.async.client.AsyncClientSession as WrappedClientSession
import com.mongodb.internal.async.client.gridfs.AsyncGridFSBucket as WrappedGridFSBucket
import com.mongodb.internal.async.client.gridfs.AsyncGridFSFindIterable
import com.mongodb.reactivestreams.client.ClientSession

import com.mongodb.reactivestreams.client.gridfs.GridFSBucket
import org.bson.BsonObjectId
import org.bson.Document
import org.reactivestreams.Subscriber
import spock.lang.Specification

class GridFSBucketImplSpecification extends Specification {

    def subscriber = Stub(Subscriber) {
        onSubscribe(_) >> { args -> args[0].request(100) }
    }

    def wrappedClientSession = Stub(WrappedClientSession)
    def clientSession = Stub(ClientSession) {
        getWrapped() >> wrappedClientSession
    }

    def 'should have the same methods as the wrapped GridFSBucket'() {
        given:
        def wrapped = (WrappedGridFSBucket.methods*.name)
        def local = (GridFSBucket.methods*.name).sort()

        wrapped = wrapped*.replaceAll('openUploadStream', 'uploadFromPublisher')
        wrapped = wrapped*.replaceAll('openDownloadStream', 'downloadToPublisher')
        wrapped = wrapped.sort()

        expect:
        wrapped == local
    }

    def 'should call the underlying GridFSBucket when getting bucket meta data'() {
        given:
        def wrapped = Mock(WrappedGridFSBucket)
        def bucket = new GridFSBucketImpl(wrapped)

        when:
        bucket.getBucketName()

        then:
        1 * wrapped.getBucketName()

        when:
        bucket.getChunkSizeBytes()

        then:
        1 * wrapped.getChunkSizeBytes()

        when:
        bucket.getWriteConcern()

        then:
        1 * wrapped.getWriteConcern()

        when:
        bucket.getReadPreference()

        then:
        1 * wrapped.getReadPreference()

        when:
        bucket.getReadConcern()

        then:
        1 * wrapped.getReadConcern()
    }

    def 'should call the underlying GridFSBucket when adjusting settings'() {
        given:
        def chunkSizeBytes = 1
        def writeConcern = WriteConcern.MAJORITY
        def readPreference = ReadPreference.secondaryPreferred()
        def readConcern = ReadConcern.MAJORITY

        def wrapped = Mock(WrappedGridFSBucket)
        def bucket = new GridFSBucketImpl(wrapped)

        when:
        bucket.withChunkSizeBytes(chunkSizeBytes)

        then:
        1 * wrapped.withChunkSizeBytes(chunkSizeBytes) >> wrapped

        when:
        bucket.withWriteConcern(writeConcern)

        then:
        1 * wrapped.withWriteConcern(writeConcern) >> wrapped

        when:
        bucket.withReadPreference(readPreference)

        then:
        1 * wrapped.withReadPreference(readPreference) >> wrapped

        when:
        bucket.withReadConcern(readConcern)

        then:
        1 * wrapped.withReadConcern(readConcern) >> wrapped
    }

    def 'should call the underlying find method'() {
        given:
        def filter = new Document('filter', 2)
        def findIterable = Stub(AsyncGridFSFindIterable)
        def wrapped = Mock(WrappedGridFSBucket)
        def bucket = new GridFSBucketImpl(wrapped)

        when:
        bucket.find()

        then:
        1 * wrapped.find() >> findIterable

        when:
        bucket.find(filter)

        then:
        1 * wrapped.find(filter) >> findIterable

        when:
        bucket.find(clientSession)

        then:
        1 * wrapped.find(wrappedClientSession) >> findIterable

        when:
        bucket.find(clientSession, filter)

        then:
        1 * wrapped.find(wrappedClientSession, filter) >> findIterable
    }

    def 'should call the underlying delete method'() {
        given:
        def fileId = new BsonObjectId()
        def wrapped = Mock(WrappedGridFSBucket)
        def bucket = new GridFSBucketImpl(wrapped)

        when:
        bucket.delete(fileId)

        then:
        0 * wrapped.delete(*_)

        when:
        bucket.delete(fileId).subscribe(subscriber)

        then:
        1 * wrapped.delete(fileId, _)

        when:
        bucket.delete(fileId.getValue()).subscribe(subscriber)

        then:
        1 * wrapped.delete(fileId.getValue(), _)

        when:
        bucket.delete(clientSession, fileId)

        then:
        0 * wrapped.delete(*_)

        when:
        bucket.delete(clientSession, fileId).subscribe(subscriber)

        then:
        1 * wrapped.delete(wrappedClientSession, fileId, _)

        when:
        bucket.delete(clientSession, fileId.getValue()).subscribe(subscriber)

        then:
        1 * wrapped.delete(wrappedClientSession, fileId.getValue(), _)
    }

    def 'should call the underlying rename method'() {
        given:
        def fileId = new BsonObjectId()
        def newFilename = 'newFilename'
        def wrapped = Mock(WrappedGridFSBucket)
        def bucket = new GridFSBucketImpl(wrapped)

        when:
        bucket.rename(fileId, newFilename)

        then:
        0 * wrapped.rename(*_)

        when:
        bucket.rename(fileId, newFilename).subscribe(subscriber)

        then:
        1 * wrapped.rename(fileId, newFilename, _)

        when:
        bucket.rename(fileId.getValue(), newFilename).subscribe(subscriber)

        then:
        1 * wrapped.rename(fileId.getValue(), newFilename, _)

        when:
        bucket.rename(clientSession, fileId, newFilename)

        then:
        0 * wrapped.rename(*_)

        when:
        bucket.rename(clientSession, fileId, newFilename).subscribe(subscriber)

        then:
        1 * wrapped.rename(wrappedClientSession, fileId, newFilename, _)

        when:
        bucket.rename(clientSession, fileId.getValue(), newFilename).subscribe(subscriber)

        then:
        1 * wrapped.rename(wrappedClientSession, fileId.getValue(), newFilename, _)
    }

    def 'should call the underlying drop method'() {
        given:
        def wrapped = Mock(WrappedGridFSBucket)
        def bucket = new GridFSBucketImpl(wrapped)

        when:
        bucket.drop()

        then:
        0 * wrapped.drop(*_)

        when:
        bucket.drop().subscribe(subscriber)

        then:
        1 * wrapped.drop(_)


        when:
        bucket.drop(clientSession)

        then:
        0 * wrapped.drop(*_)

        when:
        bucket.drop(clientSession).subscribe(subscriber)

        then:
        1 * wrapped.drop(wrappedClientSession, _)
    }

}
