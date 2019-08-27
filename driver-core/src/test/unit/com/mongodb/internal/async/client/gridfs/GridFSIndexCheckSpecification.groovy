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

import com.mongodb.MongoException
import com.mongodb.async.FutureResultCallback
import com.mongodb.internal.async.SingleResultCallback
import com.mongodb.internal.async.client.ClientSession
import com.mongodb.internal.async.client.FindIterable
import com.mongodb.internal.async.client.ListIndexesIterable
import com.mongodb.internal.async.client.MongoCollection
import org.bson.Document
import spock.lang.Specification

import static com.mongodb.ReadPreference.primary

class GridFSIndexCheckSpecification extends Specification {
    private final Document projection = new Document('_id', 1)
    private final MongoException exception = new MongoException('failure')

    def 'should do nothing more if files collection contains any documents'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def findIterable = Mock(FindIterable)
        def indexChecker = new GridFSIndexCheckImpl(clientSession, filesCollection, chunksCollection)

        when:
        indexChecker.checkAndCreateIndex(Stub(SingleResultCallback))

        then:
        1 * filesCollection.withDocumentClass(Document) >> filesCollection
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        if (clientSession != null){
            1 * filesCollection.find(clientSession) >> findIterable
        } else {
            1 * filesCollection.find() >> findIterable
        }
        1 * findIterable.projection(projection) >> findIterable
        1 * findIterable.first(_) >> { it.last().onResult(new Document(), null) }

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should not do anything if indexes exist'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def listIndexesIterable = Mock(ListIndexesIterable)
        def findIterable = Mock(FindIterable)
        def indexChecker = new GridFSIndexCheckImpl(clientSession, filesCollection, chunksCollection)

        when:
        indexChecker.checkAndCreateIndex(Stub(SingleResultCallback))

        then:
        1 * filesCollection.withDocumentClass(Document) >> filesCollection
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        if (clientSession != null){
            1 * filesCollection.find(clientSession) >> findIterable
        } else {
            1 * filesCollection.find() >> findIterable
        }
        1 * findIterable.projection(projection) >> findIterable
        1 * findIterable.first(_) >> { it.last().onResult(null, null) }

        // Files Index check
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        if (clientSession != null){
            1 * filesCollection.listIndexes(clientSession) >> listIndexesIterable
        } else {
            1 * filesCollection.listIndexes() >> listIndexesIterable
        }
        1 * listIndexesIterable.into(_, _) >> {
            it.last().onResult([Document.parse('{"key": {"_id": 1}}'),
                            Document.parse('{"key": {"filename": 1, "uploadDate": 1 }}')], null)
        }
        0 * filesCollection.createIndex(*_)

        // Chunks Index check
        1 * chunksCollection.withReadPreference(primary()) >> chunksCollection
        if (clientSession != null){
            1 * chunksCollection.listIndexes(clientSession) >> listIndexesIterable
        } else {
            1 * chunksCollection.listIndexes() >> listIndexesIterable
        }
        1 * listIndexesIterable.into(_, _) >> {
            it.last().onResult([Document.parse('{"key": {"_id": 1}}'),
                            Document.parse('{"key": {"files_id": 1, "n": 1 }}')], null)
        }
        0 * chunksCollection.createIndex(*_)

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should create a chunks index but not a files index if it exists'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def listIndexesIterable = Mock(ListIndexesIterable)
        def findIterable = Mock(FindIterable)
        def indexChecker = new GridFSIndexCheckImpl(clientSession, filesCollection, chunksCollection)

        when:
        indexChecker.checkAndCreateIndex(Stub(SingleResultCallback))

        then:
        1 * filesCollection.withDocumentClass(Document) >> filesCollection
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        if (clientSession != null){
            1 * filesCollection.find(clientSession) >> findIterable
        } else {
            1 * filesCollection.find() >> findIterable
        }
        1 * findIterable.projection(projection) >> findIterable
        1 * findIterable.first(_) >> { it.last().onResult(null, null) }

        // Files Index check
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        if (clientSession != null){
            1 * filesCollection.listIndexes(clientSession) >> listIndexesIterable
        } else {
            1 * filesCollection.listIndexes() >> listIndexesIterable
        }
        1 * listIndexesIterable.into(_, _) >> {
            it.last().onResult([Document.parse('{"key": {"_id": 1}}'),
                            Document.parse('{"key": {"filename": 1, "uploadDate": 1 }}')], null)
        }
        0 * filesCollection.createIndex(*_)

        // Chunks Index check
        1 * chunksCollection.withReadPreference(primary()) >> chunksCollection
        if (clientSession != null){
            1 * chunksCollection.listIndexes(clientSession) >> listIndexesIterable
        } else {
            1 * chunksCollection.listIndexes() >> listIndexesIterable
        }
        1 * listIndexesIterable.into(_, _) >> { it.last().onResult([], null) }
        if (clientSession != null) {
            1 * chunksCollection.createIndex(clientSession, { index -> index == Document.parse('{"files_id": 1, "n": 1}') },
                    { indexOptions -> indexOptions.isUnique() }, _) >> { it.last().onResult('files_id_1', null) }
        } else {
            1 * chunksCollection.createIndex({ index -> index == Document.parse('{"files_id": 1, "n": 1}') },
                    { indexOptions -> indexOptions.isUnique() }, _) >> { it.last().onResult('files_id_1', null) }
        }

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should create a files index but not a chunks index if it exists'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def listIndexesIterable = Mock(ListIndexesIterable)
        def findIterable = Mock(FindIterable)
        def indexChecker = new GridFSIndexCheckImpl(clientSession, filesCollection, chunksCollection)

        when:
        indexChecker.checkAndCreateIndex(Stub(SingleResultCallback))

        then:
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        if (clientSession != null){
            1 * filesCollection.find(clientSession) >> findIterable
        } else {
            1 * filesCollection.find() >> findIterable
        }
        1 * findIterable.projection(projection) >> findIterable
        1 * findIterable.first(_) >> { it.last().onResult(null, null) }

        // Files Index check
        1 * filesCollection.withDocumentClass(Document) >> filesCollection
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        if (clientSession != null){
            1 * filesCollection.listIndexes(clientSession) >> listIndexesIterable
        } else {
            1 * filesCollection.listIndexes() >> listIndexesIterable
        }
        1 * listIndexesIterable.into(_, _) >> { it.last().onResult([], null) }
        if (clientSession != null){
            1 * filesCollection.createIndex(clientSession, { index -> index == Document.parse('{"filename": 1, "uploadDate": 1 }') }, _) >>
                    { it.last().onResult('filename_1', null) }
        } else {
            1 * filesCollection.createIndex({ index -> index == Document.parse('{"filename": 1, "uploadDate": 1 }') }, _) >>
                    { it.last().onResult('filename_1', null) }
        }

        // Chunks Index check
        1 * chunksCollection.withReadPreference(primary()) >> chunksCollection
        if (clientSession != null){
            1 * chunksCollection.listIndexes(clientSession) >> listIndexesIterable
        } else {
            1 * chunksCollection.listIndexes() >> listIndexesIterable
        }
        1 * listIndexesIterable.into(_, _) >> {
            it.last().onResult([Document.parse('{"key": {"_id": 1}}'),
                            Document.parse('{"key": {"files_id": 1, "n": 1 }}')], null)
        }
        0 * chunksCollection.createIndex(*_)

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should create indexes if empty files collection and no indexes'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def listIndexesIterable = Mock(ListIndexesIterable)
        def findIterable = Mock(FindIterable)
        def indexChecker = new GridFSIndexCheckImpl(clientSession, filesCollection, chunksCollection)

        when:
        indexChecker.checkAndCreateIndex(Stub(SingleResultCallback))

        then:
        1 * filesCollection.withDocumentClass(Document) >> filesCollection
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        if (clientSession != null){
            1 * filesCollection.find(clientSession) >> findIterable
        } else {
            1 * filesCollection.find() >> findIterable
        }
        1 * findIterable.projection(projection) >> findIterable
        1 * findIterable.first(_) >> { it.last().onResult(null, null) }

        // Files Index check
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        if (clientSession != null){
            1 * filesCollection.listIndexes(clientSession) >> listIndexesIterable
        } else {
            1 * filesCollection.listIndexes() >> listIndexesIterable
        }
        1 * listIndexesIterable.into(_, _) >> { it.last().onResult([], null) }
        if (clientSession != null){
            1 * filesCollection.createIndex(clientSession, { index -> index == Document.parse('{"filename": 1, "uploadDate": 1 }') }, _) >>
                    { it.last().onResult('filename_1', null) }
        } else {
            1 * filesCollection.createIndex({ index -> index == Document.parse('{"filename": 1, "uploadDate": 1 }') }, _) >>
                    { it.last().onResult('filename_1', null) }
        }

        // Chunks Index check
        1 * chunksCollection.withReadPreference(primary()) >> chunksCollection
        if (clientSession != null){
            1 * chunksCollection.listIndexes(clientSession) >> listIndexesIterable
        } else {
            1 * chunksCollection.listIndexes() >> listIndexesIterable
        }
        1 * listIndexesIterable.into(_, _) >> { it.last().onResult([], null) }

        if (clientSession != null) {
            1 * chunksCollection.createIndex(clientSession, { index -> index == Document.parse('{"files_id": 1, "n": 1}') },
                    { indexOptions -> indexOptions.isUnique() }, _) >> { it.last().onResult('files_id_1', null) }
        } else {
            1 * chunksCollection.createIndex({ index -> index == Document.parse('{"files_id": 1, "n": 1}') },
                    { indexOptions -> indexOptions.isUnique() }, _) >> { it.last().onResult('files_id_1', null) }
        }

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should propagate errors if error when checking files collection'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def findIterable = Mock(FindIterable)
        def indexChecker = new GridFSIndexCheckImpl(clientSession, filesCollection, chunksCollection)
        def futureResult = new FutureResultCallback()

        when:
        indexChecker.checkAndCreateIndex(futureResult)

        then:
        1 * filesCollection.withDocumentClass(Document) >> filesCollection
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        if (clientSession != null){
            1 * filesCollection.find(clientSession) >> findIterable
        } else {
            1 * filesCollection.find() >> findIterable
        }
        1 * findIterable.projection(projection) >> findIterable
        1 * findIterable.first(_) >> { it.last().onResult(null, exception) }

        when:
        futureResult.get()

        then:
        def ex = thrown(MongoException)
        ex == exception

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should propagate errors if error when checking has files index'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def listIndexesIterable = Mock(ListIndexesIterable)
        def findIterable = Mock(FindIterable)
        def indexChecker = new GridFSIndexCheckImpl(clientSession, filesCollection, chunksCollection)
        def futureResult = new FutureResultCallback()

        when:
        indexChecker.checkAndCreateIndex(futureResult)

        then:
        1 * filesCollection.withDocumentClass(Document) >> filesCollection
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        if (clientSession != null){
            1 * filesCollection.find(clientSession) >> findIterable
        } else {
            1 * filesCollection.find() >> findIterable
        }
        1 * findIterable.projection(projection) >> findIterable
        1 * findIterable.first(_) >> { it.last().onResult(null, null) }

        // Files Index check
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        if (clientSession != null){
            1 * filesCollection.listIndexes(clientSession) >> listIndexesIterable
        } else {
            1 * filesCollection.listIndexes() >> listIndexesIterable
        }
        1 * listIndexesIterable.into(_, _) >> { it.last().onResult(null, exception) }

        when:
        futureResult.get()

        then:
        def ex = thrown(MongoException)
        ex == exception

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should propagate errors if error when creating files index'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def listIndexesIterable = Mock(ListIndexesIterable)
        def findIterable = Mock(FindIterable)
        def indexChecker = new GridFSIndexCheckImpl(clientSession, filesCollection, chunksCollection)
        def futureResult = new FutureResultCallback()

        when:
        indexChecker.checkAndCreateIndex(futureResult)

        then:
        1 * filesCollection.withDocumentClass(Document) >> filesCollection
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        if (clientSession != null){
            1 * filesCollection.find(clientSession) >> findIterable
        } else {
            1 * filesCollection.find() >> findIterable
        }
        1 * findIterable.projection(projection) >> findIterable
        1 * findIterable.first(_) >> { it.last().onResult(null, null) }

        // Files Index check
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        if (clientSession != null){
            1 * filesCollection.listIndexes(clientSession) >> listIndexesIterable
        } else {
            1 * filesCollection.listIndexes() >> listIndexesIterable
        }
        1 * listIndexesIterable.into(_, _) >> { it.last().onResult([], null) }
        if (clientSession != null){
            1 * filesCollection.createIndex(clientSession, { index -> index == Document.parse('{"filename": 1, "uploadDate": 1 }') }, _) >>
                    { it.last().onResult(null, exception) }
        } else {
            1 * filesCollection.createIndex({ index -> index == Document.parse('{"filename": 1, "uploadDate": 1 }') }, _) >>
                    { it.last().onResult(null, exception) }
        }

        when:
        futureResult.get()

        then:
        def ex = thrown(MongoException)
        ex == exception

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should propagate errors if error when checking has chunks index'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def listIndexesIterable = Mock(ListIndexesIterable)
        def findIterable = Mock(FindIterable)
        def indexChecker = new GridFSIndexCheckImpl(clientSession, filesCollection, chunksCollection)
        def futureResult = new FutureResultCallback()

        when:
        indexChecker.checkAndCreateIndex(futureResult)

        then:
        1 * filesCollection.withDocumentClass(Document) >> filesCollection
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        if (clientSession != null){
            1 * filesCollection.find(clientSession) >> findIterable
        } else {
            1 * filesCollection.find() >> findIterable
        }
        1 * findIterable.projection(projection) >> findIterable
        1 * findIterable.first(_) >> { it.last().onResult(null, null) }

        // Files Index check
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        if (clientSession != null){
            1 * filesCollection.listIndexes(clientSession) >> listIndexesIterable
        } else {
            1 * filesCollection.listIndexes() >> listIndexesIterable
        }
        1 * listIndexesIterable.into(_, _) >> { it.last().onResult([], null) }
        if (clientSession != null){
            1 * filesCollection.createIndex(clientSession, { index -> index == Document.parse('{"filename": 1, "uploadDate": 1 }') }, _) >>
                    { it.last().onResult('filename_1', null) }
        } else {
            1 * filesCollection.createIndex({ index -> index == Document.parse('{"filename": 1, "uploadDate": 1 }') }, _) >>
                    { it.last().onResult('filename_1', null) }
        }

        // Chunks Index check
        1 * chunksCollection.withReadPreference(primary()) >> chunksCollection
        if (clientSession != null){
            1 * chunksCollection.listIndexes(clientSession) >> listIndexesIterable
        } else {
            1 * chunksCollection.listIndexes() >> listIndexesIterable
        }
        1 * listIndexesIterable.into(_, _) >> { it.last().onResult([], exception) }

        when:
        futureResult.get()

        then:
        def ex = thrown(MongoException)
        ex == exception

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should propagate errors if error when creating chunks index'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def listIndexesIterable = Mock(ListIndexesIterable)
        def findIterable = Mock(FindIterable)
        def indexChecker = new GridFSIndexCheckImpl(clientSession, filesCollection, chunksCollection)
        def futureResult = new FutureResultCallback()

        when:
        indexChecker.checkAndCreateIndex(futureResult)

        then:
        1 * filesCollection.withDocumentClass(Document) >> filesCollection
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        if (clientSession != null){
            1 * filesCollection.find(clientSession) >> findIterable
        } else {
            1 * filesCollection.find() >> findIterable
        }
        1 * findIterable.projection(projection) >> findIterable
        1 * findIterable.first(_) >> { it.last().onResult(null, null) }

        // Files Index check
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        if (clientSession != null){
            1 * filesCollection.listIndexes(clientSession) >> listIndexesIterable
        } else {
            1 * filesCollection.listIndexes() >> listIndexesIterable
        }
        1 * listIndexesIterable.into(_, _) >> { it.last().onResult([], null) }
        if (clientSession != null){
            1 * filesCollection.createIndex(clientSession, { index -> index == Document.parse('{"filename": 1, "uploadDate": 1 }') }, _) >>
                    { it.last().onResult('filename_1', null) }
        } else {
            1 * filesCollection.createIndex({ index -> index == Document.parse('{"filename": 1, "uploadDate": 1 }') }, _) >>
                    { it.last().onResult('filename_1', null) }
        }


        // Chunks Index check
        1 * chunksCollection.withReadPreference(primary()) >> chunksCollection
        if (clientSession != null){
            1 * chunksCollection.listIndexes(clientSession) >> listIndexesIterable
        } else {
            1 * chunksCollection.listIndexes() >> listIndexesIterable
        }
        1 * listIndexesIterable.into(_, _) >> { it.last().onResult([], null) }
        if (clientSession != null){
            1 * chunksCollection.createIndex(clientSession, { index -> index == Document.parse('{"files_id": 1, "n": 1}') },
                    { indexOptions -> indexOptions.isUnique() }, _) >> { it.last().onResult(null, exception) }
        } else {
            1 * chunksCollection.createIndex({ index -> index == Document.parse('{"files_id": 1, "n": 1}') },
                    { indexOptions -> indexOptions.isUnique() }, _) >> { it.last().onResult(null, exception) }
        }

        when:
        futureResult.get()

        then:
        def ex = thrown(MongoException)
        ex == exception

        where:
        clientSession << [null, Stub(ClientSession)]
    }
}
