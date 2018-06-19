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

package com.mongodb.operation

import com.mongodb.MongoChangeStreamException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadConcern
import com.mongodb.WriteConcern
import com.mongodb.async.SingleResultCallback
import com.mongodb.binding.AsyncConnectionSource
import com.mongodb.binding.AsyncReadBinding
import com.mongodb.binding.ConnectionSource
import com.mongodb.binding.ReadBinding
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.model.changestream.ChangeStreamDocument
import com.mongodb.client.model.changestream.ChangeStreamLevel
import com.mongodb.client.model.changestream.FullDocument
import com.mongodb.client.model.changestream.OperationType
import com.mongodb.client.model.changestream.UpdateDescription
import com.mongodb.client.test.CollectionHelper
import com.mongodb.connection.AsyncConnection
import com.mongodb.connection.Connection
import com.mongodb.session.SessionContext
import org.bson.BsonArray
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.BsonTimestamp
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodec
import org.bson.codecs.ValueCodecProvider
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.getAsyncCluster
import static com.mongodb.ClusterFixture.getCluster
import static com.mongodb.ClusterFixture.isStandalone
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.internal.connection.ServerHelper.waitForLastRelease
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static org.bson.codecs.configuration.CodecRegistries.fromProviders

@IgnoreIf({ !(serverVersionAtLeast(3, 6) && !isStandalone()) })
class ChangeStreamOperationSpecification extends OperationFunctionalSpecification {

    def 'should have the correct defaults'() {
        when:
        ChangeStreamOperation operation = new ChangeStreamOperation<Document>(getNamespace(), FullDocument.DEFAULT, [], new DocumentCodec())

        then:
        operation.getBatchSize() == null
        operation.getCollation() == null
        operation.getFullDocument() == FullDocument.DEFAULT
        operation.getMaxAwaitTime(MILLISECONDS) == 0
        operation.getPipeline() == []
        operation.getStartAtOperationTime() == null
    }

    def 'should set optional values correctly'() {
        when:
        ChangeStreamOperation operation = new ChangeStreamOperation<Document>(getNamespace(), FullDocument.UPDATE_LOOKUP, [],
                new DocumentCodec())
                .batchSize(5)
                .collation(defaultCollation)
                .maxAwaitTime(15, MILLISECONDS)
                .startAtOperationTime(new BsonTimestamp(99))

        then:
        operation.getBatchSize() == 5
        operation.getCollation() == defaultCollation
        operation.getFullDocument() == FullDocument.UPDATE_LOOKUP
        operation.getMaxAwaitTime(MILLISECONDS) == 15
        operation.getStartAtOperationTime() == new BsonTimestamp(99)
    }

    def 'should create the expected command'() {
        given:
        def aggregate = changeStreamLevel == ChangeStreamLevel.COLLECTION ? new BsonString(namespace.getCollectionName()) : new BsonInt32(1)

        when:
        def pipeline = [BsonDocument.parse('{$match: {a: "A"}}')]
        def changeStream = BsonDocument.parse('''{$changeStream: {fullDocument: "default", startAtOperationTime:
                    { "$timestamp" : { "t" : 0, "i" : 0 }}}}''')
        if (changeStreamLevel == ChangeStreamLevel.CLIENT) {
            changeStream.getDocument('$changeStream').put('allChangesForCluster', BsonBoolean.TRUE)
        }


        def cursorResult = BsonDocument.parse('{ok: 1.0}')
                .append('cursor', new BsonDocument('id', new BsonInt64(0)).append('ns', new BsonString('db.coll'))
                .append('firstBatch', new BsonArrayWrapper([])))

        def operation = new ChangeStreamOperation<Document>(namespace, FullDocument.DEFAULT, pipeline, new DocumentCodec(),
                changeStreamLevel)
                .batchSize(5)
                .collation(defaultCollation)
                .maxAwaitTime(15, MILLISECONDS)
                .startAtOperationTime(new BsonTimestamp())

        def expectedCommand = new BsonDocument('aggregate', aggregate)
                .append('collation', defaultCollation.asDocument())
                .append('cursor', new BsonDocument('batchSize', new BsonInt32(5)))
                .append('pipeline', new BsonArray([changeStream, *pipeline]))
                .append('readConcern', new BsonDocument('level', new BsonString('majority')))

        then:
        testOperation(operation, [4, 0, 0], ReadConcern.MAJORITY, expectedCommand, async, cursorResult)

        where:
        [async, changeStreamLevel] << [[true, false],
                                       [ChangeStreamLevel.CLIENT, ChangeStreamLevel.DATABASE, ChangeStreamLevel.COLLECTION]].combinations()
    }

    def 'should return the expected results'() {
        given:
        def helper = getHelper()

        def pipeline = [BsonDocument.parse('{$match: {operationType: "insert"}}')]
        def operation = new ChangeStreamOperation<BsonDocument>(helper.getNamespace(), FullDocument.DEFAULT, pipeline, CODEC)

        when:
        def cursor = execute(operation, async)
        def expected = insertDocuments(helper, [1, 2])

        then:
        def next = nextAndClean(cursor, async)
        next == expected

        when:
        expected = insertDocuments(helper, [3, 4, 5, 6, 7])
        cursor.setBatchSize(5)

        then:
        cursor.getBatchSize() == 5
        nextAndClean(cursor, async) == expected

        then:
        if (async) {
            !cursor.isClosed()
        } else {
            cursor.getServerCursor() == cursor.getWrapped().getServerCursor()
            cursor.getServerAddress() == cursor.getWrapped().getServerAddress()
        }

        cleanup:
        cursor?.close()
        waitForLastRelease(async ? getAsyncCluster() : getCluster())

        where:
        async << [true, false]
    }

    def 'should decode insert to ChangeStreamDocument '() {
        given:
        def helper = getHelper()

        def pipeline = [BsonDocument.parse('{$match: {operationType: "insert"}}')]
        def operation = new ChangeStreamOperation<BsonDocument>(helper.getNamespace(), FullDocument.DEFAULT, pipeline,
                ChangeStreamDocument.createCodec(BsonDocument, fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())))

        when:
        def cursor = execute(operation, false)
        helper.insertDocuments(BsonDocument.parse('{ _id : 2, x : 2 }'))
        ChangeStreamDocument<BsonDocument> next = next(cursor, false).get(0)

        then:
        next.getResumeToken() != null
        next.getDocumentKey() == BsonDocument.parse('{ _id : 2 }')
        next.getFullDocument() == BsonDocument.parse('{ _id : 2, x : 2 }')
        next.getNamespace() == helper.getNamespace()
        next.getOperationType() == OperationType.INSERT
        next.getUpdateDescription() == null

        cleanup:
        cursor?.close()
        waitForLastRelease(getCluster())
    }

    def 'should decode update to ChangeStreamDocument '() {
        given:
        def helper = getHelper()

        def pipeline = [BsonDocument.parse('{$match: {operationType: "update"}}')]
        def operation = new ChangeStreamOperation<BsonDocument>(helper.getNamespace(), FullDocument.UPDATE_LOOKUP, pipeline,
                ChangeStreamDocument.createCodec(BsonDocument, fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())))
        helper.insertDocuments(BsonDocument.parse('{ _id : 2, x : 2, y : 3 }'))

        when:
        def cursor = execute(operation, false)
        helper.updateOne(BsonDocument.parse('{ _id : 2}'), BsonDocument.parse('{ $set : {x : 3}, $unset : {y : 1}}'))
        ChangeStreamDocument<BsonDocument> next = next(cursor, false).get(0)

        then:
        next.getResumeToken() != null
        next.getDocumentKey() == BsonDocument.parse('{ _id : 2 }')
        next.getFullDocument() == BsonDocument.parse('{ _id : 2, x : 3 }')
        next.getNamespace() == helper.getNamespace()
        next.getOperationType() == OperationType.UPDATE
        next.getUpdateDescription() == new UpdateDescription(['y'], BsonDocument.parse('{x : 3}'))

        cleanup:
        cursor?.close()
        waitForLastRelease(getCluster())
    }

    def 'should decode replace to ChangeStreamDocument '() {
        given:
        def helper = getHelper()

        def pipeline = [BsonDocument.parse('{$match: {operationType: "replace"}}')]
        def operation = new ChangeStreamOperation<BsonDocument>(helper.getNamespace(), FullDocument.UPDATE_LOOKUP, pipeline,
                ChangeStreamDocument.createCodec(BsonDocument, fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())))
        helper.insertDocuments(BsonDocument.parse('{ _id : 2, x : 2, y : 3 }'))

        when:
        def cursor = execute(operation, false)
        helper.replaceOne(BsonDocument.parse('{ _id : 2}'), BsonDocument.parse('{ _id : 2, x : 3}'), false)
        ChangeStreamDocument<BsonDocument> next = next(cursor, false).get(0)

        then:
        next.getResumeToken() != null
        next.getDocumentKey() == BsonDocument.parse('{ _id : 2 }')
        next.getFullDocument() == BsonDocument.parse('{ _id : 2, x : 3 }')
        next.getNamespace() == helper.getNamespace()
        next.getOperationType() == OperationType.REPLACE
        next.getUpdateDescription() == null

        cleanup:
        cursor?.close()
        waitForLastRelease(getCluster())
    }

    def 'should decode delete to ChangeStreamDocument '() {
        given:
        def helper = getHelper()

        def pipeline = [BsonDocument.parse('{$match: {operationType: "delete"}}')]
        def operation = new ChangeStreamOperation<BsonDocument>(helper.getNamespace(), FullDocument.UPDATE_LOOKUP, pipeline,
                ChangeStreamDocument.createCodec(BsonDocument, fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())))
        helper.insertDocuments(BsonDocument.parse('{ _id : 2, x : 2, y : 3 }'))

        when:
        def cursor = execute(operation, false)
        helper.deleteOne(BsonDocument.parse('{ _id : 2}'))
        ChangeStreamDocument<BsonDocument> next = next(cursor, false).get(0)

        then:
        next.getResumeToken() != null
        next.getDocumentKey() == BsonDocument.parse('{ _id : 2 }')
        next.getFullDocument() == null
        next.getNamespace() == helper.getNamespace()
        next.getOperationType() == OperationType.DELETE
        next.getUpdateDescription() == null

        cleanup:
        cursor?.close()
        waitForLastRelease(getCluster())
    }

    def 'should decode invalidate to ChangeStreamDocument '() {
        given:
        def helper = getHelper()

        def pipeline = [BsonDocument.parse('{$match: {operationType: "invalidate"}}')]
        def operation = new ChangeStreamOperation<BsonDocument>(helper.getNamespace(), FullDocument.UPDATE_LOOKUP, pipeline,
                ChangeStreamDocument.createCodec(BsonDocument, fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())))
        helper.insertDocuments(BsonDocument.parse('{ _id : 2, x : 2, y : 3 }'))

        when:
        def cursor = execute(operation, false)
        helper.drop()
        ChangeStreamDocument<BsonDocument> next = next(cursor, false).get(0)

        then:
        next.getResumeToken() != null
        next.getDocumentKey() == null
        next.getFullDocument() == null
        next.getNamespace() == null
        next.getOperationType() == OperationType.INVALIDATE
        next.getUpdateDescription() == null

        cleanup:
        cursor?.close()
        waitForLastRelease(getCluster())
    }

    def 'should throw if the _id field is projected out'() {
        given:
        def helper = getHelper()
        def pipeline = [BsonDocument.parse('{$project: {"_id": 0}}')]
        def operation = new ChangeStreamOperation<BsonDocument>(helper.getNamespace(), FullDocument.DEFAULT, pipeline, CODEC)

        when:
        def cursor = execute(operation, async)
        insertDocuments(helper, [11, 22])
        nextAndClean(cursor, async)

        then:
        thrown(MongoChangeStreamException)

        cleanup:
        cursor?.close()
        waitForLastRelease(async ? getAsyncCluster() : getCluster())

        where:
        async << [true, false]
    }

    def 'should act like a tailable cursor'() {
        given:
        def helper = getHelper()

        def pipeline = [BsonDocument.parse('{$match: {operationType: "insert"}}')]
        def operation = new ChangeStreamOperation<BsonDocument>(helper.getNamespace(), FullDocument.DEFAULT, pipeline, CODEC)

        when:
        def cursor = execute(operation, async)

        then:
        tryNextAndClean(cursor, async) == null

        when:
        def expected = insertDocuments(helper, [1, 2])

        then:
        nextAndClean(cursor, async) == expected

        then:
        tryNextAndClean(cursor, async) == null

        when:
        expected = insertDocuments(helper, [3, 4])

        then:
        nextAndClean(cursor, async) == expected

        cleanup:
        cursor?.close()
        waitForLastRelease(async ? getAsyncCluster() : getCluster())

        where:
        async << [true, false]
    }

    def 'should be resumable'() {
        given:
        def helper = getHelper()

        def pipeline = [BsonDocument.parse('{$match: {operationType: "insert"}}')]
        def operation = new ChangeStreamOperation<BsonDocument>(helper.getNamespace(), FullDocument.DEFAULT, pipeline, CODEC)

        when:
        def cursor = execute(operation, async)
        def expected = insertDocuments(helper, [1, 2])

        then:
        nextAndClean(cursor, async) == expected

        when:
        helper.killCursor(helper.getNamespace(), cursor.getWrapped().getServerCursor())
        expected = insertDocuments(helper, [3, 4])

        then:
        nextAndClean(cursor, async) == expected

        then:
        tryNextAndClean(cursor, async) == null

        when:
        expected = insertDocuments(helper, [5, 6])
        helper.killCursor(helper.getNamespace(), cursor.getWrapped().getServerCursor())

        then:
        nextAndClean(cursor, async) == expected

        cleanup:
        cursor?.close()
        waitForLastRelease(async ? getAsyncCluster() : getCluster())

        where:
        async << [true, false]
    }

    def 'should work with a resumeToken'() {
        given:
        def helper = getHelper()

        def pipeline = [BsonDocument.parse('{$match: {operationType: "insert"}}')]
        def operation = new ChangeStreamOperation<BsonDocument>(helper.getNamespace(), FullDocument.DEFAULT, pipeline, CODEC)

        def cursor = execute(operation, async)

        when:
        def expected = insertDocuments(helper, [1, 2])
        def result = next(cursor, async)

        then:
        result.size() == 2

        when:
        cursor.close()
        waitForLastRelease(async ? getAsyncCluster() : getCluster())

        operation.startAtOperationTime(null).resumeAfter(result.head().getDocument('_id'))
        cursor = execute(operation, async)
        result = nextAndClean(cursor, async)

        then:
        result == expected.tail()

        cleanup:
        cursor?.close()
        waitForLastRelease(async ? getAsyncCluster() : getCluster())

        where:
        async << [true, false]
    }

    def 'should support hasNext on the sync API'() {
        given:
        def helper = getHelper()
        def operation = new ChangeStreamOperation<BsonDocument>(helper.getNamespace(), FullDocument.DEFAULT, [], CODEC)

        when:
        def cursor = execute(operation, false)
        insertDocuments(helper, [1])

        then:
        cursor.hasNext()

        cleanup:
        cursor?.close()
        waitForLastRelease(getCluster())
    }

    def 'should set the startAtOperationTime on the sync cursor'() {
        given:
        def binding = Stub(ReadBinding) {
            getSessionContext() >> Stub(SessionContext) {
                getReadConcern() >> ReadConcern.DEFAULT
                getOperationTime() >> new BsonTimestamp()
            }
            getReadConnectionSource() >> Stub(ConnectionSource) {
                getConnection() >> Stub(Connection) {
                    command(*_) >>  new BsonDocument('cursor', new BsonDocument('id', new BsonInt64(1))
                            .append('ns', new BsonString(getNamespace().getFullName()))
                            .append('firstBatch', new BsonArrayWrapper([])))
                }
            }
        }

        when: 'Resume token'
        def operation = new ChangeStreamOperation<BsonDocument>(helper.getNamespace(), FullDocument.DEFAULT, [], CODEC)
                .resumeAfter(new BsonDocument())
        operation.execute(binding)

        then:
        operation.getStartAtOperationTime() == null

        when: 'No token or time'
        operation = new ChangeStreamOperation<BsonDocument>(helper.getNamespace(), FullDocument.DEFAULT, [], CODEC)
        operation.execute(binding)

        then:
        operation.getStartAtOperationTime() == new BsonTimestamp()

        when: 'Set time'
        def startAtTime = new BsonTimestamp(42)
        operation = operation.startAtOperationTime(startAtTime)
        operation.execute(binding)

        then:
        operation.getStartAtOperationTime() == startAtTime
    }

    def 'should set the startAtOperationTime on the async cursor'() {
        given:
        def binding = Stub(AsyncReadBinding) {
            getSessionContext() >> Stub(SessionContext) {
                getReadConcern() >> ReadConcern.DEFAULT
                getOperationTime() >> new BsonTimestamp()
            }
            getReadConnectionSource(_) >> {
                it.last().onResult(Stub(AsyncConnectionSource) {
                    getConnection(_) >> {
                        it.last().onResult(Stub(AsyncConnection) {
                            commandAsync(*_) >> {
                                it.last().onResult(new BsonDocument('cursor', new BsonDocument('id', new BsonInt64(1))
                                        .append('ns', new BsonString(getNamespace().getFullName()))
                                        .append('firstBatch', new BsonArrayWrapper([]))), null)
                            }
                        }, null)
                    }
                }, null)
            }
        }

        when: 'Resume Token'
        def operation = new ChangeStreamOperation<BsonDocument>(helper.getNamespace(), FullDocument.DEFAULT, [], CODEC)
                .resumeAfter(new BsonDocument())

        operation.executeAsync(binding, Stub(SingleResultCallback))

        then:
        operation.getStartAtOperationTime() == null

        when: 'No token or time'
        operation = new ChangeStreamOperation<BsonDocument>(helper.getNamespace(), FullDocument.DEFAULT, [], CODEC)
        operation.executeAsync(binding, Stub(SingleResultCallback))

        then:
        operation.getStartAtOperationTime() == new BsonTimestamp()

        when: 'set time'
        def startAtTime = new BsonTimestamp(42)
        operation = operation.startAtOperationTime(startAtTime)
        operation.executeAsync(binding, Stub(SingleResultCallback))

        then:
        operation.getStartAtOperationTime() == startAtTime
    }

    private final static CODEC = new BsonDocumentCodec()

    private CollectionHelper<Document> getHelper() {
        def helper = getCollectionHelper()
        helper.create(helper.getNamespace().getCollectionName(), new CreateCollectionOptions())
        helper
    }

    private static List<BsonDocument> insertDocuments(final CollectionHelper<?> helper, final List<Integer> docs) {
        helper.insertDocuments(docs.collect { BsonDocument.parse("{_id: $it, a: $it}") }, WriteConcern.MAJORITY)
        docs.collect {
            BsonDocument.parse("""{
                "operationType": "insert",
                "fullDocument": {"_id": $it, "a": $it},
                "ns": {"db": "${helper.getNamespace().getDatabaseName()}", "coll": "${helper.getNamespace().getCollectionName()}"},
                "documentKey": {"_id": $it}
            }""")
        }
    }

    def tryNextAndClean(cursor, boolean async) {
        removeExtra(tryNext(cursor, async))
    }

    def nextAndClean(cursor, boolean async) {
        removeExtra(next(cursor, async))
    }

    def removeExtra(List<BsonDocument> next) {
        next?.collect { doc ->
            doc.remove('_id')
            doc.remove('clusterTime')
            doc
        }
    }

}
