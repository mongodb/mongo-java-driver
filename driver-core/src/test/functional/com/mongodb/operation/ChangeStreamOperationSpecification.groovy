/*
 * Copyright 2017 MongoDB, Inc.
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

package com.mongodb.operation

import com.mongodb.MongoChangeStreamException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadConcern
import com.mongodb.WriteConcern
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.model.changestream.FullDocument
import com.mongodb.client.test.CollectionHelper
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.DocumentCodec
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static java.util.concurrent.TimeUnit.MILLISECONDS

@IgnoreIf({ !(serverVersionAtLeast(3, 5) && isDiscoverableReplicaSet()) })
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
    }

    def 'should set optional values correctly'() {
        when:
        ChangeStreamOperation operation = new ChangeStreamOperation<Document>(getNamespace(), FullDocument.UPDATE_LOOKUP, [],
                new DocumentCodec())
                .batchSize(5)
                .collation(defaultCollation)
                .maxAwaitTime(15, MILLISECONDS)

        then:
        operation.getBatchSize() == 5
        operation.getCollation() == defaultCollation
        operation.getFullDocument() == FullDocument.UPDATE_LOOKUP
        operation.getMaxAwaitTime(MILLISECONDS) == 15
    }

    def 'should create the expected command'() {
        when:
        def pipeline = [BsonDocument.parse('{$match: {a: "A"}}')]
        def changeStream = BsonDocument.parse('{$changeStream: {fullDocument: "default"}}')

        def cursorResult = BsonDocument.parse('{ok: 1.0}')
                .append('cursor', new BsonDocument('id', new BsonInt64(0)).append('ns', new BsonString('db.coll'))
                .append('firstBatch', new BsonArrayWrapper([])))

        def operation = new ChangeStreamOperation<Document>(namespace, FullDocument.DEFAULT, pipeline, new DocumentCodec())
                .batchSize(5)
                .collation(defaultCollation)
                .maxAwaitTime(15, MILLISECONDS)
                .readConcern(ReadConcern.MAJORITY)

        def expectedCommand = new BsonDocument('aggregate', new BsonString(namespace.getCollectionName()))
                .append('collation', defaultCollation.asDocument())
                .append('cursor', new BsonDocument('batchSize', new BsonInt32(5)))
                .append('pipeline', new BsonArray([changeStream, *pipeline]))
                .append('readConcern', new BsonDocument('level', new BsonString('majority')))

            then:
        testOperation(operation, [3, 6, 0], expectedCommand, async, cursorResult)

        where:
        async << [true, false]
    }

    def 'should support return the expected results'() {
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

        where:
        async << [true, false]
    }

    def 'should throw if the _id field is projected out'() {
        given:
        def helper = getHelper()
        def pipeline = [BsonDocument.parse('{$project: {"_id": 0}}')]
        def operation = new ChangeStreamOperation<BsonDocument>(helper.getNamespace(), FullDocument.DEFAULT, pipeline, CODEC)

        when:
        def cursor = execute(operation, async)
        insertDocuments(helper, [1, 2])
        nextAndClean(cursor, async)

        then:
        thrown(MongoChangeStreamException)

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
        helper?.drop()

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
        helper?.drop()

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

        operation.resumeAfter(result.head().getDocument('_id'))
        cursor = execute(operation, async)
        result = tryNextAndClean(cursor, async)

        then:
        result == expected.tail()

        cleanup:
        cursor?.close()
        helper?.drop()

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
                "_id": {
                    "documentKey": {"_id": $it}
                },
                "operationType": "insert",
                "fullDocument": {"_id": $it, "a": $it},
                "ns": {"coll": "${helper.getNamespace().getCollectionName()}", "db": "${helper.getNamespace().getDatabaseName()}"},
                "documentKey": {"_id": $it}
            }""")
        }
    }

    def tryNextAndClean(cursor, boolean async) {
        removeTimestampAndUUID(tryNext(cursor, async))
    }

    def nextAndClean(cursor, boolean async) {
        removeTimestampAndUUID(next(cursor, async))
    }

    def removeTimestampAndUUID(List<BsonDocument> next) {
        next?.collect { doc ->
            doc.getDocument('_id').remove('clusterTime')
            doc.getDocument('_id').remove('uuid')
            doc
        }
    }

}
