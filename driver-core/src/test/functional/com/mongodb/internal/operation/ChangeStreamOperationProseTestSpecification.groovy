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

package com.mongodb.internal.operation

import com.mongodb.MongoChangeStreamException
import com.mongodb.MongoException
import com.mongodb.MongoQueryException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.WriteConcern
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.model.changestream.FullDocument
import com.mongodb.client.test.CollectionHelper
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.getAsyncCluster
import static com.mongodb.ClusterFixture.getCluster
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.isStandalone
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.internal.connection.ServerHelper.waitForLastRelease
import static java.util.Arrays.asList

// See https://github.com/mongodb/specifications/tree/master/source/change-streams/tests/README.rst#prose-tests
@IgnoreIf({ !(serverVersionAtLeast(3, 6) && !isStandalone()) })
class ChangeStreamOperationProseTestSpecification extends OperationFunctionalSpecification {

    //
    // Test that the ChangeStream will throw an exception if the server response is missing the resume token (if wire version is < 8).
    //
    def 'should throw if the _id field is projected out'() {
        given:
        def helper = getHelper()
        def pipeline = [BsonDocument.parse('{$project: {"_id": 0}}')]
        def operation = new ChangeStreamOperation<BsonDocument>(helper.getNamespace(), FullDocument.DEFAULT, pipeline, CODEC)

        when:
        def cursor = execute(operation, async)
        insertDocuments(helper, [11, 22])
        next(cursor, async)

        then:
        def exception = thrown(MongoException)

        then:
        if (serverVersionAtLeast(4, 1)) {
            exception instanceof MongoQueryException
        } else {
            exception instanceof MongoChangeStreamException
        }

        cleanup:
        cursor?.close()
        waitForLastRelease(async ? getAsyncCluster() : getCluster())

        where:
        async << [true, false]
    }

    //
    // Test that the ChangeStream will automatically resume one time on a resumable error (including not master)
    // with the initial pipeline and options, except for the addition/update of a resumeToken.
    //
    @IgnoreIf({ !serverVersionAtLeast([4, 0, 0]) && !isDiscoverableReplicaSet() })
    def 'should resume after single getMore Error'() {
        given:
        def helper = getHelper()

        def pipeline = [BsonDocument.parse('{$match: {operationType: "insert"}}')]
        def failPointDocument = createFailPointDocument('getMore', 10107)
        def operation = new ChangeStreamOperation<BsonDocument>(helper.getNamespace(), FullDocument.DEFAULT, pipeline, CODEC)

        def cursor = execute(operation, async)

        when:
        insertDocuments(helper, [1, 2])
        setFailPoint(failPointDocument)

        then:
        def result = next(cursor, async, 2)

        then:
        result.size() == 2

        cleanup:
        cursor?.close()
        disableFailPoint(failPointDocument)
        waitForLastRelease(async ? getAsyncCluster() : getCluster())

        where:
        async << [true, false]
    }

    //
    // Test that ChangeStream will not attempt to resume on any error encountered while executing an aggregate command.
    //
    def 'should not resume for aggregation errors'() {
        given:
        def pipeline = [BsonDocument.parse('{$unsupportedStage: {_id: 0}}')]
        def operation = new ChangeStreamOperation<BsonDocument>(helper.getNamespace(), FullDocument.DEFAULT, pipeline, CODEC)

        when:
        def cursor = execute(operation, async)

        then:
        thrown(MongoException)

        cleanup:
        cursor?.close()
        waitForLastRelease(async ? getAsyncCluster() : getCluster())

        where:
        async << [true, false]
    }


    private final static CODEC = new BsonDocumentCodec()

    private CollectionHelper<Document> getHelper() {
        def helper = getCollectionHelper()
        helper.create(helper.getNamespace().getCollectionName(), new CreateCollectionOptions())
        helper
    }

    private static void insertDocuments(final CollectionHelper<?> helper, final List<Integer> docs) {
        helper.insertDocuments(docs.collect { BsonDocument.parse("{_id: $it, a: $it}") }, WriteConcern.MAJORITY)
    }

    private static BsonDocument createFailPointDocument(final String command, final int errCode) {
        new BsonDocument('configureFailPoint', new BsonString('failCommand'))
                .append('mode', new BsonDocument('times', new BsonInt32(1)))
                .append('data', new BsonDocument('failCommands', new BsonArray(asList(new BsonString(command))))
                        .append('errorCode', new BsonInt32(errCode))
                        .append('errorLabels', new BsonArray(asList(new BsonString('ResumableChangeStreamError')))))
    }

    def setFailPoint(final BsonDocument failPointDocument) {
        collectionHelper.runAdminCommand(failPointDocument)
    }

    def disableFailPoint(final BsonDocument failPointDocument) {
        collectionHelper.runAdminCommand(failPointDocument.append('mode', new BsonString('off')))
    }
}
