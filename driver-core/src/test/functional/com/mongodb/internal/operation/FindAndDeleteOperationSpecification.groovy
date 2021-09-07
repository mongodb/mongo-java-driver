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

import com.mongodb.MongoSocketException
import com.mongodb.MongoWriteConcernException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ServerAddress
import com.mongodb.WriteConcern
import com.mongodb.client.test.CollectionHelper
import com.mongodb.client.test.Worker
import com.mongodb.client.test.WorkerCodec
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonDocumentWrapper
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.DocumentCodec
import spock.lang.IgnoreIf

import java.util.concurrent.TimeUnit

import static com.mongodb.ClusterFixture.configureFailPoint
import static com.mongodb.ClusterFixture.disableFailPoint
import static com.mongodb.ClusterFixture.disableOnPrimaryTransactionalWriteFailPoint
import static com.mongodb.ClusterFixture.enableOnPrimaryTransactionalWriteFailPoint
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.serverVersionLessThan
import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static com.mongodb.WriteConcern.UNACKNOWLEDGED
import static com.mongodb.WriteConcern.W1
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY
import static com.mongodb.connection.ServerType.STANDALONE

class FindAndDeleteOperationSpecification extends OperationFunctionalSpecification {
    private final DocumentCodec documentCodec = new DocumentCodec()
    private final WorkerCodec workerCodec = new WorkerCodec()

    def 'should have the correct defaults'() {
        when:
        def operation = new FindAndDeleteOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec)

        then:
        operation.getNamespace() == getNamespace()
        operation.getWriteConcern() == ACKNOWLEDGED
        operation.getDecoder() == documentCodec
        operation.getFilter() == null
        operation.getSort() == null
        operation.getProjection() == null
        operation.getMaxTime(TimeUnit.MILLISECONDS) == 0
        operation.getCollation() == null
    }

    def 'should set optional values correctly'(){
        given:
        def filter = BsonDocument.parse('{ filter : 1}')
        def sort = BsonDocument.parse('{ sort : 1}')
        def projection = BsonDocument.parse('{ projection : 1}')

        when:
        def operation = new FindAndDeleteOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec)
            .filter(filter)
            .sort(sort)
            .projection(projection)
            .maxTime(10, TimeUnit.MILLISECONDS)
            .collation(defaultCollation)

        then:
        operation.getFilter() == filter
        operation.getSort() == sort
        operation.getProjection() == projection
        operation.getMaxTime(TimeUnit.MILLISECONDS) == 10
        operation.getCollation() == defaultCollation
    }

    def 'should remove single document'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')

        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam)

        when:
        def operation = new FindAndDeleteOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Document returnedDocument = execute(operation, async)

        then:
        getCollectionHelper().find().size() == 1;
        getCollectionHelper().find().first().getString('name') == 'Sam'
        returnedDocument.getString('name') == 'Pete'

        where:
        async << [true, false]
    }


    def 'should remove single document when using custom codecs'() {
        given:
        Worker pete = new Worker('Pete', 'handyman', new Date(), 3)
        Worker sam = new Worker('Sam', 'plumber', new Date(), 7)
        getWorkerCollectionHelper().insertDocuments(new WorkerCodec(), pete, sam)

        when:
        FindAndDeleteOperation<Worker> operation = new FindAndDeleteOperation<Worker>(getNamespace(), ACKNOWLEDGED, false,
                workerCodec).filter(new BsonDocument('name', new BsonString('Pete')))
        Worker returnedDocument = execute(operation, async)

        then:
        getWorkerCollectionHelper().find().size() == 1;
        getWorkerCollectionHelper().find().first() == sam
        returnedDocument == pete

        where:
        async << [true, false]
    }


    @IgnoreIf({ serverVersionLessThan(3, 2) || !isDiscoverableReplicaSet() })
    def 'should throw on write concern error'() {
        given:
        CollectionHelper<Document> helper = new CollectionHelper<Document>(documentCodec, getNamespace())
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        helper.insertDocuments(new DocumentCodec(), pete)
        def operation = new FindAndDeleteOperation<Document>(getNamespace(), new WriteConcern(5, 1), false,
                documentCodec).filter(new BsonDocument('name', new BsonString('Pete')))

        when:
        execute(operation, async)

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 100
        !ex.writeConcernError.message.isEmpty()
        ex.writeResult.count == 1
        !ex.writeResult.updateOfExisting
        ex.writeResult.upsertedId == null

        where:
        async << [true, false]
    }

    @IgnoreIf({ serverVersionLessThan(3, 8) || !isDiscoverableReplicaSet() })
    def 'should throw on write concern error on multiple failpoint'() {
        given:
        CollectionHelper<Document> helper = new CollectionHelper<Document>(documentCodec, getNamespace())
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        helper.insertDocuments(new DocumentCodec(), pete)

        def failPoint = BsonDocument.parse('''{
            "configureFailPoint": "failCommand",
            "mode": {"times": 2 },
            "data": { "failCommands": ["findAndModify"],
                      "writeConcernError": {"code": 91, "errmsg": "Replication is being shut down"}}}''')
        configureFailPoint(failPoint)

        def operation = new FindAndDeleteOperation<Document>(getNamespace(), ACKNOWLEDGED, false,
                documentCodec).filter(new BsonDocument('name', new BsonString('Pete')))

        when:
        execute(operation, async)

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 91
        !ex.writeConcernError.message.isEmpty()
        ex.writeResult.count == 1
        !ex.writeResult.updateOfExisting
        ex.writeResult.upsertedId == null

        cleanup:
        disableFailPoint('failCommand')

        where:
        async << [true, false]
    }

    def 'should create the expected command'() {
        when:
        def includeCollation = serverVersionIsGreaterThan(serverVersion, [3, 4, 0])
        def includeTxnNumber = (serverVersionIsGreaterThan(serverVersion, [3, 6, 0]) && retryWrites
                && writeConcern.isAcknowledged() && serverType != STANDALONE)
        def includeWriteConcern = (writeConcern.isAcknowledged() && !writeConcern.isServerDefault()
                && serverVersionIsGreaterThan(serverVersion, [3, 4, 0]))
        def cannedResult = new BsonDocument('value', new BsonDocumentWrapper(BsonDocument.parse('{}'), new BsonDocumentCodec()))
        def operation = new FindAndDeleteOperation<Document>(getNamespace(), writeConcern as WriteConcern,
                retryWrites as boolean, documentCodec)
        def expectedCommand = new BsonDocument('findAndModify', new BsonString(getNamespace().getCollectionName()))
                .append('remove', BsonBoolean.TRUE)

        if (includeWriteConcern) {
            expectedCommand.put('writeConcern', writeConcern.asDocument())
        }
        if (includeTxnNumber) {
            expectedCommand.put('txnNumber', new BsonInt64(0))
        }

        then:
        testOperation([operation: operation, serverVersion: serverVersion, expectedCommand: expectedCommand, async: async,
                       result: cannedResult, serverType: serverType])

        when:
        def filter = BsonDocument.parse('{ filter : 1}')
        def sort = BsonDocument.parse('{ sort : 1}')
        def projection = BsonDocument.parse('{ projection : 1}')

        operation.filter(filter)
                .sort(sort)
                .projection(projection)
                .maxTime(10, TimeUnit.MILLISECONDS)

        expectedCommand.append('query', filter)
                .append('sort', sort)
                .append('fields', projection)
                .append('maxTimeMS', new BsonInt64(10))

        if (includeCollation) {
            operation.collation(defaultCollation)
            expectedCommand.append('collation', defaultCollation.asDocument())
        }

        then:
        testOperation([operation: operation, serverVersion: serverVersion, expectedCommand: expectedCommand, async: async,
                       result: cannedResult, serverType: serverType])

        where:
        [serverVersion, serverType, writeConcern, async, retryWrites] << [
                [[3, 6, 0], [3, 4, 0], [3, 0, 0]],
                [REPLICA_SET_PRIMARY, STANDALONE],
                [ACKNOWLEDGED, W1, UNACKNOWLEDGED],
                [true, false],
                [true, false]
        ].combinations()
    }

    @IgnoreIf({ serverVersionLessThan(3, 6) || !isDiscoverableReplicaSet() })
    def 'should support retryable writes'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')

        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam)

        when:
        def operation = new FindAndDeleteOperation<Document>(getNamespace(), ACKNOWLEDGED, true, documentCodec)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        enableOnPrimaryTransactionalWriteFailPoint(BsonDocument.parse('{times: 1}'))

        Document returnedDocument = executeWithSession(operation, async)

        then:
        getCollectionHelper().find().size() == 1
        getCollectionHelper().find().first().getString('name') == 'Sam'
        returnedDocument.getString('name') == 'Pete'

        cleanup:
        disableOnPrimaryTransactionalWriteFailPoint()

        where:
        async << [true, false]
    }

    def 'should retry if the connection initially fails'() {
        when:
        def cannedResult = new BsonDocument('value', new BsonDocumentWrapper(BsonDocument.parse('{}'), new BsonDocumentCodec()))
        def operation = new FindAndDeleteOperation<Document>(getNamespace(), ACKNOWLEDGED, true, documentCodec)
        def expectedCommand = new BsonDocument('findAndModify', new BsonString(getNamespace().getCollectionName()))
                .append('remove', BsonBoolean.TRUE)
                .append('txnNumber', new BsonInt64(0))

        then:
        testOperationRetries(operation, [3, 6, 0], expectedCommand, async, cannedResult)

        where:
        async << [true, false]
    }

    def 'should throw original error when retrying and failing'() {
        given:
        def operation = new FindAndDeleteOperation<Document>(getNamespace(), ACKNOWLEDGED, true, documentCodec)
        def originalException = new MongoSocketException('Some failure', new ServerAddress())

        when:
        testRetryableOperationThrowsOriginalError(operation, [[3, 6, 0], [3, 6, 0], [3, 4, 0], [3, 4, 0]],
                [REPLICA_SET_PRIMARY, REPLICA_SET_PRIMARY], originalException, async)

        then:
        Exception commandException = thrown()
        commandException == originalException

        when:
        testRetryableOperationThrowsOriginalError(operation, [[3, 6, 0], [3, 6, 0], [3, 6, 0], [3, 6, 0]],
                [REPLICA_SET_PRIMARY, STANDALONE], originalException, async)

        then:
        commandException = thrown()
        commandException == originalException

        when:
        testRetryableOperationThrowsOriginalError(operation, [[3, 6, 0], [3, 6, 0]],
                [REPLICA_SET_PRIMARY], originalException, async, 1)

        then:
        commandException = thrown()
        commandException == originalException

        where:
        async << [true, false]
    }

    def 'should throw an exception when passing an unsupported collation'() {
        given:
        def operation = new FindAndDeleteOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec).collation(defaultCollation)

        when:
        testOperationThrows(operation, [3, 2, 0], async)

        then:
        def exception = thrown(IllegalArgumentException)
        exception.getMessage().startsWith('Collation not supported by wire version:')

        where:
        async << [true, false]
    }

    @IgnoreIf({ serverVersionLessThan(3, 4) })
    def 'should support collation'() {
        given:
        def document = Document.parse('{_id: 1, str: "foo"}')
        getCollectionHelper().insertDocuments(document)
        def operation = new FindAndDeleteOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec)
                .filter(BsonDocument.parse('{str: "FOO"}'))
                .collation(caseInsensitiveCollation)

        when:
        def result = execute(operation, async)

        then:
        result == document

        where:
        async << [true, false]
    }
}
