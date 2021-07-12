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

import com.mongodb.MongoCommandException
import com.mongodb.MongoNamespace
import com.mongodb.MongoSocketException
import com.mongodb.MongoWriteConcernException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ServerAddress
import com.mongodb.WriteConcern
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.model.ValidationOptions
import com.mongodb.client.test.CollectionHelper
import com.mongodb.client.test.Worker
import com.mongodb.client.test.WorkerCodec
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonDocumentWrapper
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonObjectId
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
import static com.mongodb.client.model.Filters.gte
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY
import static com.mongodb.connection.ServerType.STANDALONE

class FindAndReplaceOperationSpecification extends OperationFunctionalSpecification {
    private final DocumentCodec documentCodec = new DocumentCodec()
    private final WorkerCodec workerCodec = new WorkerCodec()

    def 'should have the correct defaults and passed values'() {
        when:
        def replacement = new BsonDocument('replace', new BsonInt32(1))
        def operation = new FindAndReplaceOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec, replacement)

        then:
        operation.getNamespace() == getNamespace()
        operation.getWriteConcern() == ACKNOWLEDGED
        operation.getDecoder() == documentCodec
        operation.getReplacement() == replacement
        operation.getFilter() == null
        operation.getSort() == null
        operation.getProjection() == null
        operation.getMaxTime(TimeUnit.SECONDS) == 0
        operation.getBypassDocumentValidation() == null
        operation.getCollation() == null
    }

    def 'should set optional values correctly'() {
        given:
        def filter = new BsonDocument('filter', new BsonInt32(1))
        def sort = new BsonDocument('sort', new BsonInt32(1))
        def projection = new BsonDocument('projection', new BsonInt32(1))

        when:
        def operation = new FindAndReplaceOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec,
                new BsonDocument('replace', new BsonInt32(1))).filter(filter).sort(sort).projection(projection)
                .bypassDocumentValidation(true).maxTime(1, TimeUnit.SECONDS).upsert(true).returnOriginal(false)
                .collation(defaultCollation)

        then:
        operation.getFilter() == filter
        operation.getSort() == sort
        operation.getProjection() == projection
        operation.upsert == true
        operation.getMaxTime(TimeUnit.SECONDS) == 1
        operation.getBypassDocumentValidation()
        !operation.isReturnOriginal()
        operation.getCollation() == defaultCollation
    }

    def 'should replace single document'() {
        given:
        CollectionHelper<Document> helper = new CollectionHelper<Document>(documentCodec, getNamespace())
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        BsonDocument jordan = BsonDocument.parse('{name: "Jordan", job: "sparky"}')

        helper.insertDocuments(new DocumentCodec(), pete, sam)

        when:
        def operation = new FindAndReplaceOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec, jordan)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Document returnedDocument = execute(operation, async)

        then:
        returnedDocument.getString('name') == 'Pete'
        helper.find().size() == 2
        helper.find().get(0).getString('name') == 'Jordan'

        when:
        operation = new FindAndReplaceOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec,
                new BsonDocumentWrapper<Document>(pete, documentCodec))
                .filter(new BsonDocument('name', new BsonString('Jordan')))
                .returnOriginal(false)
        returnedDocument = execute(operation, async)

        then:
        returnedDocument.getString('name') == 'Pete'

        where:
        async << [true, false]
    }

    def 'should replace single document when using custom codecs'() {
        given:
        CollectionHelper<Worker> helper = new CollectionHelper<Worker>(workerCodec, getNamespace())
        Worker pete = new Worker('Pete', 'handyman', new Date(), 3)
        Worker sam = new Worker('Sam', 'plumber', new Date(), 5)
        Worker jordan = new Worker(pete.id, 'Jordan', 'sparky', new Date(), 7)
        BsonDocument replacement = new BsonDocumentWrapper<Worker>(jordan, workerCodec)

        helper.insertDocuments(new WorkerCodec(), pete, sam)

        when:
        def operation = new FindAndReplaceOperation<Worker>(getNamespace(), ACKNOWLEDGED, false, workerCodec,
                replacement).filter(new BsonDocument('name', new BsonString('Pete')))
        Worker returnedDocument = execute(operation, async)

        then:
        returnedDocument == pete
        helper.find().get(0) == jordan

        when:
        replacement = new BsonDocumentWrapper<Worker>(pete, workerCodec)
        operation = new FindAndReplaceOperation<Worker>(getNamespace(), ACKNOWLEDGED, false, workerCodec, replacement)
                .filter(new BsonDocument('name', new BsonString('Jordan')))
                .returnOriginal(false)
        returnedDocument = execute(operation, async)

        then:
        returnedDocument == pete

        where:
        async << [true, false]
    }

    def 'should return null if query fails to match'() {
        when:
        BsonDocument jordan = BsonDocument.parse('{name: "Jordan", job: "sparky"}')
        def operation = new FindAndReplaceOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec, jordan)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Document returnedDocument = execute(operation, async)

        then:
        returnedDocument == null

        where:
        async << [true, false]
    }

    def 'should throw an exception if replacement contains update operators'() {
        given:
        def replacement = new BsonDocumentWrapper<Document>(['$inc': 1] as Document, documentCodec)
        def operation = new FindAndReplaceOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec, replacement)

        when:
        execute(operation, async)

        then:
        def e = thrown(IllegalArgumentException)
        e.getMessage() == 'Field names in a replacement document can not start with \'$\' but \'$inc\' does'

        where:
        async << [true, false]
    }

    @IgnoreIf({ serverVersionLessThan(3, 2) })
    def 'should support bypassDocumentValidation'() {
        given:
        def namespace = new MongoNamespace(getDatabaseName(), 'collectionOut')
        def collectionHelper = getCollectionHelper(namespace)
        collectionHelper.create('collectionOut', new CreateCollectionOptions().validationOptions(
                new ValidationOptions().validator(gte('level', 10))))
        collectionHelper.insertDocuments(BsonDocument.parse('{ level: 10 }'))

        when:
        def replacement = new BsonDocument('level', new BsonInt32(9))
        def operation = new FindAndReplaceOperation<Document>(namespace, ACKNOWLEDGED, false, documentCodec, replacement)
        execute(operation, async)

        then:
        thrown(MongoCommandException)

        when:
        operation.bypassDocumentValidation(false)
        execute(operation, async)

        then:
        thrown(MongoCommandException)

        when:
        operation.bypassDocumentValidation(true).returnOriginal(false)
        Document returnedDocument = execute(operation, async)

        then:
        notThrown(MongoCommandException)
        returnedDocument.getInteger('level') == 9

        cleanup:
        collectionHelper?.drop()

        where:
        async << [true, false]
    }

    @IgnoreIf({ serverVersionLessThan(3, 2) || !isDiscoverableReplicaSet() })
    def 'should throw on write concern error'() {
        given:
        CollectionHelper<Document> helper = new CollectionHelper<Document>(documentCodec, getNamespace())
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        helper.insertDocuments(new DocumentCodec(), pete)

        BsonDocument jordan = BsonDocument.parse('{name: "Jordan", job: "sparky"}')

        when:
        def operation = new FindAndReplaceOperation<Document>(getNamespace(), new WriteConcern(5, 1),
                false, documentCodec, jordan).filter(new BsonDocument('name', new BsonString('Pete')))
        execute(operation, async)

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 100
        !ex.writeConcernError.message.isEmpty()
        ex.writeResult.count == 1
        ex.writeResult.updateOfExisting
        ex.writeResult.upsertedId == null

        when:
        operation = new FindAndReplaceOperation<Document>(getNamespace(), new WriteConcern(5, 1),
                false, documentCodec, jordan).filter(new BsonDocument('name', new BsonString('Bob')))
                .upsert(true)
        execute(operation, async)

        then:
        ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 100
        !ex.writeConcernError.message.isEmpty()
        ex.writeResult.count == 1
        !ex.writeResult.updateOfExisting
        ex.writeResult.upsertedId instanceof BsonObjectId

        where:
        async << [true, false]
    }


    @IgnoreIf({ serverVersionLessThan(3, 8) || !isDiscoverableReplicaSet() })
    def 'should throw on write concern error on multiple failpoint'() {
        CollectionHelper<Document> helper = new CollectionHelper<Document>(documentCodec, getNamespace())
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        helper.insertDocuments(new DocumentCodec(), pete)

        def failPoint = BsonDocument.parse('''{
            "configureFailPoint": "failCommand",
            "mode": {"times": 2 },
            "data": { "failCommands": ["findAndModify"],
                      "writeConcernError": {"code": 91, "errmsg": "Replication is being shut down"}}}''')
        configureFailPoint(failPoint)

        BsonDocument jordan = BsonDocument.parse('{name: "Jordan", job: "sparky"}')
        def operation = new FindAndReplaceOperation<Document>(getNamespace(), ACKNOWLEDGED,
                false, documentCodec, jordan).filter(new BsonDocument('name', new BsonString('Pete')))

        when:
        execute(operation, async)

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 91
        !ex.writeConcernError.message.isEmpty()
        ex.writeResult.count == 1
        ex.writeResult.updateOfExisting
        ex.writeResult.upsertedId == null

        cleanup:
        disableFailPoint('failCommand')

        where:
        async << [true, false]
    }

    def 'should create the expected command'() {
        when:
        def includeBypassValidation = serverVersionIsGreaterThan(serverVersion, [3, 4, 0])
        def includeCollation = serverVersionIsGreaterThan(serverVersion, [3, 4, 0])
        def includeTxnNumber = (serverVersionIsGreaterThan(serverVersion, [3, 6, 0]) && retryWrites
                && writeConcern.isAcknowledged() && serverType != STANDALONE)
        def includeWriteConcern = (writeConcern.isAcknowledged() && !writeConcern.isServerDefault()
                && serverVersionIsGreaterThan(serverVersion, [3, 4, 0]))


        def cannedResult = new BsonDocument('value', new BsonDocumentWrapper(BsonDocument.parse('{}'), new BsonDocumentCodec()))
        def replacement = BsonDocument.parse('{ replacement: 1}')
        def operation = new FindAndReplaceOperation<Document>(getNamespace(), writeConcern, retryWrites, documentCodec, replacement)
        def expectedCommand = new BsonDocument('findAndModify', new BsonString(getNamespace().getCollectionName()))
                .append('update', replacement)
        if (includeWriteConcern) {
            expectedCommand.put('writeConcern', writeConcern.asDocument())
        }
        if (includeTxnNumber) {
            expectedCommand.put('txnNumber', new BsonInt64(0))
        }
        expectedCommand.put('new', BsonBoolean.FALSE)

        then:
        testOperation([operation: operation, serverVersion: serverVersion, expectedCommand: expectedCommand, async: async,
                       result   : cannedResult, serverType: serverType])

        when:
        def filter = BsonDocument.parse('{ filter : 1}')
        def sort = BsonDocument.parse('{ sort : 1}')
        def projection = BsonDocument.parse('{ projection : 1}')

        operation.filter(filter)
                .sort(sort)
                .projection(projection)
                .bypassDocumentValidation(true)
                .maxTime(10, TimeUnit.MILLISECONDS)

        expectedCommand.append('query', filter)
                .append('sort', sort)
                .append('fields', projection)
                .append('maxTimeMS', new BsonInt64(10))

        if (includeCollation) {
            operation.collation(defaultCollation)
            expectedCommand.append('collation', defaultCollation.asDocument())
        }
        if (includeBypassValidation) {
            expectedCommand.append('bypassDocumentValidation', BsonBoolean.TRUE)
        }

        then:
        testOperation([operation: operation, serverVersion: serverVersion, expectedCommand: expectedCommand, async: async,
                       result   : cannedResult, serverType: serverType])

        where:
        [serverVersion, serverType, writeConcern, async, retryWrites] << [
                [[3, 6, 0], [3, 4, 0]],
                [REPLICA_SET_PRIMARY, STANDALONE],
                [ACKNOWLEDGED, W1, UNACKNOWLEDGED],
                [true, false],
                [true, false]
        ].combinations()
    }

    @IgnoreIf({ serverVersionLessThan(3, 6) || !isDiscoverableReplicaSet() })
    def 'should support retryable writes'() {
        given:
        CollectionHelper<Document> helper = new CollectionHelper<Document>(documentCodec, getNamespace())
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        BsonDocument jordan = BsonDocument.parse('{name: "Jordan", job: "sparky"}')

        helper.insertDocuments(new DocumentCodec(), pete, sam)

        when:
        def operation = new FindAndReplaceOperation<Document>(getNamespace(), ACKNOWLEDGED, true, documentCodec, jordan)
                .filter(new BsonDocument('name', new BsonString('Pete')))

        enableOnPrimaryTransactionalWriteFailPoint(BsonDocument.parse('{times: 1}'))
        Document returnedDocument = executeWithSession(operation, async)

        then:
        returnedDocument.getString('name') == 'Pete'
        helper.find().size() == 2
        helper.find().get(0).getString('name') == 'Jordan'

        cleanup:
        disableOnPrimaryTransactionalWriteFailPoint()

        where:
        async << [true, false]
    }

    def 'should retry if the connection initially fails'() {
        when:
        def cannedResult = new BsonDocument('value', new BsonDocumentWrapper(BsonDocument.parse('{}'), new BsonDocumentCodec()))
        def replacement = BsonDocument.parse('{ replacement: 1}')
        def operation = new FindAndReplaceOperation<Document>(getNamespace(), ACKNOWLEDGED, true, documentCodec, replacement)
        def expectedCommand = new BsonDocument('findAndModify', new BsonString(getNamespace().getCollectionName()))
                .append('update', replacement)
                .append('txnNumber', new BsonInt64(0))
                .append('new', BsonBoolean.FALSE)

        then:
        testOperationRetries(operation, [3, 6, 0], expectedCommand, async, cannedResult)

        where:
        async << [true, false]
    }

    def 'should throw original error when retrying and failing'() {
        given:
        def replacement = BsonDocument.parse('{ replacement: 1}')
        def operation = new FindAndReplaceOperation<Document>(getNamespace(), ACKNOWLEDGED, true, documentCodec, replacement)
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

    @IgnoreIf({ serverVersionLessThan(3, 4) })
    def 'should support collation'() {
        given:
        def document = Document.parse('{_id: 1, str: "foo"}')
        getCollectionHelper().insertDocuments(document)
        def replacement = BsonDocument.parse('{str: "bar"}')
        def operation = new FindAndReplaceOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec, replacement)
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

