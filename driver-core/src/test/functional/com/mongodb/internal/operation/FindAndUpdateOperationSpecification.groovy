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
import org.bson.BsonArray
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
import static java.util.Collections.singletonList

class FindAndUpdateOperationSpecification extends OperationFunctionalSpecification {
    private final DocumentCodec documentCodec = new DocumentCodec()
    private final WorkerCodec workerCodec = new WorkerCodec()

    def 'should have the correct defaults and passed values'() {
        when:
        def update = new BsonDocument('update', new BsonInt32(1))
        def operation = new FindAndUpdateOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec, update)

        then:
        operation.getNamespace() == getNamespace()
        operation.getWriteConcern() == ACKNOWLEDGED
        operation.getDecoder() == documentCodec
        operation.getUpdate() == update
        operation.getFilter() == null
        operation.getSort() == null
        operation.getProjection() == null
        operation.getMaxTime(TimeUnit.SECONDS) == 0
        operation.getBypassDocumentValidation() == null
        operation.getCollation() == null
    }

    @IgnoreIf({ serverVersionLessThan(4, 2) })
    def 'should have the correct defaults and passed values using update pipelines'() {
        when:
        def updatePipeline = new BsonArray(singletonList(new BsonDocument('update', new BsonInt32(1))))
        def operation = new FindAndUpdateOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec, updatePipeline)

        then:
        operation.getNamespace() == getNamespace()
        operation.getWriteConcern() == ACKNOWLEDGED
        operation.getDecoder() == documentCodec
        operation.getUpdatePipeline() == updatePipeline
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
        def operation = new FindAndUpdateOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec,
                new BsonDocument('update', new BsonInt32(1))).filter(filter).sort(sort).projection(projection)
                .bypassDocumentValidation(true).maxTime(1, TimeUnit.SECONDS).upsert(true)
                .returnOriginal(false)
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

    @IgnoreIf({ serverVersionLessThan(4, 2) })
    def 'should set optional values correctly when using update pipelines'(){
        given:
        def filter = new BsonDocument('filter', new BsonInt32(1))
        def sort = new BsonDocument('sort', new BsonInt32(1))
        def projection = new BsonDocument('projection', new BsonInt32(1))

        when:
        def operation = new FindAndUpdateOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec,
                new BsonArray(singletonList(new BsonDocument('update', new BsonInt32(1)))))
                        .filter(filter).sort(sort).projection(projection)
                .bypassDocumentValidation(true).maxTime(1, TimeUnit.SECONDS).upsert(true)
                .returnOriginal(false)
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

    def 'should update single document'() {
        given:
        CollectionHelper<Document> helper = new CollectionHelper<Document>(documentCodec, getNamespace())
        Document pete = new Document('name', 'Pete').append('numberOfJobs', 3)
        Document sam = new Document('name', 'Sam').append('numberOfJobs', 5)

        helper.insertDocuments(new DocumentCodec(), pete, sam)

        when:
        def update = new BsonDocument('$inc', new BsonDocument('numberOfJobs', new BsonInt32(1)))
        def operation = new FindAndUpdateOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec, update)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Document returnedDocument = execute(operation, async)

        then:
        returnedDocument.getInteger('numberOfJobs') == 3
        helper.find().size() == 2
        helper.find().get(0).getInteger('numberOfJobs') == 4

        when:
        update = new BsonDocument('$inc', new BsonDocument('numberOfJobs', new BsonInt32(1)))
        operation = new FindAndUpdateOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec, update)
                .filter(new BsonDocument('name', new BsonString('Pete')))
                .returnOriginal(false)
        returnedDocument = execute(operation, async)

        then:
        returnedDocument.getInteger('numberOfJobs') == 5

        where:
        async << [true, false]
    }

    @IgnoreIf({ serverVersionLessThan(4, 2) })
    def 'should add field using update pipeline'() {
        given:
        CollectionHelper<Document> helper = new CollectionHelper<Document>(documentCodec, getNamespace())
        Document pete = new Document('name', 'Pete').append('numberOfJobs', 3)
        Document sam = new Document('name', 'Sam').append('numberOfJobs', 5)

        helper.insertDocuments(new DocumentCodec(), pete, sam)

        when:
        def update = new BsonArray(singletonList(new BsonDocument('$addFields', new BsonDocument('foo', new BsonInt32(1)))))
        def operation = new FindAndUpdateOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec, update)
                .filter(new BsonDocument('name', new BsonString('Pete')))
                .returnOriginal(false)
        Document returnedDocument = execute(operation, false)

        then:
        returnedDocument.getInteger('numberOfJobs') == 3
        helper.find().get(0).getInteger('foo') == 1

        when:
        update = new BsonArray(singletonList(new BsonDocument('$addFields', new BsonDocument('foo', new BsonInt32(1)))))
        operation = new FindAndUpdateOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec, update)
                .filter(new BsonDocument('name', new BsonString('Pete')))
                .returnOriginal(false)
        returnedDocument = execute(operation, false)

        then:
        returnedDocument.getInteger('numberOfJobs') == 3
        helper.find().get(0).getInteger('foo') == 1
    }

    def 'should update single document when using custom codecs'() {
        given:
        CollectionHelper<Worker> helper = new CollectionHelper<Worker>(workerCodec, getNamespace())
        Worker pete = new Worker('Pete', 'handyman', new Date(), 3)
        Worker sam = new Worker('Sam', 'plumber', new Date(), 5)

        helper.insertDocuments(new WorkerCodec(), pete, sam)

        when:
        def update = new BsonDocument('$inc', new BsonDocument('numberOfJobs', new BsonInt32(1)))
        def operation = new FindAndUpdateOperation<Worker>(getNamespace(), ACKNOWLEDGED, false, workerCodec, update)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Worker returnedDocument = execute(operation, async)

        then:
        returnedDocument.numberOfJobs == 3
        helper.find().size() == 2
        helper.find().get(0).numberOfJobs == 4

        when:
        update = new BsonDocument('$inc', new BsonDocument('numberOfJobs', new BsonInt32(1)))
        operation = new FindAndUpdateOperation<Worker>(getNamespace(), ACKNOWLEDGED, false, workerCodec, update)
                .filter(new BsonDocument('name', new BsonString('Pete')))
                .returnOriginal(false)
        returnedDocument = execute(operation, async)

        then:
        returnedDocument.numberOfJobs == 5

        where:
        async << [true, false]
    }

    @IgnoreIf({ serverVersionLessThan(4, 2) })
    def 'should update using pipeline when using custom codecs'() {
        given:
        CollectionHelper<Document> helper = new CollectionHelper<Document>(documentCodec, getNamespace())
        Document pete = new Document('name', 'Pete').append('numberOfJobs', 3)
        Document sam = new Document('name', 'Sam').append('numberOfJobs', 5)

        helper.insertDocuments(new DocumentCodec(), pete, sam)

        when:
        def update = new BsonArray(singletonList(new BsonDocument('$project', new BsonDocument('name', new BsonInt32(1)))))
        def operation = new FindAndUpdateOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec, update)
                .filter(new BsonDocument('name', new BsonString('Pete')))
                .returnOriginal(false)
        Document returnedDocument = execute(operation, async)

        then:
        returnedDocument.getString('name') == 'Pete'
        !returnedDocument.containsKey('numberOfJobs')

        where:
        async << [true, false]
    }

    def 'should return null if query fails to match'() {
        when:
        def update = new BsonDocument('$inc', new BsonDocument('numberOfJobs', new BsonInt32(1)))
        def operation = new FindAndUpdateOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec, update)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Document returnedDocument = execute(operation, async)

        then:
        returnedDocument == null

        where:
        async << [true, false]
    }

    def 'should throw an exception if update contains fields that are not update operators'() {
        given:
        def update = new BsonDocument('x', new BsonInt32(1))
        def operation = new FindAndUpdateOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec, update)

        when:
        execute(operation, async)

        then:
        def e = thrown(IllegalArgumentException)
        e.getMessage() == 'All update operators must start with \'$\', but \'x\' does not'

        where:
        async << [true, false]
    }

    @IgnoreIf({ serverVersionLessThan(4, 2) })
    def 'should throw an exception if update pipeline contains operations that are not supported'() {
        when:
        def update = new BsonArray(singletonList(new BsonDocument('$foo', new BsonDocument('x', new BsonInt32(1)))))
        def operation = new FindAndUpdateOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec, update)
        execute(operation, async)

        then:
        thrown(MongoCommandException)

        when:
        update = singletonList(new BsonInt32(1))
        operation = new FindAndUpdateOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec, update)
        execute(operation, async)

        then:
        thrown(MongoCommandException)

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
        def update = new BsonDocument('$inc', new BsonDocument('level', new BsonInt32(-1)))
        def operation = new FindAndUpdateOperation<Document>(namespace, ACKNOWLEDGED, false, documentCodec, update)
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
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('name', 'Pete'))
        def update = new BsonDocument('$inc', new BsonDocument('numberOfJobs', new BsonInt32(1)))

        when:
        def operation = new FindAndUpdateOperation<Document>(getNamespace(), new WriteConcern(5, 1), false, documentCodec, update)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        execute(operation, async)

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 100
        !ex.writeConcernError.message.isEmpty()
        ex.writeResult.count == 1
        ex.writeResult.updateOfExisting
        ex.writeResult.upsertedId == null

        when:
        operation = new FindAndUpdateOperation<Document>(getNamespace(), new WriteConcern(5, 1), false, documentCodec, update)
                .filter(new BsonDocument('name', new BsonString('Bob')))
                .upsert(true)
        execute(operation, async)

        then:
        ex = thrown(MongoWriteConcernException)
        ex.writeResult.count == 1
        !ex.writeResult.updateOfExisting
        ex.writeResult.upsertedId instanceof BsonObjectId

        where:
        async << [true, false]
    }

    @IgnoreIf({ serverVersionLessThan(3, 8) || !isDiscoverableReplicaSet() })
    def 'should throw on write concern error on multiple failpoint'() {
        given:
        CollectionHelper<Document> helper = new CollectionHelper<Document>(documentCodec, getNamespace())
        helper.insertDocuments(new DocumentCodec(), new Document('name', 'Pete'))

        def failPoint = BsonDocument.parse('''{
            "configureFailPoint": "failCommand",
            "mode": {"times": 2 },
            "data": { "failCommands": ["findAndModify"],
                      "writeConcernError": {"code": 91, "errmsg": "Replication is being shut down"}}}''')
        configureFailPoint(failPoint)

        def update = new BsonDocument('$inc', new BsonDocument('numberOfJobs', new BsonInt32(1)))
        def operation = new FindAndUpdateOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec, update)
                .filter(new BsonDocument('name', new BsonString('Pete')))

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
        def includeTxnNumber = retryWrites && writeConcern.isAcknowledged() && serverType != STANDALONE
        def includeWriteConcern = writeConcern.isAcknowledged() && !writeConcern.isServerDefault()
        def cannedResult = new BsonDocument('value', new BsonDocumentWrapper(BsonDocument.parse('{}'), new BsonDocumentCodec()))
        def update = BsonDocument.parse('{ update: 1}')
        def operation = new FindAndUpdateOperation<Document>(getNamespace(), writeConcern, retryWrites, documentCodec, update)
        def expectedCommand = new BsonDocument('findAndModify', new BsonString(getNamespace().getCollectionName()))
                .append('update', update)
        if (includeWriteConcern) {
            expectedCommand.put('writeConcern', writeConcern.asDocument())
        }
        if (includeTxnNumber) {
            expectedCommand.put('txnNumber', new BsonInt64(0))
        }
        expectedCommand.put('new', BsonBoolean.FALSE)

        then:
        testOperation([operation: operation, serverVersion: [3, 6, 0], expectedCommand: expectedCommand, async: async,
                       result: cannedResult, serverType: serverType])

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

        operation.collation(defaultCollation)
        expectedCommand.append('collation', defaultCollation.asDocument())
        expectedCommand.append('bypassDocumentValidation', BsonBoolean.TRUE)

        then:
        testOperation([operation: operation, serverVersion: [3, 6, 0], expectedCommand: expectedCommand, async: async,
                       result: cannedResult, serverType: serverType])

        where:
        [serverType, writeConcern, async, retryWrites] << [
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
        Document pete = new Document('name', 'Pete').append('numberOfJobs', 3)
        Document sam = new Document('name', 'Sam').append('numberOfJobs', 5)

        helper.insertDocuments(new DocumentCodec(), pete, sam)

        when:
        def update = new BsonDocument('$inc', new BsonDocument('numberOfJobs', new BsonInt32(1)))
        def operation = new FindAndUpdateOperation<Document>(getNamespace(), ACKNOWLEDGED, true, documentCodec, update)
                .filter(new BsonDocument('name', new BsonString('Pete')))

        enableOnPrimaryTransactionalWriteFailPoint(BsonDocument.parse('{times: 1}'))

        Document returnedDocument = executeWithSession(operation, async)

        then:
        returnedDocument.getInteger('numberOfJobs') == 3
        helper.find().size() == 2
        helper.find().get(0).getInteger('numberOfJobs') == 4

        cleanup:
        disableOnPrimaryTransactionalWriteFailPoint()

        where:
        async << [true, false]
    }

    def 'should retry if the connection initially fails'() {
        when:
        def cannedResult = new BsonDocument('value', new BsonDocumentWrapper(BsonDocument.parse('{}'), new BsonDocumentCodec()))
        def update = BsonDocument.parse('{ update: 1}')
        def operation = new FindAndUpdateOperation<Document>(getNamespace(), ACKNOWLEDGED, true, documentCodec, update)
        def expectedCommand = new BsonDocument('findAndModify', new BsonString(getNamespace().getCollectionName()))
                .append('update', update)
                .append('txnNumber', new BsonInt64(0))
                .append('new', BsonBoolean.FALSE)

        then:
        testOperationRetries(operation, [3, 6, 0], expectedCommand, async, cannedResult)

        where:
        async << [true, false]
    }

    def 'should throw original error when retrying and failing'() {
        given:
        def update = BsonDocument.parse('{ update: 1}')
        def operation = new FindAndUpdateOperation<Document>(getNamespace(), ACKNOWLEDGED, true, documentCodec, update)
        def originalException = new MongoSocketException('Some failure', new ServerAddress())

        when:
        testRetryableOperationThrowsOriginalError(operation, [[3, 6, 0], [3, 6, 0]],
                [REPLICA_SET_PRIMARY, STANDALONE], originalException, async)

        then:
        Exception commandException = thrown()
        commandException == originalException

        when:
        testRetryableOperationThrowsOriginalError(operation, [[3, 6, 0]],
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
        def update = BsonDocument.parse('{ $set: {str: "bar"}}')
        def operation = new FindAndUpdateOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec, update)
                .filter(BsonDocument.parse('{str: "FOO"}'))
                .collation(caseInsensitiveCollation)

        when:
        def result = execute(operation, async)

        then:
        result == document

        where:
        async << [true, false]
    }

    @IgnoreIf({ serverVersionLessThan(3, 6) })
    def 'should support array filters'() {
        given:
        def documentOne = Document.parse('{_id: 1, y: [ {b: 3}, {b: 1}]}')
        def documentTwo = Document.parse('{_id: 2, y: [ {b: 0}, {b: 1}]}')
        getCollectionHelper().insertDocuments(documentOne, documentTwo)
        def update = BsonDocument.parse('{ $set: {"y.$[i].b": 2}}')
        def arrayFilters = [BsonDocument.parse('{"i.b": 3}')]
        def operation = new FindAndUpdateOperation<Document>(getNamespace(), ACKNOWLEDGED, false, documentCodec, update)
                .returnOriginal(false)
                .arrayFilters(arrayFilters)

        when:
        def result = execute(operation, async)

        then:
        result == Document.parse('{_id: 1, y: [ {b: 2}, {b: 1}]}')

        where:
        async << [true, false]
    }
}
