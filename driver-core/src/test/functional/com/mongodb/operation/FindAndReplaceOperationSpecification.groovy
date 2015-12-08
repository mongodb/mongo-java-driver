/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import category.Async
import com.mongodb.MongoCommandException
import com.mongodb.MongoNamespace
import com.mongodb.MongoWriteConcernException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.WriteConcern
import com.mongodb.async.SingleResultCallback
import com.mongodb.binding.AsyncConnectionSource
import com.mongodb.binding.AsyncWriteBinding
import com.mongodb.binding.ConnectionSource
import com.mongodb.binding.WriteBinding
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.model.ValidationOptions
import com.mongodb.client.test.CollectionHelper
import com.mongodb.client.test.Worker
import com.mongodb.client.test.WorkerCodec
import com.mongodb.connection.AsyncConnection
import com.mongodb.connection.Connection
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ServerVersion
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
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import java.util.concurrent.TimeUnit

import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.client.model.Filters.gte
import static java.util.Arrays.asList

class FindAndReplaceOperationSpecification extends OperationFunctionalSpecification {
    private final DocumentCodec documentCodec = new DocumentCodec()
    private final WorkerCodec workerCodec = new WorkerCodec()
    def writeConcern = WriteConcern.ACKNOWLEDGED


    def 'should have the correct defaults and passed values'() {
        when:
        def replacement = new BsonDocument('replace', new BsonInt32(1))
        def operation = new FindAndReplaceOperation<Document>(getNamespace(), writeConcern, documentCodec, replacement)

        then:
        operation.getNamespace() == getNamespace()
        operation.getWriteConcern() == writeConcern
        operation.getDecoder() == documentCodec
        operation.getReplacement() == replacement
        operation.getFilter() == null
        operation.getSort() == null
        operation.getProjection() == null
        operation.getMaxTime(TimeUnit.SECONDS) == 0
        operation.getBypassDocumentValidation() == null
    }

    def 'should set optional values correctly'(){
        given:
        def filter = new BsonDocument('filter', new BsonInt32(1))
        def sort = new BsonDocument('sort', new BsonInt32(1))
        def projection = new BsonDocument('projection', new BsonInt32(1))

        when:
        def operation = new FindAndReplaceOperation<Document>(getNamespace(), writeConcern, documentCodec,
                new BsonDocument('replace', new BsonInt32(1))).filter(filter).sort(sort).projection(projection)
                .bypassDocumentValidation(true).maxTime(1, TimeUnit.SECONDS).upsert(true).returnOriginal(false)

        then:
        operation.getFilter() == filter
        operation.getSort() == sort
        operation.getProjection() == projection
        operation.upsert == true
        operation.getMaxTime(TimeUnit.SECONDS) == 1
        operation.getBypassDocumentValidation()
        !operation.isReturnOriginal()
    }

    def 'should replace single document'() {
        given:
        CollectionHelper<Document> helper = new CollectionHelper<Document>(documentCodec, getNamespace())
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        BsonDocument jordan = new BsonDocumentWrapper<Document>([name: 'Jordan', job: 'sparky'] as Document, documentCodec)

        helper.insertDocuments(new DocumentCodec(), pete, sam)

        when:
        def operation = new FindAndReplaceOperation<Document>(getNamespace(), writeConcern, documentCodec, jordan)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Document returnedDocument = operation.execute(getBinding())

        then:
        returnedDocument.getString('name') == 'Pete'
        helper.find().size() == 2;
        helper.find().get(0).getString('name') == 'Jordan'

        when:
        operation = new FindAndReplaceOperation<Document>(getNamespace(), writeConcern, documentCodec,
                                                          new BsonDocumentWrapper<Document>(pete, documentCodec))
                .filter(new BsonDocument('name', new BsonString('Jordan')))
                .returnOriginal(false)
        returnedDocument = operation.execute(getBinding())

        then:
        returnedDocument.getString('name') == 'Pete'
    }

    @Category(Async)
    def 'should replace single document asynchronously'() {
        given:
        CollectionHelper<Document> helper = new CollectionHelper<Document>(documentCodec, getNamespace())
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        BsonDocument jordan = new BsonDocumentWrapper<Document>([name: 'Jordan', job: 'sparky'] as Document, documentCodec)

        helper.insertDocuments(new DocumentCodec(), pete, sam)

        when:
        def operation = new FindAndReplaceOperation<Document>(getNamespace(), writeConcern, documentCodec, jordan)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Document returnedDocument = executeAsync(operation)

        then:
        returnedDocument.getString('name') == 'Pete'
        helper.find().size() == 2;
        helper.find().get(0).getString('name') == 'Jordan'

        when:
        operation = new FindAndReplaceOperation<Document>(getNamespace(), writeConcern, documentCodec,
                                                          new BsonDocumentWrapper<Document>(pete, documentCodec))
                .filter(new BsonDocument('name', new BsonString('Jordan')))
                .returnOriginal(false)
        returnedDocument = executeAsync(operation)

        then:
        returnedDocument.getString('name') == 'Pete'
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
        def operation = new FindAndReplaceOperation<Document>(getNamespace(), writeConcern, workerCodec,
                replacement).filter(new BsonDocument('name', new BsonString('Pete')))
        Worker returnedDocument = operation.execute(getBinding())

        then:
        returnedDocument == pete
        helper.find().get(0) == jordan

        when:
        replacement = new BsonDocumentWrapper<Worker>(pete, workerCodec)
        operation = new FindAndReplaceOperation<Document>(getNamespace(), writeConcern, workerCodec, replacement)
                .filter(new BsonDocument('name', new BsonString('Jordan')))
                .returnOriginal(false)
        returnedDocument = operation.execute(getBinding())

        then:
        returnedDocument == pete
    }

    @Category(Async)
    def 'should replace single document when using custom codecs asynchronously'() {
        given:
        CollectionHelper<Worker> helper = new CollectionHelper<Worker>(workerCodec, getNamespace())
        Worker pete = new Worker('Pete', 'handyman', new Date(), 3)
        Worker sam = new Worker('Sam', 'plumber', new Date(), 5)
        Worker jordan = new Worker(pete.id, 'Jordan', 'sparky', new Date(), 7)
        BsonDocument replacement = new BsonDocumentWrapper<Worker>(jordan, workerCodec)

        helper.insertDocuments(new WorkerCodec(), pete, sam)

        when:
        def operation = new FindAndReplaceOperation<Document>(getNamespace(), writeConcern, workerCodec, replacement)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Worker returnedDocument = executeAsync(operation)

        then:
        returnedDocument == pete
        helper.find().get(0) == jordan

        when:
        replacement = new BsonDocumentWrapper<Worker>(pete, workerCodec)
        operation = new FindAndReplaceOperation<Document>(getNamespace(), writeConcern, workerCodec, replacement)
                .filter(new BsonDocument('name', new BsonString('Jordan')))
                .returnOriginal(false)
        returnedDocument = executeAsync(operation)

        then:
        returnedDocument == pete
    }

    def 'should return null if query fails to match'() {
        when:
        BsonDocument jordan = new BsonDocumentWrapper<Document>([name: 'Jordan', job: 'sparky'] as Document, documentCodec)
        def operation = new FindAndReplaceOperation<Document>(getNamespace(), writeConcern, documentCodec, jordan)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Document returnedDocument = operation.execute(getBinding())

        then:
        returnedDocument == null
    }

    @Category(Async)
    def 'should return null if query fails to match asynchronously'() {
        when:
        BsonDocument jordan = new BsonDocumentWrapper<Document>([name: 'Jordan', job: 'sparky'] as Document, documentCodec)
        def operation = new FindAndReplaceOperation<Document>(getNamespace(), writeConcern, documentCodec, jordan)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Document returnedDocument = executeAsync(operation)

        then:
        returnedDocument == null
    }

    def 'should throw an exception if replacement contains update operators'() {
        when:
        def replacement = new BsonDocumentWrapper<Document>(['$inc': 1] as Document, documentCodec)
        new FindAndReplaceOperation<Document>(getNamespace(), writeConcern, documentCodec, replacement).execute(getBinding())

        then:
        thrown(IllegalArgumentException)
    }

    @Category(Async)
    def 'should throw an exception if replacement contains update operators asynchronously'() {
        when:
        def replacement = new BsonDocumentWrapper<Document>(['$inc': 1] as Document, documentCodec)
        executeAsync(new FindAndReplaceOperation<Document>(getNamespace(), writeConcern, documentCodec, replacement))

        then:
        thrown(IllegalArgumentException)
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 1, 8)) })
    def 'should support bypassDocumentValidation'() {
        given:
        def namespace = new MongoNamespace(getDatabaseName(), 'collectionOut')
        def collectionHelper = getCollectionHelper(namespace)
        collectionHelper.create('collectionOut', new CreateCollectionOptions().validationOptions(
                new ValidationOptions().validator(gte('level', 10))))
        collectionHelper.insertDocuments(BsonDocument.parse('{ level: 10 }'))

        when:
        def replacement = new BsonDocument('level', new BsonInt32(9))
        def operation = new FindAndReplaceOperation<Document>(namespace, writeConcern, documentCodec, replacement)
        operation.execute(getBinding())

        then:
        thrown(MongoCommandException)

        when:
        operation.bypassDocumentValidation(false).execute(getBinding())

        then:
        thrown(MongoCommandException)

        when:
        Document returnedDocument = operation.bypassDocumentValidation(true).returnOriginal(false).execute(getBinding())

        then:
        notThrown(MongoCommandException)
        returnedDocument.getInteger('level') == 9

        cleanup:
        collectionHelper?.drop()
    }

    @Category(Async)
    @IgnoreIf({ !serverVersionAtLeast(asList(3, 1, 8)) })
    def 'should support bypassDocumentValidation asynchronously'() {
        given:
        def namespace = new MongoNamespace(getDatabaseName(), 'collectionOut')
        def collectionHelper = getCollectionHelper(namespace)
        collectionHelper.create('collectionOut', new CreateCollectionOptions().validationOptions(
                new ValidationOptions().validator(gte('level', 10))))
        collectionHelper.insertDocuments(BsonDocument.parse('{ level: 10 }'))

        when:
        def replacement = new BsonDocument('level', new BsonInt32(9))
        def operation = new FindAndReplaceOperation<Document>(namespace, writeConcern, documentCodec, replacement)
        executeAsync(operation)

        then:
        thrown(MongoCommandException)

        when:
        executeAsync(operation.bypassDocumentValidation(false))

        then:
        thrown(MongoCommandException)

        when:
        Document returnedDocument = executeAsync(operation.bypassDocumentValidation(true).returnOriginal(false))

        then:
        notThrown(MongoCommandException)
        returnedDocument.getInteger('level') == 9

        cleanup:
        collectionHelper?.drop()
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 2, 0)) || !isDiscoverableReplicaSet() })
    def 'should throw on write concern error'() {
        given:
        CollectionHelper<Document> helper = new CollectionHelper<Document>(documentCodec, getNamespace())
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        helper.insertDocuments(new DocumentCodec(), pete)

        BsonDocument jordan = new BsonDocumentWrapper<Document>([name: 'Jordan', job: 'sparky'] as Document, documentCodec)

        when:
        def operation = new FindAndReplaceOperation<Document>(getNamespace(), new WriteConcern(5, 1), documentCodec, jordan)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        operation.execute(getBinding())

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 100
        !ex.writeConcernError.message.isEmpty()
        ex.writeResult.count == 1
        ex.writeResult.updateOfExisting
        ex.writeResult.upsertedId == null

        when:
        operation = new FindAndReplaceOperation<Document>(getNamespace(), new WriteConcern(5, 1), documentCodec, jordan)
                .filter(new BsonDocument('name', new BsonString('Bob')))
                .upsert(true)
        operation.execute(getBinding())

        then:
        ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 100
        !ex.writeConcernError.message.isEmpty()
        ex.writeResult.count == 1
        !ex.writeResult.updateOfExisting
        ex.writeResult.upsertedId instanceof BsonObjectId
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 2, 0)) || !isDiscoverableReplicaSet() })
    def 'should throw on write concern error asynchronously'() {
        given:
        CollectionHelper<Document> helper = new CollectionHelper<Document>(documentCodec, getNamespace())
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        helper.insertDocuments(new DocumentCodec(), pete)

        BsonDocument jordan = new BsonDocumentWrapper<Document>([name: 'Jordan', job: 'sparky'] as Document, documentCodec)

        when:
        def operation = new FindAndReplaceOperation<Document>(getNamespace(), new WriteConcern(5, 1), documentCodec, jordan)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        executeAsync(operation)

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 100
        !ex.writeConcernError.message.isEmpty()
        ex.writeResult.count == 1
        ex.writeResult.updateOfExisting
        ex.writeResult.upsertedId == null

        when:
        operation = new FindAndReplaceOperation<Document>(getNamespace(), new WriteConcern(5, 1), documentCodec, jordan)
                .filter(new BsonDocument('name', new BsonString('Bob')))
                .upsert(true)
        executeAsync(operation)

        then:
        ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 100
        !ex.writeConcernError.message.isEmpty()
        ex.writeResult.count == 1
        !ex.writeResult.updateOfExisting
        ex.writeResult.upsertedId instanceof BsonObjectId
    }

    def 'should create the expected command'() {
        given:
        def cannedResult = new BsonDocument('value', new BsonDocumentWrapper(BsonDocument.parse('{}'), new BsonDocumentCodec()))
        def replacement = BsonDocument.parse('{ replacement: 1}')
        def filter = BsonDocument.parse('{ filter : 1}')
        def sort = BsonDocument.parse('{ sort : 1}')
        def projection = BsonDocument.parse('{ projection : 1}')

        def connection = Mock(Connection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getServerVersion() >> serverVersion
            }
        }
        def connectionSource = Stub(ConnectionSource) {
            getConnection() >> connection
        }

        def writeBinding = Stub(WriteBinding) {
            getWriteConnectionSource() >> connectionSource
        }
        def operation = new FindAndReplaceOperation<Document>(getNamespace(), writeConcern, documentCodec, replacement)
        def expectedCommand = new BsonDocument('findandmodify', new BsonString(getNamespace().getCollectionName()))
                .append('update', replacement)
        if (includeWriteConcern) {
            expectedCommand.put('writeConcern', writeConcern.asDocument())
        }

        when:
        operation.execute(writeBinding)

        then:
        1 * connection.command(getNamespace().getDatabaseName(), expectedCommand, _, _, _) >> cannedResult
        1 * connection.release()

        when:
        operation.filter(filter)
                .sort(sort)
                .projection(projection)
                .bypassDocumentValidation(true)
                .maxTime(10, TimeUnit.MILLISECONDS)

        expectedCommand.append('query', filter)
                .append('sort', sort)
                .append('fields', projection)
                .append('maxTimeMS', new BsonInt64(10))

        if (includeBypassValidation) {
            expectedCommand.append('bypassDocumentValidation', BsonBoolean.TRUE)
        }

        operation.execute(writeBinding)

        then:
        1 * connection.command(getNamespace().getDatabaseName(), expectedCommand, _, _, _) >> cannedResult
        1 * connection.release()

        where:
        serverVersion                | writeConcern                 | includeWriteConcern   | includeBypassValidation
        new ServerVersion([3, 2, 0]) | WriteConcern.W1              | true                  | true
        new ServerVersion([3, 2, 0]) | WriteConcern.ACKNOWLEDGED    | false                 | true
        new ServerVersion([3, 2, 0]) | WriteConcern.UNACKNOWLEDGED  | false                 | true
        new ServerVersion([3, 0, 0]) | WriteConcern.ACKNOWLEDGED    | false                 | false
        new ServerVersion([3, 0, 0]) | WriteConcern.UNACKNOWLEDGED  | false                 | false
        new ServerVersion([3, 0, 0]) | WriteConcern.W1              | false                 | false
    }

    def 'should create the expected command asynchronously'() {
        given:
        def cannedResult = new BsonDocument('value', new BsonDocumentWrapper(BsonDocument.parse('{}'), new BsonDocumentCodec()))
        def replacement = BsonDocument.parse('{ replacement: 1}')
        def filter = BsonDocument.parse('{ filter : 1}')
        def sort = BsonDocument.parse('{ sort : 1}')
        def projection = BsonDocument.parse('{ projection : 1}')
        def connection = Mock(AsyncConnection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getServerVersion() >> serverVersion
            }
        }
        def connectionSource = Stub(AsyncConnectionSource) {
            getConnection(_) >> { it[0].onResult(connection, null) }
        }
        def writeBinding = Stub(AsyncWriteBinding) {
            getWriteConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
        }
        def operation = new FindAndReplaceOperation<Document>(getNamespace(), writeConcern, documentCodec, replacement)
        def expectedCommand = new BsonDocument('findandmodify', new BsonString(getNamespace().getCollectionName()))
                .append('update', replacement)
        if (includeWriteConcern) {
            expectedCommand.put('writeConcern', writeConcern.asDocument())
        }

        when:
        operation.executeAsync(writeBinding, Stub(SingleResultCallback))

        then:
        1 * connection.commandAsync(getNamespace().getDatabaseName(), expectedCommand, _, _, _, _) >> { it[5].onResult(cannedResult, null) }
        1 * connection.release()

        when:
        operation.filter(filter)
                .sort(sort)
                .projection(projection)
                .bypassDocumentValidation(true)
                .maxTime(10, TimeUnit.MILLISECONDS)

        expectedCommand.append('query', filter)
                .append('sort', sort)
                .append('fields', projection)
                .append('maxTimeMS', new BsonInt64(10))

        if (includeBypassValidation) {
            expectedCommand.append('bypassDocumentValidation', BsonBoolean.TRUE)
        }

        operation.executeAsync(writeBinding, Stub(SingleResultCallback))

        then:
        1 * connection.commandAsync(getNamespace().getDatabaseName(), expectedCommand, _, _, _, _) >> { it[5].onResult(cannedResult, null) }
        1 * connection.release()

        where:
        serverVersion                | writeConcern                 | includeWriteConcern   | includeBypassValidation
        new ServerVersion([3, 2, 0]) | WriteConcern.W1              | true                  | true
        new ServerVersion([3, 2, 0]) | WriteConcern.ACKNOWLEDGED    | false                 | true
        new ServerVersion([3, 2, 0]) | WriteConcern.UNACKNOWLEDGED  | false                 | true
        new ServerVersion([3, 0, 0]) | WriteConcern.ACKNOWLEDGED    | false                 | false
        new ServerVersion([3, 0, 0]) | WriteConcern.UNACKNOWLEDGED  | false                 | false
        new ServerVersion([3, 0, 0]) | WriteConcern.W1              | false                 | false
    }
}
