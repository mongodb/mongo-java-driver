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
import com.mongodb.MongoWriteConcernException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.WriteConcern
import com.mongodb.async.SingleResultCallback
import com.mongodb.binding.AsyncConnectionSource
import com.mongodb.binding.AsyncWriteBinding
import com.mongodb.binding.ConnectionSource
import com.mongodb.binding.WriteBinding
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
import org.bson.BsonInt64
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
import static java.util.Arrays.asList

class FindAndDeleteOperationSpecification extends OperationFunctionalSpecification {
    private final DocumentCodec documentCodec = new DocumentCodec()
    private final WorkerCodec workerCodec = new WorkerCodec()
    def writeConcern = WriteConcern.ACKNOWLEDGED


    def 'should have the correct defaults'() {
        when:
        def operation = new FindAndDeleteOperation<Document>(getNamespace(), writeConcern, documentCodec)

        then:
        operation.getNamespace() == getNamespace()
        operation.getWriteConcern() == writeConcern
        operation.getDecoder() == documentCodec
        operation.getFilter() == null
        operation.getSort() == null
        operation.getProjection() == null
        operation.getMaxTime(TimeUnit.MILLISECONDS) == 0
    }

    def 'should set optional values correctly'(){
        given:
        def filter = BsonDocument.parse('{ filter : 1}')
        def sort = BsonDocument.parse('{ sort : 1}')
        def projection = BsonDocument.parse('{ projection : 1}')

        when:
        def operation = new FindAndDeleteOperation<Document>(getNamespace(), writeConcern, documentCodec)
            .filter(filter)
            .sort(sort)
            .projection(projection)
            .maxTime(10, TimeUnit.MILLISECONDS)

        then:
        operation.getFilter() == filter
        operation.getSort() == sort
        operation.getProjection() == projection
        operation.getMaxTime(TimeUnit.MILLISECONDS) == 10
    }

    def 'should remove single document'() {

        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')

        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam)

        when:
        def operation = new FindAndDeleteOperation<Document>(getNamespace(), writeConcern, documentCodec)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Document returnedDocument = operation.execute(getBinding())

        then:
        getCollectionHelper().find().size() == 1;
        getCollectionHelper().find().first().getString('name') == 'Sam'
        returnedDocument.getString('name') == 'Pete'
    }


    @Category(Async)
    def 'should remove single document asynchronously'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')

        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam)

        when:
        def operation = new FindAndDeleteOperation<Document>(getNamespace(), writeConcern, documentCodec)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Document returnedDocument = executeAsync(operation)

        then:
        getCollectionHelper().find().size() == 1;
        getCollectionHelper().find().first().getString('name') == 'Sam'
        returnedDocument.getString('name') == 'Pete'
    }

    def 'should remove single document when using custom codecs'() {
        given:
        Worker pete = new Worker('Pete', 'handyman', new Date(), 3)
        Worker sam = new Worker('Sam', 'plumber', new Date(), 7)
        getWorkerCollectionHelper().insertDocuments(new WorkerCodec(), pete, sam)

        when:
        FindAndDeleteOperation<Worker> operation = new FindAndDeleteOperation<Document>(getNamespace(), writeConcern, workerCodec)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Worker returnedDocument = operation.execute(getBinding())

        then:
        getWorkerCollectionHelper().find().size() == 1;
        getWorkerCollectionHelper().find().first() == sam
        returnedDocument == pete
    }

    @Category(Async)
    def 'should remove single document when using custom codecs asynchronously'() {
        given:
        Worker pete = new Worker('Pete', 'handyman', new Date(), 3)
        Worker sam = new Worker('Sam', 'plumber', new Date(), 7)
        getWorkerCollectionHelper().insertDocuments(new WorkerCodec(), pete, sam)

        when:
        FindAndDeleteOperation<Worker> operation = new FindAndDeleteOperation<Document>(getNamespace(), writeConcern, workerCodec)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Worker returnedDocument = operation.execute(getBinding())

        then:
        getWorkerCollectionHelper().find().size() == 1;
        getWorkerCollectionHelper().find().first() == sam
        returnedDocument == pete
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 2, 0)) || !isDiscoverableReplicaSet() })
    def 'should throw on write concern error'() {
        given:
        CollectionHelper<Document> helper = new CollectionHelper<Document>(documentCodec, getNamespace())
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        helper.insertDocuments(new DocumentCodec(), pete)

        when:
        def operation = new FindAndDeleteOperation<Document>(getNamespace(), new WriteConcern(5, 1), documentCodec)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        operation.execute(getBinding())

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 100
        !ex.writeConcernError.message.isEmpty()
        ex.writeResult.count == 1
        !ex.writeResult.updateOfExisting
        ex.writeResult.upsertedId == null
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 2, 0)) || !isDiscoverableReplicaSet() })
    def 'should throw on write concern error asynchronously'() {
        given:
        CollectionHelper<Document> helper = new CollectionHelper<Document>(documentCodec, getNamespace())
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        helper.insertDocuments(new DocumentCodec(), pete)

        when:
        def operation = new FindAndDeleteOperation<Document>(getNamespace(), new WriteConcern(5, 1), documentCodec)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        executeAsync(operation)

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 100
        !ex.writeConcernError.message.isEmpty()
        ex.writeResult.count == 1
        !ex.writeResult.updateOfExisting
        ex.writeResult.upsertedId == null
    }

    def 'should create the expected command'() {
        given:
        def cannedResult = new BsonDocument('value', new BsonDocumentWrapper(BsonDocument.parse('{}'), new BsonDocumentCodec()))
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
        def operation = new FindAndDeleteOperation<Document>(getNamespace(), writeConcern, documentCodec)
        def expectedCommand = new BsonDocument('findandmodify', new BsonString(getNamespace().getCollectionName()))
                .append('remove', BsonBoolean.TRUE)

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
                .maxTime(10, TimeUnit.MILLISECONDS)

        expectedCommand.append('query', filter)
                .append('sort', sort)
                .append('fields', projection)
                .append('maxTimeMS', new BsonInt64(10))

        operation.execute(writeBinding)

        then:
        1 * connection.command(getNamespace().getDatabaseName(), expectedCommand, _, _, _) >> cannedResult
        1 * connection.release()

        where:
        serverVersion                | writeConcern                 | includeWriteConcern
        new ServerVersion([3, 2, 0]) | WriteConcern.W1              | true
        new ServerVersion([3, 2, 0]) | WriteConcern.ACKNOWLEDGED    | false
        new ServerVersion([3, 2, 0]) | WriteConcern.UNACKNOWLEDGED  | false
        new ServerVersion([3, 0, 0]) | WriteConcern.ACKNOWLEDGED    | false
        new ServerVersion([3, 0, 0]) | WriteConcern.UNACKNOWLEDGED  | false
        new ServerVersion([3, 0, 0]) | WriteConcern.W1              | false
    }

    def 'should create the expected command asynchronously'() {
        given:
        def cannedResult = new BsonDocument('value', new BsonDocumentWrapper(BsonDocument.parse('{}'), new BsonDocumentCodec()))
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
        def operation = new FindAndDeleteOperation<Document>(getNamespace(), writeConcern, documentCodec)
        def expectedCommand = new BsonDocument('findandmodify', new BsonString(getNamespace().getCollectionName()))
                .append('remove', BsonBoolean.TRUE)

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
                .maxTime(10, TimeUnit.MILLISECONDS)
        expectedCommand.append('query', filter)
                .append('sort', sort)
                .append('fields', projection)
                .append('maxTimeMS', new BsonInt64(10))

        operation.executeAsync(writeBinding, Stub(SingleResultCallback))

        then:
        1 * connection.commandAsync(getNamespace().getDatabaseName(), expectedCommand, _, _, _, _) >> { it[5].onResult(cannedResult, null) }
        1 * connection.release()

        where:
        serverVersion                | writeConcern                 | includeWriteConcern
        new ServerVersion([3, 2, 0]) | WriteConcern.W1              | true
        new ServerVersion([3, 2, 0]) | WriteConcern.ACKNOWLEDGED    | false
        new ServerVersion([3, 2, 0]) | WriteConcern.UNACKNOWLEDGED  | false
        new ServerVersion([3, 0, 0]) | WriteConcern.ACKNOWLEDGED    | false
        new ServerVersion([3, 0, 0]) | WriteConcern.UNACKNOWLEDGED  | false
        new ServerVersion([3, 0, 0]) | WriteConcern.W1              | false
    }

}
