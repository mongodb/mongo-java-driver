/*
 * Copyright (c) 2008-2016 MongoDB, Inc.
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
import com.mongodb.MongoExecutionTimeoutException
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
import com.mongodb.connection.AsyncConnection
import com.mongodb.connection.Connection
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ServerVersion
import org.bson.BsonArray
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonDouble
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.client.model.Filters.gte
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS

class AggregateToCollectionOperationSpecification extends OperationFunctionalSpecification {

    def aggregateCollectionNamespace = new MongoNamespace(getDatabaseName(), 'aggregateCollectionName')

    def setup() {
        CollectionHelper.drop(aggregateCollectionNamespace)
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)
    }

    def 'should have the correct defaults'() {
        given:
        def pipeline = [new BsonDocument('$out', new BsonString(aggregateCollectionNamespace.collectionName))]

        when:
        AggregateToCollectionOperation operation = new AggregateToCollectionOperation(getNamespace(), pipeline)

        then:
        operation.getAllowDiskUse() == null
        operation.getMaxTime(MILLISECONDS) == 0
        operation.getPipeline() == pipeline
        operation.getBypassDocumentValidation() == null
        operation.getWriteConcern() == null
    }

    def 'should set optional values correctly'(){
        given:
        def pipeline = [new BsonDocument('$out', new BsonString(aggregateCollectionNamespace.collectionName))]

        when:
        AggregateToCollectionOperation operation = new AggregateToCollectionOperation(getNamespace(), pipeline, WriteConcern.MAJORITY)
                .allowDiskUse(true)
                .maxTime(10, MILLISECONDS)
                .bypassDocumentValidation(true)

        then:
        operation.getAllowDiskUse()
        operation.getMaxTime(MILLISECONDS) == 10
        operation.getBypassDocumentValidation() == true
        operation.getWriteConcern() == WriteConcern.MAJORITY
    }

    def 'should not accept an empty pipeline'() {
        when:
        new AggregateToCollectionOperation(getNamespace(), [])


        then:
        thrown(IllegalArgumentException)
    }

    def 'should not accept a pipeline without the last stage specifying an output-collection'() {
        when:
        new AggregateToCollectionOperation(getNamespace(), [new BsonDocument('$match', new BsonDocument('job', new BsonString('plumber')))])


        then:
        thrown(IllegalArgumentException)
    }

    @IgnoreIf({ !serverVersionAtLeast([2, 6, 0]) })
    def 'should be able to output to a collection'() {
        when:
        AggregateToCollectionOperation operation =
                new AggregateToCollectionOperation(getNamespace(),
                                                   [new BsonDocument('$out', new BsonString(aggregateCollectionNamespace.collectionName))])
        operation.execute(getBinding());

        then:
        getCollectionHelper(aggregateCollectionNamespace).count() == 3
    }

    @Category(Async)
    @IgnoreIf({ !serverVersionAtLeast([2, 6, 0]) })
    def 'should be able to output to a collection asynchronously'() {
        when:
        AggregateToCollectionOperation operation =
                new AggregateToCollectionOperation(getNamespace(),
                                                   [new BsonDocument('$out', new BsonString(aggregateCollectionNamespace.collectionName))])
        executeAsync(operation);

        then:
        getCollectionHelper(aggregateCollectionNamespace).count() == 3
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should be able to match then output to a collection'() {
        when:
        AggregateToCollectionOperation operation =
                new AggregateToCollectionOperation(getNamespace(),
                                                   [new BsonDocument('$match', new BsonDocument('job', new BsonString('plumber'))),
                                                    new BsonDocument('$out', new BsonString(aggregateCollectionNamespace.collectionName))])
        operation.execute(getBinding());

        then:
        getCollectionHelper(aggregateCollectionNamespace).count() == 1
    }

    @Category(Async)
    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should be able to match then output to a collection asynchronously'() {
        when:
        AggregateToCollectionOperation operation =
                new AggregateToCollectionOperation(getNamespace(),
                                                   [new BsonDocument('$match', new BsonDocument('job', new BsonString('plumber'))),
                                                    new BsonDocument('$out', new BsonString(aggregateCollectionNamespace.collectionName))])
        executeAsync(operation);

        then:
        getCollectionHelper(aggregateCollectionNamespace).count() == 1
    }


    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should throw execution timeout exception from execute'() {
        given:
        AggregateToCollectionOperation operation =
                new AggregateToCollectionOperation(getNamespace(),
                                                   [new BsonDocument('$match', new BsonDocument('job', new BsonString('plumber'))),
                                                    new BsonDocument('$out', new BsonString(aggregateCollectionNamespace.collectionName))])
                        .maxTime(1, SECONDS)
        enableMaxTimeFailPoint()

        when:
        operation.execute(getBinding())

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }

    @Category(Async)
    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should throw execution timeout exception from executeAsync'() {
        given:
        AggregateToCollectionOperation operation =
                new AggregateToCollectionOperation(getNamespace(),
                                                   [new BsonDocument('$match', new BsonDocument('job', new BsonString('plumber'))),
                                                    new BsonDocument('$out', new BsonString(aggregateCollectionNamespace.collectionName))])
                        .maxTime(1, SECONDS)
        enableMaxTimeFailPoint()

        when:
        executeAsync(operation)

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 3, 8)) || !isDiscoverableReplicaSet() })
    def 'should throw on write concern error'() {
        given:
        AggregateToCollectionOperation operation =
                new AggregateToCollectionOperation(getNamespace(),
                        [new BsonDocument('$out', new BsonString(aggregateCollectionNamespace.collectionName))],
                        new WriteConcern(5))

        when:
        async ? executeAsync(operation) : operation.execute(getBinding())

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 100
        ex.writeResult.wasAcknowledged()

        where:
        async << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 1, 8)) })
    def 'should support bypassDocumentValidation'() {
        given:
        def collectionOutHelper = getCollectionHelper(new MongoNamespace(getDatabaseName(), 'collectionOut'))
        collectionOutHelper.create('collectionOut', new CreateCollectionOptions().validationOptions(
                new ValidationOptions().validator(gte('level', 10))))
        getCollectionHelper().insertDocuments(BsonDocument.parse('{ level: 9 }'))

        when:
        def operation = new AggregateToCollectionOperation(getNamespace(), [BsonDocument.parse('{$out: "collectionOut"}')])
        operation.execute(getBinding())

        then:
        thrown(MongoCommandException)

        when:
        operation.bypassDocumentValidation(false).execute(getBinding())

        then:
        thrown(MongoCommandException)

        when:
        operation.bypassDocumentValidation(true).execute(getBinding())

        then:
        notThrown(MongoCommandException)

        cleanup:
        collectionOutHelper?.drop()
    }

    @Category(Async)
    @IgnoreIf({ !serverVersionAtLeast(asList(3, 1, 8)) })
    def 'should support bypassDocumentValidation asynchronously'() {
        given:
        def collectionOutHelper = getCollectionHelper(new MongoNamespace(getDatabaseName(), 'collectionOut'))
        collectionOutHelper.create('collectionOut', new CreateCollectionOptions().validationOptions(
                new ValidationOptions().validator(gte('level', 10))))
        getCollectionHelper().insertDocuments(BsonDocument.parse('{ level: 9 }'))

        when:
        def operation = new AggregateToCollectionOperation(getNamespace(), [BsonDocument.parse('{$out: "collectionOut"}')])
        executeAsync(operation)

        then:
        thrown(MongoCommandException)

        when:
        executeAsync(operation.bypassDocumentValidation(false))

        then:
        thrown(MongoCommandException)

        when:
        executeAsync(operation.bypassDocumentValidation(true))

        then:
        notThrown(MongoCommandException)

        cleanup:
        collectionOutHelper?.drop()
    }

    def 'should create the expected command'() {
        given:
        def okReply = new BsonDocument('ok', new BsonDouble(1))
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

        def pipeline = [BsonDocument.parse('{$out: "collectionOut"}')]
        def expectedCommand = new BsonDocument('aggregate', new BsonString(getNamespace().getCollectionName()))
                .append('pipeline', new BsonArray(pipeline))
        if (includeWriteConcern) {
            expectedCommand.append('writeConcern', new BsonDocument('w', new BsonString('majority')))
        }

        when:
        def operation = new AggregateToCollectionOperation(getNamespace(), pipeline, WriteConcern.MAJORITY)
        operation.execute(writeBinding)

        then:
        1 * connection.command(getNamespace().getDatabaseName(), expectedCommand, _, _, _) >> okReply
        1 * connection.release()

        when:
        expectedCommand = expectedCommand.append('maxTimeMS', new BsonInt64(10))
                .append('allowDiskUse', new BsonBoolean(true))

        if (includeBypassValidation) {
            expectedCommand.append('bypassDocumentValidation', BsonBoolean.TRUE)
        }

        operation.allowDiskUse(true)
                .maxTime(10, MILLISECONDS)
                .bypassDocumentValidation(true)

        operation.execute(writeBinding)

        then:
        1 * connection.command(getNamespace().getDatabaseName(), expectedCommand, _, _, _) >> okReply
        1 * connection.release()

        where:
        serverVersion                   | includeBypassValidation | includeWriteConcern
        new ServerVersion([3, 4, 0])    | true                    | true
        new ServerVersion([3, 2, 0])    | true                    | false
        new ServerVersion([3, 0, 0])    | false                   | false
    }

    def 'should create the expected command asynchronously'() {
        given:
        def okReply = new BsonDocument('ok', new BsonDouble(1))
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
        def pipeline = [BsonDocument.parse('{$out: "collectionOut"}')]
        def expectedCommand = new BsonDocument('aggregate', new BsonString(getNamespace().getCollectionName()))
                .append('pipeline', new BsonArray(pipeline))

        if (includeWriteConcern) {
            expectedCommand.append('writeConcern', new BsonDocument('w', new BsonString('majority')))
        }

        when:
        def operation = new AggregateToCollectionOperation(getNamespace(), pipeline, WriteConcern.MAJORITY)
        operation.executeAsync(writeBinding, Stub(SingleResultCallback))

        then:
        1 * connection.commandAsync(getNamespace().getDatabaseName(), expectedCommand, _, _, _, _) >> { it[5].onResult(okReply, null) }
        1 * connection.release()

        when:
        operation.allowDiskUse(true)
                .maxTime(10, MILLISECONDS)
                .bypassDocumentValidation(true)

        expectedCommand.append('maxTimeMS', new BsonInt64(10))
                .append('allowDiskUse', new BsonBoolean(true))
        if (includeBypassValidation) {
            expectedCommand.put('bypassDocumentValidation', BsonBoolean.TRUE)
        }
        operation.executeAsync(writeBinding, Stub(SingleResultCallback))

        then:
        1 * connection.commandAsync(getNamespace().getDatabaseName(), expectedCommand, _, _, _, _) >> { it[5].onResult(okReply, null) }
        1 * connection.release()

        where:
        serverVersion                   | includeBypassValidation | includeWriteConcern
        new ServerVersion([3, 4, 0])    | true                    | true
        new ServerVersion([3, 2, 0])    | true                    | false
        new ServerVersion([3, 0, 0])    | false                   | false
    }

}
