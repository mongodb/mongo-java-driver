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
import com.mongodb.MongoExecutionTimeoutException
import com.mongodb.MongoNamespace
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.async.FutureResultCallback
import com.mongodb.async.SingleResultCallback
import com.mongodb.binding.AsyncConnectionSource
import com.mongodb.binding.AsyncReadBinding
import com.mongodb.binding.ConnectionSource
import com.mongodb.binding.ReadBinding
import com.mongodb.client.test.Worker
import com.mongodb.client.test.WorkerCodec
import com.mongodb.connection.AsyncConnection
import com.mongodb.connection.Connection
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ServerVersion
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonInvalidOperationException
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.Decoder
import org.bson.codecs.DocumentCodec
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.ValueCodecProvider
import org.bson.types.ObjectId
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS
import static org.bson.codecs.configuration.CodecRegistries.fromProviders

class DistinctOperationSpecification extends OperationFunctionalSpecification {

    def codecRegistry = fromProviders([new ValueCodecProvider(), new DocumentCodecProvider(), new BsonValueCodecProvider()])

    def getCodec(final Class clazz) {
        codecRegistry.get(clazz);
    }

    def stringDecoder = getCodec(String);

    def 'should have the correct defaults'() {
        when:
        DistinctOperation operation = new DistinctOperation(getNamespace(), 'name', stringDecoder)

        then:
        operation.getFilter() == null
        operation.getMaxTime(MILLISECONDS) == 0
        operation.getReadConcern() == ReadConcern.DEFAULT
    }

    def 'should set optional values correctly'(){
        given:
        def filter = new BsonDocument('filter', new BsonInt32(1))

        when:
        DistinctOperation operation = new DistinctOperation(getNamespace(), 'name', stringDecoder)
                .maxTime(10, MILLISECONDS)
                .filter(filter)
                .readConcern(ReadConcern.MAJORITY)

        then:
        operation.getFilter() == filter
        operation.getMaxTime(MILLISECONDS) == 10
        operation.getReadConcern() == ReadConcern.MAJORITY
    }

    def 'should be able to distinct by name'() {
        given:
        Document pete = new Document('name', 'Pete').append('age', 38)
        Document sam = new Document('name', 'Sam').append('age', 21)
        Document pete2 = new Document('name', 'Pete').append('age', 25)
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)

        when:
        DistinctOperation operation = new DistinctOperation(getNamespace(), 'name', stringDecoder)
        def result = operation.execute(getBinding()).next();

        then:
        result == ['Pete', 'Sam']
    }

    @Category(Async)
    def 'should be able to distinct by name asynchronously'() {
        given:
        Document pete = new Document('name', 'Pete').append('age', 38)
        Document sam = new Document('name', 'Sam').append('age', 21)
        Document pete2 = new Document('name', 'Pete').append('age', 25)
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)

        when:
        DistinctOperation operation = new DistinctOperation(getNamespace(), 'name', stringDecoder)
        def futureResult = new FutureResultCallback()
        executeAsync(operation).next(futureResult)
        def result = futureResult.get(60, SECONDS)

        then:
        result == ['Pete', 'Sam']
    }

    def 'should be able to distinct by name with find'() {
        given:
        Document pete = new Document('name', 'Pete').append('age', 38)
        Document sam = new Document('name', 'Sam').append('age', 21)
        Document pete2 = new Document('name', 'Pete').append('age', 25)
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)

        when:
        DistinctOperation operation = new DistinctOperation(getNamespace(), 'name', stringDecoder)
                .filter(new BsonDocument('age', new BsonInt32(25)))
        def result = operation.execute(getBinding());

        then:
        result.next() == ['Pete']
    }

    @Category(Async)
    def 'should be able to distinct by name with find asynchronously'() {
        given:
        Document pete = new Document('name', 'Pete').append('age', 38)
        Document sam = new Document('name', 'Sam').append('age', 21)
        Document pete2 = new Document('name', 'Pete').append('age', 25)
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)

        when:
        DistinctOperation operation = new DistinctOperation(getNamespace(), 'name', stringDecoder)
                .filter(new BsonDocument('age', new BsonInt32(25)))
        def futureResult = new FutureResultCallback()
        executeAsync(operation).next(futureResult)
        def result = futureResult.get(60, SECONDS)

        then:
        result == ['Pete']
    }

    def 'should be able to distinct with custom codecs'() {
        given:
        Worker pete = new Worker(new ObjectId(), 'Pete', 'handyman', new Date(), 3)
        Worker sam = new Worker(new ObjectId(), 'Sam', 'plumber', new Date(), 7)

        Document peteDocument = new Document('_id', pete.id)
                .append('name', pete.name)
                .append('jobTitle', pete.jobTitle)
                .append('dateStarted', pete.dateStarted)
                .append('numberOfJobs', pete.numberOfJobs)

        Document samDocument = new Document('_id', sam.id)
                .append('name', sam.name)
                .append('jobTitle', sam.jobTitle)
                .append('dateStarted', sam.dateStarted)
                .append('numberOfJobs', sam.numberOfJobs)

        getCollectionHelper().insertDocuments(new Document('worker', peteDocument), new Document('worker', samDocument));

        when:
        DistinctOperation operation = new DistinctOperation(getNamespace(), 'worker', new WorkerCodec())
        def result = operation.execute(getBinding()).next();

        then:
        result == [pete, sam]
    }

    @Category(Async)
    def 'should be able to distinct with custom codecs asynchronously'() {
        given:
        Worker pete = new Worker(new ObjectId(), 'Pete', 'handyman', new Date(), 3)
        Worker sam = new Worker(new ObjectId(), 'Sam', 'plumber', new Date(), 7)

        Document peteDocument = new Document('_id', pete.id)
                .append('name', pete.name)
                .append('jobTitle', pete.jobTitle)
                .append('dateStarted', pete.dateStarted)
                .append('numberOfJobs', pete.numberOfJobs)

        Document samDocument = new Document('_id', sam.id)
                .append('name', sam.name)
                .append('jobTitle', sam.jobTitle)
                .append('dateStarted', sam.dateStarted)
                .append('numberOfJobs', sam.numberOfJobs)

        getCollectionHelper().insertDocuments(new Document('worker', peteDocument), new Document('worker', samDocument));

        when:
        DistinctOperation operation = new DistinctOperation(getNamespace(), 'worker', new WorkerCodec())
        def futureResult = new FutureResultCallback()
        executeAsync(operation).next(futureResult)
        def result = futureResult.get(60, SECONDS)

        then:
        result == [pete, sam]
    }

    def 'should throw if invalid decoder passed to distinct'() {
        given:
        Document pete = new Document('name', 'Pete')
        Document sam = new Document('name', 1)
        Document pete2 = new Document('name', new Document('earle', 'Jones'))
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)

        when:
        DistinctOperation operation = new DistinctOperation(getNamespace(), 'name', stringDecoder)
        operation.execute(getBinding()).next();

        then:
        thrown(BsonInvalidOperationException)
    }

    @Category(Async)
    def 'should throw if invalid decoder passed to distinct asynchronously'() {
        given:
        Document pete = new Document('name', 'Pete')
        Document sam = new Document('name', 1)
        Document pete2 = new Document('name', new Document('earle', 'Jones'))
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)

        when:
        DistinctOperation operation = new DistinctOperation(getNamespace(), 'name', stringDecoder)
        def futureResult = new FutureResultCallback()
        executeAsync(operation).next(futureResult)
        futureResult.get(5, SECONDS)

        then:
        thrown(BsonInvalidOperationException)
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should throw execution timeout exception from execute'() {
        given:
        def operation = new DistinctOperation(getNamespace(), 'name', stringDecoder).maxTime(1, SECONDS)
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
        def operation = new DistinctOperation(getNamespace(), 'name', stringDecoder).maxTime(1, SECONDS)
        enableMaxTimeFailPoint()

        when:
        executeAsync(operation)

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }

    def 'should use the ReadBindings readPreference to set slaveOK'() {
        given:
        def connection = Mock(Connection) {
            _ * getDescription() >> helper.connectionDescription
        }
        def readBinding = Stub(ReadBinding) {
            getReadConnectionSource() >> Stub(ConnectionSource) {
                getConnection() >> connection
            }
            getReadPreference() >> readPreference
        }
        def operation = new DistinctOperation(helper.namespace, 'name', helper.decoder)

        when:
        operation.execute(readBinding)

        then:
        1 * connection.command(helper.dbName, _, readPreference.isSlaveOk(), _, _) >> helper.commandResult
        1 * connection.release()

        where:
        readPreference << [ReadPreference.primary(), ReadPreference.secondary()]
    }

    def 'should use the AsyncReadBindings readPreference to set slaveOK'() {
        given:
        def connection = Mock(AsyncConnection) {
            _ * getDescription() >> helper.connectionDescription
        }
        def connectionSource = Stub(AsyncConnectionSource) {
            getConnection(_) >> { it[0].onResult(connection, null) }
        }
        def readBinding = Stub(AsyncReadBinding) {
            getReadPreference() >> readPreference
            getReadConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
        }
        def operation = new DistinctOperation(helper.namespace, 'name', helper.decoder)

        when:
        operation.executeAsync(readBinding, Stub(SingleResultCallback))

        then:
        1 * connection.commandAsync(helper.dbName, _, readPreference.isSlaveOk(), _, _, _) >> { it[5].onResult(helper.commandResult, null) }
        1 * connection.release()

        where:
        readPreference << [ReadPreference.primary(), ReadPreference.secondary()]
    }

    def 'should create the expected command'() {
        given:
        def connection = Mock(Connection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getServerVersion() >> new ServerVersion([3, 2, 0])
            }
        }
        def connectionSource = Stub(ConnectionSource) {
            getConnection() >> connection
        }
        def readBinding = Stub(ReadBinding) {
            getReadConnectionSource() >> connectionSource
        }
        def operation = new DistinctOperation(helper.namespace, 'name', helper.decoder)
        def expectedCommand = new BsonDocument('distinct', new BsonString(helper.namespace.getCollectionName()))
                .append('key', new BsonString('name'))

        when:
        operation.execute(readBinding)

        then:
        1 * connection.command(helper.dbName, expectedCommand, _, _, _) >> {  helper.commandResult }
        1 * connection.release()

        when:
        operation.filter(new BsonDocument('a', BsonBoolean.TRUE))
                .maxTime(10, MILLISECONDS)
                .readConcern(ReadConcern.MAJORITY)

        expectedCommand.append('filter', operation.getFilter())
                .append('maxTimeMS', new BsonInt64(operation.getMaxTime(MILLISECONDS)))
                .append('readConcern', new BsonDocument('level', new BsonString('majority')))

        operation.execute(readBinding)

        then:
        1 * connection.command(helper.dbName, _, _, _, _) >> { helper.commandResult }
        1 * connection.release()
    }

    def 'should create the expected command asynchronously'() {
        given:
        def connection = Mock(AsyncConnection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getServerVersion() >> new ServerVersion([3, 2, 0])
            }
        }
        def connectionSource = Stub(AsyncConnectionSource) {
            getConnection(_) >> { it[0].onResult(connection, null) }
        }
        def readBinding = Stub(AsyncReadBinding) {
            getReadConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
        }
        def operation = new DistinctOperation(helper.namespace, 'name', helper.decoder)
        def expectedCommand = new BsonDocument('distinct', new BsonString(helper.namespace.getCollectionName()))
                .append('key', new BsonString('name'))

        when:
        operation.executeAsync(readBinding, Stub(SingleResultCallback))

        then:
        1 * connection.commandAsync(helper.dbName, expectedCommand, _, _, _, _) >> { it[5].onResult(helper.commandResult, null) }
        1 * connection.release()

        when:
        operation.filter(new BsonDocument('a', BsonBoolean.TRUE))
                .maxTime(10, MILLISECONDS)
                .readConcern(ReadConcern.MAJORITY)

        expectedCommand.append('filter', operation.getFilter())
                .append('maxTimeMS', new BsonInt64(operation.getMaxTime(MILLISECONDS)))
                .append('readConcern', new BsonDocument('level', new BsonString('majority')))

        operation.executeAsync(readBinding, Stub(SingleResultCallback))

        then:
        1 * connection.commandAsync(helper.dbName, _, _, _, _, _) >> { it[5].onResult(helper.commandResult, null) }
        1 * connection.release()
    }

    def 'should validate the ReadConcern'() {
        given:
        def readBinding = Stub(ReadBinding) {
            getReadConnectionSource() >> Stub(ConnectionSource) {
                getConnection() >> Stub(Connection) {
                    getDescription() >> Stub(ConnectionDescription) {
                        getServerVersion() >> new ServerVersion([3, 0, 0])
                    }
                }
            }
        }
        def operation = new DistinctOperation(helper.namespace, 'name', helper.decoder).readConcern(readConcern)

        when:
        operation.execute(readBinding)

        then:
        thrown(IllegalArgumentException)

        where:
        readConcern << [ReadConcern.MAJORITY, ReadConcern.LOCAL]
    }

    def 'should validate the ReadConcern asynchronously'() {
        given:
        def connection = Stub(AsyncConnection) {
            getDescription() >>  Stub(ConnectionDescription) {
                getServerVersion() >> new ServerVersion([3, 0, 0])
            }
        }
        def connectionSource = Stub(AsyncConnectionSource) {
            getConnection(_) >> { it[0].onResult(connection, null) }
        }
        def readBinding = Stub(AsyncReadBinding) {
            getReadConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
        }
        def operation = new DistinctOperation(helper.namespace, 'name', helper.decoder).readConcern(readConcern)
        def callback = Mock(SingleResultCallback)

        when:
        operation.executeAsync(readBinding, callback)

        then:
        1 * callback.onResult(null, _ as IllegalArgumentException)

        where:
        readConcern << [ReadConcern.MAJORITY, ReadConcern.LOCAL]
    }

    def helper = [
        dbName: 'db',
        namespace: new MongoNamespace('db', 'coll'),
        decoder: Stub(Decoder),
        commandResult: BsonDocument.parse('{ok: 1.0}').append('values', new BsonArrayWrapper([])),
        connectionDescription: Stub(ConnectionDescription)
    ]
}
