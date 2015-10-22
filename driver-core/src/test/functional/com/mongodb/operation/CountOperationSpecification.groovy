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
import com.mongodb.ExplainVerbosity
import com.mongodb.MongoException
import com.mongodb.MongoExecutionTimeoutException
import com.mongodb.MongoNamespace
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.async.SingleResultCallback
import com.mongodb.binding.AsyncConnectionSource
import com.mongodb.binding.AsyncReadBinding
import com.mongodb.binding.ConnectionSource
import com.mongodb.binding.ReadBinding
import com.mongodb.bulk.IndexRequest
import com.mongodb.connection.AsyncConnection
import com.mongodb.connection.Connection
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ServerVersion
import org.bson.BsonDocument
import org.bson.BsonInt32
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
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS

class CountOperationSpecification extends OperationFunctionalSpecification {

    private documents;

    def setup() {
        documents = [
                new Document('x', 1),
                new Document('x', 2),
                new Document('x', 3),
                new Document('x', 4),
                new Document('x', 5)
        ]
        getCollectionHelper().insertDocuments(new DocumentCodec(), documents)
    }

    def 'should have the correct defaults'() {
        when:
        CountOperation operation = new CountOperation(getNamespace())

        then:
        operation.getFilter() == null
        operation.getMaxTime(MILLISECONDS) == 0
        operation.getHint() == null
        operation.getLimit() == 0
        operation.getSkip() == 0
        operation.getReadConcern() == ReadConcern.DEFAULT
    }

    def 'should set optional values correctly'(){
        given:
        def filter = new BsonDocument('filter', new BsonInt32(1))
        def hint = new BsonString('hint')

        when:
        CountOperation operation = new CountOperation(getNamespace())
                .maxTime(10, MILLISECONDS)
                .filter(filter)
                .hint(hint)
                .limit(20)
                .skip(30)
                .readConcern(ReadConcern.MAJORITY)

        then:
        operation.getFilter() == filter
        operation.getMaxTime(MILLISECONDS) == 10
        operation.getHint() == hint
        operation.getLimit() == 20
        operation.getSkip() == 30
        operation.getReadConcern() == ReadConcern.MAJORITY
    }

    def 'should get the count'() {
        expect:
        new CountOperation(getNamespace()).execute(getBinding()) == documents.size()
    }

    @Category(Async)
    def 'should get the count asynchronously'() {
        expect:
        executeAsync(new CountOperation(getNamespace())) ==
        documents.size()
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should throw execution timeout exception from execute'() {
        given:
        def countOperation = new CountOperation(getNamespace())
                .maxTime(1, SECONDS)
        enableMaxTimeFailPoint()

        when:
        countOperation.execute(getBinding())

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }

    @Category(Async)
    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should throw execution timeout exception from executeAsync'() {
        given:
        def countOperation = new CountOperation(getNamespace())
                .maxTime(1, SECONDS)
        enableMaxTimeFailPoint()

        when:
        executeAsync(countOperation)

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }

    def 'should use limit with the count'() {
        when:
        def countOperation = new CountOperation(getNamespace())
                .limit(1)
        then:
        countOperation.execute(getBinding()) == 1
    }

    @Category(Async)
    def 'should use limit with the count asynchronously'() {
        when:
        def countOperation = new CountOperation(getNamespace())
                .limit(1)

        then:
        executeAsync(countOperation) == 1
    }

    def 'should use skip with the count'() {
        when:
        def countOperation = new CountOperation(getNamespace()).skip(documents.size() - 2)

        then:
        countOperation.execute(getBinding()) == 2
    }

    @Category(Async)
    def 'should use skip with the count asynchronously'() {
        when:
        def countOperation = new CountOperation(getNamespace()).skip(documents.size() - 2)

        then:
        executeAsync(countOperation)  == 2
    }

    def 'should use hint with the count'() {
        given:
        def createIndexOperation = new CreateIndexesOperation(getNamespace(),
                                                              [new IndexRequest(new BsonDocument('x', new BsonInt32(1))).sparse(true)])
        def countOperation = new CountOperation(getNamespace()).hint(new BsonString('x_1'))

        when:
        createIndexOperation.execute(getBinding())

        then:
        countOperation.execute(getBinding()) == serverVersionAtLeast(asList(2, 6, 0)) ? 1 : documents.size()
    }

    @Category(Async)
    def 'should use hint with the count asynchronously'() {
        given:
        def createIndexOperation = new CreateIndexesOperation(getNamespace(),
                                                              [new IndexRequest(new BsonDocument('x', new BsonInt32(1))).sparse(true)])
        def countOperation = new CountOperation(getNamespace()).hint(new BsonString('x_1'))

        when:
        executeAsync(createIndexOperation)

        then:
        executeAsync(countOperation) == serverVersionAtLeast(asList(2, 6, 0)) ? 1 : documents.size()
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should throw with bad hint with mongod 2.6+'() {
        given:
        def countOperation = new CountOperation(getNamespace())
                .filter(new BsonDocument('a', new BsonInt32(1)))
                .hint(new BsonString('BAD HINT'))

        when:
        countOperation.execute(getBinding())

        then:
        thrown(MongoException)
    }

    @Category(Async)
    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should throw with bad hint with mongod 2.6+ asynchronously'() {
        given:
        def countOperation = new CountOperation(getNamespace())
                .filter(new BsonDocument('a', new BsonInt32(1)))
                .hint(new BsonString('BAD HINT'))

        when:
        executeAsync(countOperation)

        then:
        thrown(MongoException)
    }

    @IgnoreIf({ serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should ignore with bad hint with mongod < 2.6'() {
        given:
        def countOperation = new CountOperation(getNamespace())
                .filter(new BsonDocument('a', new BsonInt32(1)))
                .hint(new BsonString('BAD HINT'))

        when:
        countOperation.execute(getBinding())

        then:
        notThrown(MongoException)
    }

    @Category(Async)
    @IgnoreIf({ serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should ignore with bad hint with mongod < 2.6 asynchronously'() {
        given:
        def countOperation = new CountOperation(getNamespace())
                .filter(new BsonDocument('a', new BsonInt32(1)))
                .hint(new BsonString('BAD HINT'))

        when:
        executeAsync(countOperation)

        then:
        notThrown(MongoException)
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 0, 0)) || isSharded() })
    def 'should explain'() {
        given:
        def countOperation = new CountOperation(getNamespace())
                .filter(new BsonDocument('a', new BsonInt32(1)))

        when:
        BsonDocument result = countOperation.asExplainableOperation(ExplainVerbosity.QUERY_PLANNER).execute(getBinding())

        then:
        result.getNumber('ok').intValue() == 1
    }

    @Category(Async)
    @IgnoreIf({ !serverVersionAtLeast(asList(3, 0, 0)) || isSharded() })
    def 'should explain asynchronously'() {
        given:
        def countOperation = new CountOperation(getNamespace())
                .filter(new BsonDocument('a', new BsonInt32(1)))

        when:
        BsonDocument result = executeAsync(countOperation.asExplainableOperationAsync(ExplainVerbosity.QUERY_PLANNER))

        then:
        result.getNumber('ok').intValue() == 1
    }

    def 'should use the ReadBindings readPreference to set slaveOK'() {
        given:
        def connection = Mock(Connection)
        def connectionSource = Stub(ConnectionSource) {
            getConnection() >> connection
        }
        def readBinding = Stub(ReadBinding) {
            getReadConnectionSource() >> connectionSource
            getReadPreference() >> readPreference
        }
        def operation = new CountOperation(helper.namespace).filter(BsonDocument.parse('{a: 1}'))

        when:
        operation.execute(readBinding)

        then:
        _ * connection.getDescription() >> helper.connectionDescription
        1 * connection.command(helper.dbName, _, readPreference.isSlaveOk(), _, _) >> helper.commandResult
        1 * connection.release()

        where:
        readPreference << [ReadPreference.primary(), ReadPreference.secondary()]
    }

    def 'should use the AsyncReadBindings readPreference to set slaveOK'() {
        given:
        def connection = Mock(AsyncConnection)
        def connectionSource = Stub(AsyncConnectionSource) {
            getConnection(_) >> { it[0].onResult(connection, null) }
        }
        def readBinding = Stub(AsyncReadBinding) {
            getReadPreference() >> readPreference
            getReadConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
        }
        def operation = new CountOperation(helper.namespace).filter(BsonDocument.parse('{a: 1}'))

        when:
        operation.executeAsync(readBinding, Stub(SingleResultCallback))

        then:
        _ * connection.getDescription() >> helper.connectionDescription
        1 * connection.commandAsync(helper.dbName, _, readPreference.isSlaveOk(), _, _, _) >> { it[5].onResult(helper.commandResult, null) }
        1 * connection.release()

        where:
        readPreference << [ReadPreference.primary(), ReadPreference.secondary()]
    }

    def 'should create the expected command'() {
        given:
        def filter = new BsonDocument('filter', new BsonInt32(1))
        def hint = new BsonString('hint')
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
        def operation = new CountOperation(helper.namespace)
        def expectedCommand = new BsonDocument('count', new BsonString(helper.namespace.getCollectionName()))
        when:
        operation.execute(readBinding)

        then:
        1 * connection.command(helper.dbName, expectedCommand, _, _, _) >> { helper.commandResult }
        1 * connection.release()

        when:
        operation.filter(filter)
                .limit(20)
                .skip(30)
                .hint(hint)
                .maxTime(10, MILLISECONDS)
                .readConcern(ReadConcern.MAJORITY)

         expectedCommand.append('query', filter)
                .append('limit', new BsonInt64(20))
                .append('skip', new BsonInt64(30))
                .append('hint', hint)
                .append('maxTimeMS', new BsonInt64(10))
                .append('readConcern', new BsonDocument('level', new BsonString('majority')))

        operation.execute(readBinding)

        then:
        1 * connection.command(helper.dbName, expectedCommand, _, _, _) >> { helper.commandResult }
        1 * connection.release()
    }

    def 'should create the expected command asynchronously'() {
        given:
        def filter = new BsonDocument('filter', new BsonInt32(1))
        def hint = new BsonString('hint')
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
        def operation = new CountOperation(helper.namespace)
        def expectedCommand = new BsonDocument('count', new BsonString(helper.namespace.getCollectionName()))

        when:
        operation.executeAsync(readBinding, Stub(SingleResultCallback))

        then:
        1 * connection.commandAsync(helper.dbName, expectedCommand, _, _, _, _) >> { it[5].onResult(helper.commandResult, null) }
        1 * connection.release()

        when:
        operation.filter(filter)
                .limit(20)
                .skip(30)
                .hint(hint)
                .maxTime(10, MILLISECONDS)
                .readConcern(ReadConcern.MAJORITY)

        expectedCommand.append('query', filter)
                .append('limit', new BsonInt64(20))
                .append('skip', new BsonInt64(30))
                .append('hint', hint)
                .append('maxTimeMS', new BsonInt64(10))
                .append('readConcern', new BsonDocument('level', new BsonString('majority')))

        operation.executeAsync(readBinding, Stub(SingleResultCallback))

        then:
        1 * connection.commandAsync(helper.dbName, expectedCommand, _, _, _, _) >> { it[5].onResult(helper.commandResult, null) }
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
        def operation = new CountOperation(helper.namespace).readConcern(readConcern)

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
        def operation = new CountOperation(helper.namespace).readConcern(readConcern)
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
        commandResult: BsonDocument.parse('{ok: 1.0, n: 10}'),
        connectionDescription: Stub(ConnectionDescription)
    ]
}
