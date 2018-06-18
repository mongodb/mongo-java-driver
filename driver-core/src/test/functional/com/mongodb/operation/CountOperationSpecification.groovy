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

package com.mongodb.operation

import com.mongodb.ExplainVerbosity
import com.mongodb.MongoException
import com.mongodb.MongoExecutionTimeoutException
import com.mongodb.MongoNamespace
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.binding.AsyncConnectionSource
import com.mongodb.binding.AsyncReadBinding
import com.mongodb.binding.ConnectionSource
import com.mongodb.binding.ReadBinding
import com.mongodb.bulk.IndexRequest
import com.mongodb.internal.client.model.CountStrategy
import com.mongodb.connection.AsyncConnection
import com.mongodb.connection.ClusterId
import com.mongodb.connection.Connection
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ConnectionId
import com.mongodb.connection.ServerId
import com.mongodb.connection.ServerVersion
import com.mongodb.session.SessionContext
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.BsonTimestamp
import org.bson.Document
import org.bson.codecs.DocumentCodec
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.connection.ServerType.STANDALONE
import static com.mongodb.operation.OperationReadConcernHelper.appendReadConcernToCommand
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS

class CountOperationSpecification extends OperationFunctionalSpecification {

    private documents

    def setup() {
        documents = [
                new Document('x', 1),
                new Document('x', 2),
                new Document('x', 3),
                new Document('x', 4),
                new Document('x', 5).append('y', 1)
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

        then:
        operation.getFilter() == filter
        operation.getMaxTime(MILLISECONDS) == 10
        operation.getHint() == hint
        operation.getLimit() == 20
        operation.getSkip() == 30
    }

    def 'should get the count'() {
        expect:
        execute(new CountOperation(getNamespace(), strategy), async) == documents.size()

        where:
        [async, strategy] << [[true, false], [CountStrategy.AGGREGATE, CountStrategy.COMMAND]].combinations()
    }

    def 'should return 0 if no collection'() {
        when:
        getCollectionHelper().drop()

        then:
        execute(new CountOperation(getNamespace(), strategy), async) == 0

        where:
        [async, strategy] << [[true, false], [CountStrategy.AGGREGATE, CountStrategy.COMMAND]].combinations()
    }

    def 'should return 0 if empty collection'() {
        when:
        getCollectionHelper().drop()
        getCollectionHelper().create()

        then:
        execute(new CountOperation(getNamespace(), strategy), async) == 0

        where:
        [async, strategy] << [[true, false], [CountStrategy.AGGREGATE, CountStrategy.COMMAND]].combinations()
    }

    def 'should throw execution timeout exception from execute'() {
        given:
        def operation = new CountOperation(getNamespace(), strategy).maxTime(1, SECONDS)
        enableMaxTimeFailPoint()

        when:
        execute(operation, async)

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()

        where:
        [async, strategy] << [[true, false], [CountStrategy.AGGREGATE, CountStrategy.COMMAND]].combinations()
    }

    def 'should use limit with the count'() {
        when:
        def operation = new CountOperation(getNamespace(), strategy).limit(1)

        then:
        execute(operation, async) == 1

        where:
        [async, strategy] << [[true, false], [CountStrategy.AGGREGATE, CountStrategy.COMMAND]].combinations()
    }

    def 'should use skip with the count'() {
        when:
        def operation = new CountOperation(getNamespace(), strategy).skip(documents.size() - 2)

        then:
        execute(operation, async)

        where:
        [async, strategy] << [[true, false], [CountStrategy.AGGREGATE, CountStrategy.COMMAND]].combinations()
    }

    def 'should use hint with the count'() {
        given:
        def indexDefinition = new BsonDocument('y', new BsonInt32(1))
        new CreateIndexesOperation(getNamespace(), [new IndexRequest(indexDefinition).sparse(true)])
                .execute(getBinding())
        def operation = new CountOperation(getNamespace(), strategy).hint(indexDefinition)

        when:
        def count = execute(operation, async)

        then:
        count == (serverVersionAtLeast(3, 4) ? 1 : documents.size())

        where:
        [async, strategy] << [[true, false], [CountStrategy.AGGREGATE, CountStrategy.COMMAND]].combinations()
    }

    def 'should throw with bad hint with mongod 2.6+'() {
        given:
        def operation = new CountOperation(getNamespace(), strategy)
                .filter(new BsonDocument('a', new BsonInt32(1)))
                .hint(new BsonString('BAD HINT'))

        when:
        execute(operation, async)

        then:
        thrown(MongoException)

        where:
        [async, strategy] << [[true, false], [CountStrategy.AGGREGATE, CountStrategy.COMMAND]].combinations()
    }

    def 'should support hints that are bson documents or strings'() {
        expect:
        execute(new CountOperation(getNamespace(), strategy).hint(hint), async) == 5

        where:
        [async, strategy, hint] << [[true, false], [CountStrategy.AGGREGATE, CountStrategy.COMMAND],
                                    [new BsonString('_id_'), BsonDocument.parse('{_id: 1}')]].combinations()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 0) || isSharded() })
    def 'should explain'() {
        when:
        def operation = new CountOperation(getNamespace(), strategy)
                .filter(new BsonDocument('a', new BsonInt32(1)))
                .asExplainableOperation(ExplainVerbosity.QUERY_PLANNER)

        then:
        execute(operation, async).getNumber('ok').intValue() == 1

        where:
        [async, strategy] << [[true, false], [CountStrategy.AGGREGATE, CountStrategy.COMMAND]].combinations()
    }


    def 'should use the ReadBindings readPreference to set slaveOK'() {
        when:
        def operation = new CountOperation(helper.namespace, CountStrategy.COMMAND).filter(BsonDocument.parse('{a: 1}'))

        then:
        testOperationSlaveOk(operation, [3, 4, 0], readPreference, async, helper.commandResult)

        where:
        [async, readPreference] << [[true, false], [ReadPreference.primary(), ReadPreference.secondary()]].combinations()
    }

    def 'should create the expected command'() {
        when:
        def filter = new BsonDocument('filter', new BsonInt32(1))
        def hint = new BsonDocument('hint', new BsonInt32(1))
        def operation = new CountOperation(helper.namespace, CountStrategy.COMMAND)
        def expectedCommand = new BsonDocument('count', new BsonString(helper.namespace.getCollectionName()))

        then:
        testOperation(operation, [3, 4, 0], expectedCommand, async, helper.commandResult)

        when:
        operation.filter(filter)
                .limit(20)
                .skip(30)
                .hint(hint)
                .maxTime(10, MILLISECONDS)
                .collation(defaultCollation)

         expectedCommand.append('query', filter)
                .append('limit', new BsonInt64(20))
                .append('skip', new BsonInt64(30))
                .append('hint', hint)
                .append('maxTimeMS', new BsonInt64(10))
                .append('collation', defaultCollation.asDocument())

        then:
        testOperation(operation, [3, 4, 0], expectedCommand, async, helper.commandResult)

        where:
        async << [true, false]
    }

    def 'should create the expected aggregation command'() {
        when:
        def filter = new BsonDocument('filter', new BsonInt32(1))
        def operation = new CountOperation(helper.namespace, CountStrategy.AGGREGATE)
        def pipeline = [BsonDocument.parse('{ $match: {}}'), BsonDocument.parse('{$group: {_id: null, n: {$sum: 1}}}')]
        def expectedCommand = new BsonDocument('aggregate', new BsonString(helper.namespace.getCollectionName()))
                .append('pipeline', new BsonArray(pipeline))
                .append('cursor', new BsonDocument())

        then:
        testOperation(operation, [3, 4, 0], expectedCommand, async, helper.cursorResult)

        when:
        operation.filter(filter)
                .limit(20)
                .skip(30)
                .hint(hint)
                .maxTime(10, MILLISECONDS)
                .collation(defaultCollation)

        expectedCommand = expectedCommand
                .append('pipeline', new BsonArray([new BsonDocument('$match', filter),
                                                   new BsonDocument('$skip', new BsonInt64(30)),
                                                   new BsonDocument('$limit', new BsonInt64(20)),
                                                   pipeline.last()]))
                .append('maxTimeMS', new BsonInt64(10))
                .append('collation', defaultCollation.asDocument())
                .append('hint', hint)

        then:
        testOperation(operation, [3, 4, 0], expectedCommand, async, helper.cursorResult)

        where:
        [async, hint] << [[true, false], [new BsonString('hint_1'), BsonDocument.parse('{hint: 1}')]].combinations()
    }

    def 'should throw an exception when using an unsupported ReadConcern'() {
        given:
        def operation = new CountOperation(helper.namespace, strategy)

        when:
        testOperationThrows(operation, [3, 0, 0], readConcern, async)

        then:
        def exception = thrown(IllegalArgumentException)
        exception.getMessage().startsWith('ReadConcern not supported by server version:')

        where:
        [async, strategy, readConcern] << [[true, false], [CountStrategy.AGGREGATE, CountStrategy.COMMAND],
                                           [ReadConcern.MAJORITY, ReadConcern.LOCAL]].combinations()
    }

    def 'should throw an exception when using an unsupported Collation'() {
        given:
        def operation = new CountOperation(helper.namespace, strategy).collation(defaultCollation)

        when:
        testOperationThrows(operation, [3, 2, 0], async)

        then:
        def exception = thrown(IllegalArgumentException)
        exception.getMessage().startsWith('Collation not supported by server version:')

        where:
        [async, strategy] << [[true, false], [CountStrategy.AGGREGATE, CountStrategy.COMMAND]].combinations()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) })
    def 'should support collation'() {
        given:
        getCollectionHelper().insertDocuments(BsonDocument.parse('{str: "foo"}'))
        def operation = new CountOperation(namespace, strategy).filter(BsonDocument.parse('{str: "FOO"}'))
                .collation(caseInsensitiveCollation)

        when:
        def result = execute(operation, async)

        then:
        result == 1

        where:
        [async, strategy] << [[true, false], [CountStrategy.AGGREGATE, CountStrategy.COMMAND]].combinations()
    }

    def 'should add read concern to command'() {
        given:
        def binding = Stub(ReadBinding)
        def source = Stub(ConnectionSource)
        def connection = Mock(Connection)
        binding.readPreference >> ReadPreference.primary()
        binding.readConnectionSource >> source
        binding.sessionContext >> sessionContext
        source.connection >> connection
        source.retain() >> source
        def commandDocument = new BsonDocument('count', new BsonString(getCollectionName()))
        appendReadConcernToCommand(sessionContext, commandDocument)

        def operation = new CountOperation(getNamespace())

        when:
        operation.execute(binding)

        then:
        _ * connection.description >> new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
                new ServerVersion(3, 6), STANDALONE, 1000, 100000, 100000, [])
        1 * connection.command(_, commandDocument, _, _, _, sessionContext) >>
                new BsonDocument('n', new BsonInt64(42))
        1 * connection.release()

        where:
        sessionContext << [
                Stub(SessionContext) {
                    isCausallyConsistent() >> true
                    getOperationTime() >> new BsonTimestamp(42, 0)
                    hasActiveTransaction() >> false
                    getReadConcern() >> ReadConcern.MAJORITY
                }
        ]
    }

    def 'should add read concern to command asynchronously'() {
        given:
        def binding = Stub(AsyncReadBinding)
        def source = Stub(AsyncConnectionSource)
        def connection = Mock(AsyncConnection)
        binding.readPreference >> ReadPreference.primary()
        binding.getReadConnectionSource(_) >> { it[0].onResult(source, null) }
        binding.sessionContext >> sessionContext
        source.getConnection(_) >> { it[0].onResult(connection, null) }
        source.retain() >> source
        def commandDocument = new BsonDocument('count', new BsonString(getCollectionName()))
        appendReadConcernToCommand(sessionContext, commandDocument)

        def operation = new CountOperation(getNamespace())

        when:
        executeAsync(operation, binding)

        then:
        _ * connection.description >> new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
                new ServerVersion(3, 6), STANDALONE, 1000, 100000, 100000, [])
        1 * connection.commandAsync(_, commandDocument, _, _, _, sessionContext, _) >> {
            it[6].onResult(new BsonDocument('n', new BsonInt64(42)), null)
        }
        1 * connection.release()

        where:
        sessionContext << [
                Stub(SessionContext) {
                    isCausallyConsistent() >> true
                    getOperationTime() >> new BsonTimestamp(42, 0)
                    hasActiveTransaction() >> false
                    getReadConcern() >> ReadConcern.MAJORITY
                }
        ]
    }
    def helper = [
        dbName: 'db',
        namespace: new MongoNamespace('db', 'coll'),
        commandResult: BsonDocument.parse('{ok: 1.0, n: 10}'),
        cursorResult: BsonDocument.parse('{ok: 1.0}')
                .append('cursor', new BsonDocument('id', new BsonInt64(0)).append('ns', new BsonString('db.coll'))
                .append('firstBatch', new BsonArrayWrapper([ BsonDocument.parse('{n: 10}') ]))),
        connectionDescription: Stub(ConnectionDescription)
    ]
}
