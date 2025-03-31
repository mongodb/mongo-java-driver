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

import com.mongodb.MongoException
import com.mongodb.MongoNamespace
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ConnectionId
import com.mongodb.connection.ServerId
import com.mongodb.internal.binding.AsyncConnectionSource
import com.mongodb.internal.binding.AsyncReadBinding
import com.mongodb.internal.binding.ConnectionSource
import com.mongodb.internal.binding.ReadBinding
import com.mongodb.internal.bulk.IndexRequest
import com.mongodb.internal.connection.AsyncConnection
import com.mongodb.internal.connection.Connection
import com.mongodb.internal.session.SessionContext
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.BsonTimestamp
import org.bson.Document
import org.bson.codecs.DocumentCodec
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT
import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.connection.ServerType.STANDALONE
import static com.mongodb.internal.operation.OperationReadConcernHelper.appendReadConcernToCommand
import static com.mongodb.internal.operation.ServerVersionHelper.UNKNOWN_WIRE_VERSION

class CountDocumentsOperationSpecification extends OperationFunctionalSpecification {

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
        CountDocumentsOperation operation = new CountDocumentsOperation(getNamespace())

        then:
        operation.getFilter() == null
        operation.getHint() == null
        operation.getLimit() == 0
        operation.getSkip() == 0
    }

    def 'should set optional values correctly'() {
        given:
        def filter = new BsonDocument('filter', new BsonInt32(1))
        def hint = new BsonString('hint')

        when:
        CountDocumentsOperation operation = new CountDocumentsOperation(getNamespace())
                .filter(filter)
                .hint(hint)
                .limit(20)
                .skip(30)

        then:
        operation.getFilter() == filter
        operation.getHint() == hint
        operation.getLimit() == 20
        operation.getSkip() == 30
    }

    def 'should get the count'() {
        expect:
        execute(new CountDocumentsOperation(getNamespace()), async) == documents.size()

        where:
        async << [true, false]
    }

    def 'should return 0 if no collection'() {
        when:
        getCollectionHelper().drop()

        then:
        execute(new CountDocumentsOperation(getNamespace()), async) == 0

        where:
        async << [true, false]
    }

    def 'should return 0 if empty collection'() {
        when:
        getCollectionHelper().drop()
        getCollectionHelper().create()

        then:
        execute(new CountDocumentsOperation(getNamespace()), async) == 0

        where:
        async << [true, false]
    }

    def 'should use limit with the count'() {
        when:
        def operation = new CountDocumentsOperation(getNamespace()).limit(1)

        then:
        execute(operation, async) == 1

        where:
        async << [true, false]
    }

    def 'should use skip with the count'() {
        when:
        def operation = new CountDocumentsOperation(getNamespace()).skip(documents.size() - 2)

        then:
        execute(operation, async)

        where:
        async << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 6) })
    def 'should use hint with the count'() {
        given:
        def indexDefinition = new BsonDocument('y', new BsonInt32(1))
        new CreateIndexesOperation(getNamespace(), [new IndexRequest(indexDefinition).sparse(true)], null)
                .execute(getBinding())
        def operation = new CountDocumentsOperation(getNamespace()).hint(indexDefinition)

        when:
        def count = execute(operation, async)

        then:
        count == 1

        where:
        async << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 6) })
    def 'should support hints that are bson documents or strings'() {
        expect:
        execute(new CountDocumentsOperation(getNamespace()).hint(hint), async) == 5

        where:
        [async, hint] << [[true, false], [new BsonString('_id_'), BsonDocument.parse('{_id: 1}')]].combinations()
    }

    def 'should throw with bad hint'() {
        given:
        def operation = new CountDocumentsOperation(getNamespace())
                .filter(new BsonDocument('a', new BsonInt32(1)))
                .hint(new BsonString('BAD HINT'))

        when:
        execute(operation, async)

        then:
        thrown(MongoException)

        where:
        async << [true, false]
    }

    def 'should use the ReadBindings readPreference to set secondaryOk'() {
        when:
        def operation = new CountDocumentsOperation(helper.namespace)
                .filter(BsonDocument.parse('{a: 1}'))

        then:
        testOperationSecondaryOk(operation, [3, 4, 0], readPreference, async, helper.cursorResult)

        where:
        [async, readPreference] << [[true, false], [ReadPreference.primary(), ReadPreference.secondary()]].combinations()
    }

    def 'should create the expected aggregation command'() {
        when:
        def filter = new BsonDocument('filter', new BsonInt32(1))
        def operation = new CountDocumentsOperation(helper.namespace)
        def pipeline = [BsonDocument.parse('{ $match: {}}'), BsonDocument.parse('{$group: {_id: 1, n: {$sum: 1}}}')]
        def expectedCommand = new BsonDocument('aggregate', new BsonString(helper.namespace.getCollectionName()))
                .append('pipeline', new BsonArray(pipeline))
                .append('cursor', new BsonDocument())

        then:
        testOperation(operation, [3, 4, 0], expectedCommand, async, helper.cursorResult)

        when:
        operation = new CountDocumentsOperation(helper.namespace)
                .filter(filter)
                .limit(20)
                .skip(30)
                .hint(hint)
                .collation(defaultCollation)

        expectedCommand = expectedCommand
                .append('pipeline', new BsonArray([new BsonDocument('$match', filter),
                                                   new BsonDocument('$skip', new BsonInt64(30)),
                                                   new BsonDocument('$limit', new BsonInt64(20)),
                                                   pipeline.last()]))
                .append('collation', defaultCollation.asDocument())
                .append('hint', hint)

        then:
        testOperation(operation, [3, 4, 0], expectedCommand, async, helper.cursorResult)

        where:
        [async, hint] << [[true, false], [new BsonString('hint_1'), BsonDocument.parse('{hint: 1}')]].combinations()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) })
    def 'should support collation'() {
        given:
        getCollectionHelper().insertDocuments(BsonDocument.parse('{str: "foo"}'))
        def operation = new CountDocumentsOperation(namespace)
                .filter(BsonDocument.parse('{str: "FOO"}'))
                .collation(caseInsensitiveCollation)

        when:
        def result = execute(operation, async)

        then:
        result == 1

        where:
        async << [true, false]
    }

    def 'should add read concern to command'() {
        given:
        def operationContext = OPERATION_CONTEXT.withSessionContext(sessionContext)
        def binding = Stub(ReadBinding)
        def source = Stub(ConnectionSource)
        def connection = Mock(Connection)
        binding.readPreference >> ReadPreference.primary()
        binding.operationContext >> operationContext
        binding.readConnectionSource >> source
        source.connection >> connection
        source.retain() >> source
        source.operationContext >> operationContext
        def pipeline = new BsonArray([BsonDocument.parse('{ $match: {}}'), BsonDocument.parse('{$group: {_id: 1, n: {$sum: 1}}}')])
        def commandDocument = new BsonDocument('aggregate', new BsonString(getCollectionName()))
                .append('pipeline', pipeline)
                .append('cursor', new BsonDocument())
        appendReadConcernToCommand(sessionContext, UNKNOWN_WIRE_VERSION, commandDocument)

        def operation = new CountDocumentsOperation(getNamespace())

        when:
        operation.execute(binding)

        then:
        _ * connection.description >> new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
                6, STANDALONE, 1000, 100000, 100000, [])
        1 * connection.command(_, commandDocument, _, _, _, operationContext) >> helper.cursorResult
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
        def operationContext = OPERATION_CONTEXT.withSessionContext(sessionContext)
        def binding = Stub(AsyncReadBinding)
        def source = Stub(AsyncConnectionSource)
        def connection = Mock(AsyncConnection)
        binding.readPreference >> ReadPreference.primary()
        binding.operationContext >> operationContext
        binding.getReadConnectionSource(_) >> { it[0].onResult(source, null) }
        source.getConnection(_) >> { it[0].onResult(connection, null) }
        source.retain() >> source
        source.operationContext >> operationContext
        def pipeline = new BsonArray([BsonDocument.parse('{ $match: {}}'), BsonDocument.parse('{$group: {_id: 1, n: {$sum: 1}}}')])
        def commandDocument = new BsonDocument('aggregate', new BsonString(getCollectionName()))
                .append('pipeline', pipeline)
                .append('cursor', new BsonDocument())
        appendReadConcernToCommand(sessionContext, UNKNOWN_WIRE_VERSION, commandDocument)

        def operation = new CountDocumentsOperation(getNamespace())

        when:
        executeAsync(operation, binding)

        then:
        _ * connection.description >> new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
                6, STANDALONE, 1000, 100000, 100000, [])
        1 * connection.commandAsync(_, commandDocument, _, _, _, *_) >> {
            it.last().onResult(helper.cursorResult, null)
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
        cursorResult: BsonDocument.parse('{ok: 1.0}')
                .append('cursor', new BsonDocument('id', new BsonInt64(0)).append('ns', new BsonString('db.coll'))
                .append('firstBatch', new BsonArrayWrapper([BsonDocument.parse('{n: 10}') ]))),
        connectionDescription: Stub(ConnectionDescription)
    ]
}
