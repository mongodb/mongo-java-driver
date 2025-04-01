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


import com.mongodb.MongoNamespace
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.WriteConcern
import com.mongodb.client.model.Collation
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.model.Filters
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ConnectionId
import com.mongodb.connection.ServerId
import com.mongodb.connection.ServerVersion
import com.mongodb.internal.binding.AsyncConnectionSource
import com.mongodb.internal.binding.AsyncReadBinding
import com.mongodb.internal.binding.ConnectionSource
import com.mongodb.internal.binding.ReadBinding
import com.mongodb.internal.connection.AsyncConnection
import com.mongodb.internal.connection.Connection
import com.mongodb.internal.session.SessionContext
import org.bson.BsonArray
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.BsonTimestamp
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.DocumentCodec
import spock.lang.IgnoreIf

import static TestOperationHelper.getKeyPattern
import static com.mongodb.ClusterFixture.OPERATION_CONTEXT
import static com.mongodb.ClusterFixture.collectCursorResults
import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getAsyncCluster
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.getCluster
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.isStandalone
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.ClusterFixture.serverVersionLessThan
import static com.mongodb.ExplainVerbosity.QUERY_PLANNER
import static com.mongodb.connection.ServerType.STANDALONE
import static com.mongodb.internal.connection.ServerHelper.waitForLastRelease
import static com.mongodb.internal.operation.OperationReadConcernHelper.appendReadConcernToCommand
import static com.mongodb.internal.operation.ServerVersionHelper.UNKNOWN_WIRE_VERSION

class AggregateOperationSpecification extends OperationFunctionalSpecification {

    def setup() {
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)
    }

    def 'should have the correct defaults'() {
        when:
        AggregateOperation operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec())

        then:
        operation.getAllowDiskUse() == null
        operation.getBatchSize() == null
        operation.getCollation() == null
        operation.getPipeline() == []
    }

    def 'should set optional values correctly'(){
        given:
        def hint = BsonDocument.parse('{a: 1}')

        when:
        AggregateOperation operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec())
                .allowDiskUse(true)
                .batchSize(10)
                .collation(defaultCollation)
                .hint(hint)

        then:
        operation.getAllowDiskUse()
        operation.getBatchSize() == 10
        operation.getCollation() == defaultCollation
        operation.getHint() == hint
    }

    def 'should throw when using invalid hint'() {
        given:
        def hint = new BsonString('ok')
        def operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec()).hint(hint)

        when:
        operation.getHint()

        then:
        thrown(IllegalArgumentException)

        when:
        def result = operation.getHintBsonValue()

        then:
        result == hint

        when:
        operation.hint(new BsonInt32(1))

        then:
        thrown(IllegalArgumentException)
    }

    def 'should create the expected command'() {
        when:
        def pipeline = [new BsonDocument('$match', new BsonDocument('a', new BsonString('A')))]
        def operation = new AggregateOperation<Document>(helper.namespace, pipeline, new DocumentCodec())

        def expectedCommand = new BsonDocument('aggregate', new BsonString(helper.namespace.getCollectionName()))
                .append('pipeline', new BsonArray(pipeline))
                .append('cursor', new BsonDocument())

        then:
        testOperation(operation, [3, 4, 0], expectedCommand, async, helper.cursorResult)

        when:
        operation = new AggregateOperation<Document>(helper.namespace, pipeline, new DocumentCodec())
                .allowDiskUse(true)
                .batchSize(10)
                .collation(defaultCollation)

        expectedCommand = new BsonDocument('aggregate', new BsonString(helper.namespace.getCollectionName()))
                .append('pipeline', new BsonArray(pipeline))
                .append('allowDiskUse', new BsonBoolean(true))
                .append('collation', defaultCollation.asDocument())
                .append('cursor', new BsonDocument('batchSize', new BsonInt32(10)))

        then:
        testOperation(operation, [3, 4, 0], expectedCommand, async, helper.cursorResult)

        where:
        async << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) })
    def 'should support collation'() {
        given:
        def document = BsonDocument.parse('{_id: 1, str: "foo"}')
        getCollectionHelper().insertDocuments(document)
        def pipeline = [BsonDocument.parse('{$match: {str: "FOO"}}')]
        def operation = new AggregateOperation<BsonDocument>(namespace, pipeline, new BsonDocumentCodec())
                .collation(caseInsensitiveCollation)

        when:
        def result = executeAndCollectBatchCursorResults(operation, async)

        then:
        result == [document]

        where:
        async << [true, false]
    }

    @IgnoreIf({ !(serverVersionAtLeast(3, 6) && !isStandalone()) })
    def 'should support changeStreams'() {
        given:
        def expected = [createExpectedChangeNotification(namespace, 0), createExpectedChangeNotification(namespace, 1)]
        def pipeline = ['{$changeStream: {}}', '{$project: {"_id.clusterTime": 0, "_id.uuid": 0}}'].collect { BsonDocument.parse(it) }
        def operation = new AggregateOperation<BsonDocument>(namespace, pipeline, new BsonDocumentCodec())
        def helper = getCollectionHelper()

        when:
        helper.create(helper.getNamespace().getCollectionName(), new CreateCollectionOptions())
        def cursor = execute(operation, async)
        helper.insertDocuments(['{_id: 0, a: 0}', '{_id: 1, a: 1}'].collect { BsonDocument.parse(it) })

        then:
        def nextDoc = next(cursor, async).collect { doc ->
            doc.remove('_id')
            doc.remove('clusterTime')
            doc.remove('wallTime')
            doc
        }
        nextDoc == expected

        cleanup:
        cursor?.close()
        waitForLastRelease(async ? getAsyncCluster() : getCluster())

        where:
        async << [true, false]
    }

    def 'should be able to aggregate'() {
        when:
        AggregateOperation operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec())
        def batchCursor = execute(operation, async)
        def results = collectCursorResults(batchCursor)*.getString('name')

        then:
        results.size() == 3
        results.containsAll(['Pete', 'Sam'])

        where:
        async << [true, false]
    }

    @IgnoreIf({ serverVersionLessThan(3, 4) })
    def 'should be able to aggregate on a view'() {
        given:
        def viewSuffix = '-view'
        def viewName = getCollectionName() + viewSuffix
        def viewNamespace = new MongoNamespace(getDatabaseName(), viewName)
        new CreateViewOperation(getDatabaseName(), viewName, getCollectionName(), [], WriteConcern.ACKNOWLEDGED)
                .execute(getBinding(getCluster()))

        when:
        AggregateOperation operation = new AggregateOperation<Document>(viewNamespace, [], new DocumentCodec())
        def batchCursor = execute(operation, async)
        def results = collectCursorResults(batchCursor)*.getString('name')

        then:
        results.size() == 3
        results.containsAll(['Pete', 'Sam'])

        cleanup:
        new DropCollectionOperation(viewNamespace, WriteConcern.ACKNOWLEDGED)
                .execute(getBinding(getCluster()))

        where:
        async << [true, false]
    }

    def 'should be able to aggregate with pipeline'() {
        when:
        AggregateOperation operation = new AggregateOperation<Document>(getNamespace(),
                [new BsonDocument('$match', new BsonDocument('job', new BsonString('plumber')))], new DocumentCodec())
        def batchCursor = execute(operation, async)
        def results = collectCursorResults(batchCursor)*.getString('name')

        then:
        results.size() == 1
        results == ['Sam']

        where:
        async << [true, false]
    }

    def 'should allow disk usage'() {
        when:
        AggregateOperation operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec())
                .allowDiskUse(allowDiskUse)
        def cursor = operation.execute(getBinding())

        then:
        cursor.next()*.getString('name') == ['Pete', 'Sam', 'Pete']

        where:
        allowDiskUse << [null, true, false]
    }

    def 'should allow batch size'() {
        when:
        AggregateOperation operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec())
                .batchSize(batchSize)
        def cursor = operation.execute(getBinding())

        then:
        cursor.next()*.getString('name') == ['Pete', 'Sam', 'Pete']

        where:
        batchSize << [null, 0, 10]
    }

    @IgnoreIf({ serverVersionLessThan(3, 6) })
    def 'should be able to explain an empty pipeline'() {
        given:
        def operation = new AggregateOperation(getNamespace(), [], new BsonDocumentCodec())
        operation = async ? operation.asAsyncExplainableOperation(QUERY_PLANNER, new BsonDocumentCodec()) :
                            operation.asExplainableOperation(QUERY_PLANNER, new BsonDocumentCodec())

        when:
        def result = execute(operation, async)

        then:
        result.containsKey('stages') || result.containsKey('queryPlanner') || result.containsKey('shards')

        where:
        async << [true, false]
    }

    @IgnoreIf({ serverVersionLessThan(3, 4) })
    def 'should be able to aggregate with collation'() {
        when:
        AggregateOperation operation = new AggregateOperation<Document>(getNamespace(),
                [BsonDocument.parse('{$match: {job : "plumber"}}')], new DocumentCodec()
        ).collation(options)
        def batchCursor = execute(operation, async)
        def results = collectCursorResults(batchCursor)*.getString('name')

        then:
        results.size() == 1
        results == ['Sam']

        where:
        [async, options] << [[true, false], [defaultCollation, null, Collation.builder().build()]].combinations()
    }

    // Explain output keeps changing so only testing this in the range where the assertion still works
    @IgnoreIf({ serverVersionLessThan(3, 6) || serverVersionAtLeast(4, 2) })
    def 'should apply $hint'() {
        given:
        def index = new BsonDocument('a', new BsonInt32(1))
        collectionHelper.createIndex(index)

        def operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec())
                .hint(hint)

        when:
        execute(operation, async)
        BsonDocument explainPlan = execute(operation.asExplainableOperation(QUERY_PLANNER, new BsonDocumentCodec()), async)

        then:
        getKeyPattern(explainPlan.getArray('stages').get(0).asDocument().getDocument('$cursor')) == index

        where:
        [async, hint] << [[true, false], [BsonDocument.parse('{a: 1}'), new BsonString('a_1')]].combinations()
    }

    @IgnoreIf({ isSharded() || serverVersionLessThan(3, 6) })
    def 'should apply comment'() {
        given:
        def profileCollectionHelper = getCollectionHelper(new MongoNamespace(getDatabaseName(), 'system.profile'))
        new CommandReadOperation<>(getDatabaseName(), new BsonDocument('profile', new BsonInt32(2)),
                new BsonDocumentCodec()).execute(getBinding())
        def expectedComment = 'this is a comment'
        def operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec())
                .comment(new BsonString(expectedComment))

        when:
        execute(operation, async)

        then:
        Document profileDocument = profileCollectionHelper.find(Filters.exists('command.aggregate')).get(0)
        ((Document) profileDocument.get('command')).get('comment') == expectedComment

        cleanup:
        new CommandReadOperation<>(getDatabaseName(), new BsonDocument('profile', new BsonInt32(0)),
                new BsonDocumentCodec()).execute(getBinding())
        profileCollectionHelper.drop()

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
        def commandDocument = new BsonDocument('aggregate', new BsonString(getCollectionName()))
                .append('pipeline', new BsonArray())
                .append('cursor', new BsonDocument())
        appendReadConcernToCommand(operationContext.getSessionContext(), UNKNOWN_WIRE_VERSION, commandDocument)

        def operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec())

        when:
        operation.execute(binding)

        then:
        _ * connection.description >> new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
                6, STANDALONE, 1000, 100000, 100000, [])
        1 * connection.command(_, commandDocument, _, _, _, operationContext) >>
                new BsonDocument('cursor', new BsonDocument('id', new BsonInt64(1))
                        .append('ns', new BsonString(getNamespace().getFullName()))
                        .append('firstBatch', new BsonArrayWrapper([])))
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
        binding.operationContext >> operationContext
        binding.getReadConnectionSource(_) >> { it[0].onResult(source, null) }
        source.operationContext >> operationContext
        source.getConnection(_) >> { it[0].onResult(connection, null) }
        source.retain() >> source
        def commandDocument = new BsonDocument('aggregate', new BsonString(getCollectionName()))
                .append('pipeline', new BsonArray())
                .append('cursor', new BsonDocument())
        appendReadConcernToCommand(sessionContext, UNKNOWN_WIRE_VERSION, commandDocument)

        def operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec())

        when:
        executeAsync(operation, binding)

        then:
        _ * connection.description >> new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
                6, STANDALONE, 1000, 100000, 100000, [])
        1 * connection.commandAsync(_, commandDocument, _, _, _, operationContext, _) >> {
            it.last().onResult(new BsonDocument('cursor', new BsonDocument('id', new BsonInt64(1))
                    .append('ns', new BsonString(getNamespace().getFullName()))
                    .append('firstBatch', new BsonArrayWrapper([]))), null)
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

    def 'should use the ReadBindings readPreference to set secondaryOk'() {
        when:
        def operation = new AggregateOperation(helper.namespace, [], new BsonDocumentCodec())

        then:
        testOperationSecondaryOk(operation, [2, 6, 0], readPreference, async, helper.cursorResult)

        where:
        [async, readPreference] << [[true, false], [ReadPreference.primary(), ReadPreference.secondary()]].combinations()
    }

    def helper = [
            dbName: 'db',
            namespace: new MongoNamespace('db', 'coll'),
            twoSixConnectionDescription : Stub(ConnectionDescription) {
                getServerVersion() >> new ServerVersion([2, 6, 0])
            },
            inlineResult: BsonDocument.parse('{ok: 1.0}').append('result', new BsonArrayWrapper([])),
            cursorResult: BsonDocument.parse('{ok: 1.0}')
                    .append('cursor', new BsonDocument('id', new BsonInt64(0)).append('ns', new BsonString('db.coll'))
                    .append('firstBatch', new BsonArrayWrapper([])))
    ]

    private static BsonDocument createExpectedChangeNotification(MongoNamespace namespace, int idValue) {
        BsonDocument.parse("""{
            "operationType": "insert",
            "fullDocument": {"_id": $idValue, "a": $idValue},
            "ns": {"coll": "${namespace.getCollectionName()}", "db": "${namespace.getDatabaseName()}"},
            "documentKey": {"_id": $idValue}
        }""")
    }
}
