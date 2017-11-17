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

import com.mongodb.ExplainVerbosity
import com.mongodb.MongoExecutionTimeoutException
import com.mongodb.MongoNamespace
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.WriteConcern
import com.mongodb.binding.AsyncConnectionSource
import com.mongodb.binding.AsyncReadBinding
import com.mongodb.binding.ConnectionSource
import com.mongodb.binding.ReadBinding
import com.mongodb.client.model.Collation
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.model.Filters
import com.mongodb.connection.AsyncConnection
import com.mongodb.connection.ClusterId
import com.mongodb.connection.Connection
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ConnectionId
import com.mongodb.connection.ServerId
import com.mongodb.connection.ServerVersion
import com.mongodb.session.SessionContext
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

import static com.mongodb.ClusterFixture.collectCursorResults
import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.getCluster
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.ExplainVerbosity.QUERY_PLANNER
import static com.mongodb.connection.ServerType.STANDALONE
import static com.mongodb.operation.QueryOperationHelper.getKeyPattern
import static com.mongodb.operation.ReadConcernHelper.appendReadConcernToCommand
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS

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
        operation.getMaxAwaitTime(MILLISECONDS) == 0
        operation.getMaxTime(MILLISECONDS) == 0
        operation.getPipeline() == []
        operation.getReadConcern() == ReadConcern.DEFAULT
        operation.getUseCursor() == null
    }

    def 'should set optional values correctly'(){
        when:
        AggregateOperation operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec())
                .allowDiskUse(true)
                .batchSize(10)
                .collation(defaultCollation)
                .maxAwaitTime(10, MILLISECONDS)
                .maxTime(10, MILLISECONDS)
                .readConcern(ReadConcern.MAJORITY)
                .useCursor(true)

        then:
        operation.getAllowDiskUse()
        operation.getBatchSize() == 10
        operation.getCollation() == defaultCollation
        operation.getMaxAwaitTime(MILLISECONDS) == 10
        operation.getMaxTime(MILLISECONDS) == 10
        operation.getReadConcern() == ReadConcern.MAJORITY
        operation.getUseCursor()
    }

    def 'should create the expected command'() {
        when:
        def pipeline = [new BsonDocument('$match', new BsonDocument('a', new BsonString('A')))]
        def operation = new AggregateOperation<Document>(helper.namespace, pipeline, new DocumentCodec())
                .allowDiskUse(true)
                .batchSize(10)
                .collation(defaultCollation)
                .maxAwaitTime(15, MILLISECONDS)
                .maxTime(10, MILLISECONDS)
                .readConcern(ReadConcern.MAJORITY)
                .useCursor(true)

        def expectedCommand = new BsonDocument('aggregate', new BsonString(helper.namespace.getCollectionName()))
                .append('pipeline', new BsonArray(pipeline))
                .append('allowDiskUse', new BsonBoolean(true))
                .append('collation', defaultCollation.asDocument())
                .append('cursor', new BsonDocument('batchSize', new BsonInt32(10)))
                .append('maxTimeMS', new BsonInt64(10))
                .append('readConcern', new BsonDocument('level', new BsonString('majority')))

        then:
        testOperation(operation, [3, 4, 0], expectedCommand, async, helper.cursorResult)

        where:
        async << [true, false]
    }

    def 'should throw an exception when using an unsupported ReadConcern'() {
        given:
        def pipeline = [new BsonDocument('$match', new BsonDocument('a', new BsonString('A')))]
        def operation = new AggregateOperation<Document>(helper.namespace, pipeline, new DocumentCodec()).readConcern(readConcern)

        when:
        testOperationThrows(operation, [3, 0, 0], async)

        then:
        def exception = thrown(IllegalArgumentException)
        exception.getMessage().startsWith('ReadConcern not supported by server version:')

        where:
        [async, readConcern] << [[true, false], [ReadConcern.MAJORITY, ReadConcern.LOCAL]].combinations()
    }

    def 'should throw an exception when using an unsupported Collation'() {
        given:
        def pipeline = [new BsonDocument('$match', new BsonDocument('a', new BsonString('A')))]
        def operation = new AggregateOperation<Document>(helper.namespace, pipeline, new DocumentCodec())
                .collation(defaultCollation)

        when:
        testOperationThrows(operation, [3, 2, 0], async)

        then:
        def exception = thrown(IllegalArgumentException)
        exception.getMessage().startsWith('Collation not supported by server version:')

        where:
        async << [false, false]
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

    @IgnoreIf({ !(serverVersionAtLeast(3, 6) && isDiscoverableReplicaSet()) })
    def 'should support changeStreams'() {
        given:
        def expected = [createExpectedChangeNotification(namespace, 0), createExpectedChangeNotification(namespace, 1)]

        def pipeline = ['{$changeStream: {}}', '{$project: {"_id.clusterTime": 0, "_id.uuid": 0}}'].collect { BsonDocument.parse(it) }
        def operation = new AggregateOperation<BsonDocument>(namespace, pipeline, new BsonDocumentCodec())

        when:
        getCollectionHelper().drop()
        getCollectionHelper().create(getCollectionHelper().getNamespace().getCollectionName(), new CreateCollectionOptions())
        def cursor = execute(operation, async)
        getCollectionHelper().insertDocuments(['{_id: 0, a: 0}', '{_id: 1, a: 1}'].collect { BsonDocument.parse(it) })

        then:
        def next = next(cursor, async).collect { doc ->
            doc.remove('_id')
            doc
        }
        next == expected

        cleanup:
        cursor?.close()

        where:
        async << [true, false]
    }

    def 'should be able to aggregate'() {
        when:
        AggregateOperation operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec()).useCursor(useCursor)
        def batchCursor = execute(operation, async)
        def results = collectCursorResults(batchCursor)*.getString('name')

        then:
        results.size() == 3
        results.containsAll(['Pete', 'Sam'])

        where:
        [async, useCursor] << [[true, false], useCursorOptions()].combinations()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) })
    def 'should be able to aggregate on a view'() {
        given:
        def viewSuffix = '-view'
        def viewName = getCollectionName() + viewSuffix
        def viewNamespace = new MongoNamespace(getDatabaseName(), viewName)
        new CreateViewOperation(getDatabaseName(), viewName, getCollectionName(), [], WriteConcern.ACKNOWLEDGED)
                .execute(getBinding(getCluster()))

        when:
        AggregateOperation operation = new AggregateOperation<Document>(viewNamespace, [], new DocumentCodec())
                .useCursor(useCursor)
        def batchCursor = execute(operation, async)
        def results = collectCursorResults(batchCursor)*.getString('name')

        then:
        results.size() == 3
        results.containsAll(['Pete', 'Sam'])

        cleanup:
        new DropCollectionOperation(viewNamespace, WriteConcern.ACKNOWLEDGED).execute(getBinding(getCluster()))

        where:
        [async, useCursor] << [[true, false], useCursorOptions()].combinations()
    }

    def 'should be able to aggregate with pipeline'() {
        when:
        AggregateOperation operation = new AggregateOperation<Document>(getNamespace(),
                [new BsonDocument('$match', new BsonDocument('job', new BsonString('plumber')))], new DocumentCodec()).useCursor(useCursor)
        def batchCursor = execute(operation, async)
        def results = collectCursorResults(batchCursor)*.getString('name')

        then:
        results.size() == 1
        results == ['Sam']

        where:
        [async, useCursor] << [[true, false], useCursorOptions()].combinations()
    }

    def 'should allow disk usage'() {
        when:
        AggregateOperation operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec()).allowDiskUse(allowDiskUse)
        def cursor = operation.execute(getBinding())

        then:
        cursor.next()*.getString('name') == ['Pete', 'Sam', 'Pete']

        where:
        allowDiskUse << [null, true, false]
    }

    def 'should allow batch size'() {
        when:
        AggregateOperation operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec()).batchSize(batchSize)
        def cursor = operation.execute(getBinding())

        then:
        cursor.next()*.getString('name') == ['Pete', 'Sam', 'Pete']

        where:
        batchSize << [null, 0, 10]
    }

    @IgnoreIf({ isSharded() })
    def 'should throw execution timeout exception from execute'() {
        given:
        def operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec()).maxTime(1, SECONDS)
        enableMaxTimeFailPoint()

        when:
        execute(operation, async)

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()

        where:
        async << [true, false]
    }

    def 'should be able to explain an empty pipeline'() {
        given:
        def operation = new AggregateOperation(getNamespace(), [], new BsonDocumentCodec())
        operation = async ? operation.asExplainableOperationAsync(ExplainVerbosity.QUERY_PLANNER) :
                            operation.asExplainableOperation(ExplainVerbosity.QUERY_PLANNER)

        when:
        def result = execute(operation, async)

        then:
        result.containsKey('stages')

        where:
        async << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) })
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

    @IgnoreIf({ !serverVersionAtLeast(3, 6) })
    def 'should apply $hint'() {
        given:
        def hint = new BsonDocument('a', new BsonInt32(1))
        collectionHelper.createIndex(hint)

        def operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec())
                .hint(hint)

        when:
        execute(operation, async)
        BsonDocument explainPlan = execute(operation.asExplainableOperation(QUERY_PLANNER), async)

        then:
        getKeyPattern(explainPlan.getArray('stages').get(0).asDocument().getDocument('$cursor')) == hint

        where:
        async << [true, false]
    }

    @IgnoreIf({ isSharded() || !serverVersionAtLeast(3, 6) })
    def 'should apply comment'() {
        given:
        def profileCollectionHelper = getCollectionHelper(new MongoNamespace(getDatabaseName(), 'system.profile'))
        new CommandWriteOperation(getDatabaseName(), new BsonDocument('profile', new BsonInt32(2)), new BsonDocumentCodec())
                .execute(getBinding())
        def expectedComment = 'this is a comment'
        def operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec())
                .comment(expectedComment)

        when:
        execute(operation, async)

        then:
        Document profileDocument = profileCollectionHelper.find(Filters.exists('command.aggregate')).get(0)
        ((Document) profileDocument.get('command')).get('comment') == expectedComment

        cleanup:
        new CommandWriteOperation(getDatabaseName(), new BsonDocument('profile', new BsonInt32(0)), new BsonDocumentCodec())
                .execute(getBinding())
        profileCollectionHelper.drop();

        where:
        async << [true, false]
    }

    @IgnoreIf({ isSharded() || !serverVersionAtLeast(3, 2) })
    def 'should be able to respect maxTime with pipeline'() {
        given:
        enableMaxTimeFailPoint()
        AggregateOperation operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec())
                .maxTime(10, MILLISECONDS)

        when:
        execute(operation, async)

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()

        where:
        async << [true, false]
    }

    @IgnoreIf({ isSharded() || !serverVersionAtLeast(3, 2) })
    def 'should be able to respect maxAwaitTime with pipeline'() {
        given:
        enableMaxTimeFailPoint()
        AggregateOperation operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec())
                .batchSize(2)
                .maxAwaitTime(10, MILLISECONDS)

        when:
        def cursor = execute(operation, async)
        next(cursor, async)

        then:
        notThrown(MongoExecutionTimeoutException)

        when:
        next(cursor, async)

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()

        where:
        async << [true, false]
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
        def commandDocument = new BsonDocument('aggregate', new BsonString(getCollectionName()))
                .append('pipeline', new BsonArray())
                .append('cursor', new BsonDocument())
        appendReadConcernToCommand(ReadConcern.MAJORITY, sessionContext, commandDocument)

        def operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec())
                .readConcern(ReadConcern.MAJORITY)

        when:
        operation.execute(binding)

        then:
        _ * connection.description >> new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
                new ServerVersion(3, 6), STANDALONE, 1000, 100000, 100000, [])
        1 * connection.command(_, commandDocument, _, _, _, sessionContext) >>
                new BsonDocument('cursor', new BsonDocument('id', new BsonInt64(1))
                        .append('ns', new BsonString(getNamespace().getFullName()))
                        .append('firstBatch', new BsonArrayWrapper([])))
        1 * connection.release()

        where:
        sessionContext << [
                Stub(SessionContext) {
                    isCausallyConsistent() >> true
                    getOperationTime() >> new BsonTimestamp(42, 0)
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
        def commandDocument = new BsonDocument('aggregate', new BsonString(getCollectionName()))
                .append('pipeline', new BsonArray())
                .append('cursor', new BsonDocument())
        appendReadConcernToCommand(ReadConcern.MAJORITY, sessionContext, commandDocument)

        def operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec())
                .readConcern(ReadConcern.MAJORITY)

        when:
        executeAsync(operation, binding)

        then:
        _ * connection.description >> new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
                new ServerVersion(3, 6), STANDALONE, 1000, 100000, 100000, [])
        1 * connection.commandAsync(_, commandDocument, _, _, _, sessionContext, _) >> {
            it[6].onResult(new BsonDocument('cursor', new BsonDocument('id', new BsonInt64(1))
                    .append('ns', new BsonString(getNamespace().getFullName()))
                    .append('firstBatch', new BsonArrayWrapper([]))), null)
        }
        1 * connection.release()

        where:
        sessionContext << [
                Stub(SessionContext) {
                    isCausallyConsistent() >> true
                    getOperationTime() >> new BsonTimestamp(42, 0)
                }
        ]
    }

    def 'should use the ReadBindings readPreference to set slaveOK'() {
        when:
        def operation = new AggregateOperation(helper.namespace, [], new BsonDocumentCodec())

        then:
        testOperationSlaveOk(operation, [2, 6, 0], readPreference, async, helper.cursorResult)

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

    private static List<Boolean> useCursorOptions() {
        [null, true, false]
    }

    private static BsonDocument createExpectedChangeNotification(MongoNamespace namespace, int idValue) {
        BsonDocument.parse("""{
            "operationType": "insert",
            "fullDocument": {"_id": $idValue, "a": $idValue},
            "ns": {"coll": "${namespace.getCollectionName()}", "db": "${namespace.getDatabaseName()}"},
            "documentKey": {"_id": $idValue}
        }""")
    }
}
