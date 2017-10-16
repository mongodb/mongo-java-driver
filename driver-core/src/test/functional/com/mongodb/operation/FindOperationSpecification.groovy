/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

import com.mongodb.ClusterFixture
import com.mongodb.MongoExecutionTimeoutException
import com.mongodb.MongoNamespace
import com.mongodb.MongoQueryException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.async.FutureResultCallback
import com.mongodb.binding.AsyncClusterBinding
import com.mongodb.binding.AsyncConnectionSource
import com.mongodb.binding.AsyncReadBinding
import com.mongodb.binding.ClusterBinding
import com.mongodb.binding.ConnectionSource
import com.mongodb.binding.ReadBinding
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.connection.AsyncConnection
import com.mongodb.connection.ClusterId
import com.mongodb.connection.Connection
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ConnectionId
import com.mongodb.connection.QueryResult
import com.mongodb.connection.ServerId
import com.mongodb.connection.ServerVersion
import com.mongodb.connection.SessionContext
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

import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.executeSync
import static com.mongodb.ClusterFixture.getAsyncCluster
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.getCluster
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.CursorType.NonTailable
import static com.mongodb.CursorType.Tailable
import static com.mongodb.CursorType.TailableAwait
import static com.mongodb.ExplainVerbosity.QUERY_PLANNER
import static com.mongodb.connection.ServerType.STANDALONE
import static com.mongodb.operation.ReadConcernHelper.appendReadConcernToCommand
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS
import static org.junit.Assert.assertEquals

class FindOperationSpecification extends OperationFunctionalSpecification {

    def 'should have the correct defaults'() {
        given:
        def decoder = new DocumentCodec()

        when:
        FindOperation operation = new FindOperation<Document>(getNamespace(), decoder)

        then:
        operation.getNamespace() == getNamespace()
        operation.getDecoder() == decoder
        operation.getFilter() == null
        operation.getMaxTime(MILLISECONDS) == 0
        operation.getMaxAwaitTime(MILLISECONDS) == 0
        operation.getLimit() == 0
        operation.getSkip() == 0
        operation.getBatchSize() == 0
        operation.getModifiers() == null
        operation.getProjection() == null
        operation.getReadConcern() == ReadConcern.DEFAULT
        operation.getCollation() == null
        !operation.isNoCursorTimeout()
        !operation.isOplogReplay()
        !operation.isPartial()
        !operation.isSlaveOk()
    }

    def 'should set optional values correctly'() {
        given:
        def filter = new BsonDocument('filter', new BsonInt32(1))
        def projection = new BsonDocument('projection', new BsonInt32(1))
        def modifiers = new BsonDocument('modifiers', new BsonInt32(1))

        when:
        FindOperation operation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .maxTime(10, SECONDS)
                .maxAwaitTime(20, SECONDS)
                .filter(filter)
                .limit(20)
                .skip(30)
                .batchSize(40)
                .projection(projection)
                .modifiers(modifiers)
                .cursorType(Tailable)
                .readConcern(ReadConcern.MAJORITY)
                .collation(defaultCollation)
                .partial(true)
                .slaveOk(true)
                .oplogReplay(true)
                .noCursorTimeout(true)

        then:
        operation.getFilter() == filter
        operation.getMaxTime(MILLISECONDS) == 10000
        operation.getMaxAwaitTime(MILLISECONDS) == 20000
        operation.getLimit() == 20
        operation.getSkip() == 30
        operation.getBatchSize() == 40
        operation.getProjection() == projection
        operation.getModifiers() == modifiers
        operation.getReadConcern() == ReadConcern.MAJORITY
        operation.getCollation() == defaultCollation
        operation.isNoCursorTimeout()
        operation.isOplogReplay()
        operation.isPartial()
        operation.isSlaveOk()
    }

    def 'should query with default values'() {
        given:
        def document = new Document('_id', 1)
        getCollectionHelper().insertDocuments(new DocumentCodec(), document);
        def operation = new FindOperation<Document>(getNamespace(), new DocumentCodec())

        when:
        def results = executeAndCollectBatchCursorResults(operation, async)

        then:
        results == [document]

        where:
        async << [true, false]
    }

    def 'should apply filter'() {
        given:
        def document = new Document('_id', 1)
        getCollectionHelper().insertDocuments(new DocumentCodec(), document, new Document());

        when:
        def results = executeAndCollectBatchCursorResults(operation, async)

        then:
        results == [document]

        where:
        [async, operation] << [
                [true, false],
                [new FindOperation<Document>(getNamespace(), new DocumentCodec()).filter(new BsonDocument('_id', new BsonInt32(1))),
                 new FindOperation<Document>(getNamespace(), new DocumentCodec())
                         .modifiers(new BsonDocument('$query', new BsonDocument('_id', new BsonInt32(1))))]
        ].combinations()
    }

    def 'should apply sort'() {
        given:
        def documents = [new Document('_id', 3), new Document('_id', 1), new Document('_id', 2), new Document('_id', 5),
                         new Document('_id', 4)]
        getCollectionHelper().insertDocuments(new DocumentCodec(), documents);


        when: 'ascending'
        def results = executeAndCollectBatchCursorResults(operation, async)

        then:
        results == [new Document('_id', 1), new Document('_id', 2), new Document('_id', 3), new Document('_id', 4), new Document('_id', 5)]

        where:
        [async, operation] << [
                [true, false],
                [new FindOperation<Document>(getNamespace(), new DocumentCodec()).sort(new BsonDocument('_id', new BsonInt32(1))),
                 new FindOperation<Document>(getNamespace(), new DocumentCodec())
                         .modifiers(new BsonDocument('$orderby', new BsonDocument('_id', new BsonInt32(1))))]
        ].combinations()
    }

    def 'should apply projection'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document('x', 5).append('y', 10), new Document('_id', 1).append('x', 10));
        def operation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .projection(new BsonDocument('_id', new BsonInt32(0)).append('x', new BsonInt32(1)))

        when:
        def results = executeAndCollectBatchCursorResults(operation, async)

        then:
        results == [new Document('x', 5), new Document('x', 10)]

        where:
        async << [true, false]
    }

    def 'should apply skip'() {
        given:
        def documents = [new Document('_id', 3), new Document('_id', 1), new Document('_id', 2), new Document('_id', 4),
                         new Document('_id', 5)]
        getCollectionHelper().insertDocuments(new DocumentCodec(), documents);

        def operation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .sort(new BsonDocument('_id', new BsonInt32(1)))
                .skip(3)

        when:
        def results = executeAndCollectBatchCursorResults(operation, async)

        then:
        results == [new Document('_id', 4), new Document('_id', 5)]

        where:
        async << [true, false]
    }

    def 'should apply limit'() {
        given:
        def documents = [new Document('_id', 1), new Document('_id', 2), new Document('_id', 3), new Document('_id', 4),
                         new Document('_id', 5)]
        getCollectionHelper().insertDocuments(new DocumentCodec(), documents);

        def operation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .sort(new BsonDocument('_id', new BsonInt32(1)))
                .limit(limit)

        when:
        def results = executeAndCollectBatchCursorResults(operation, async)

        then:
        results == [new Document('_id', 1), new Document('_id', 2), new Document('_id', 3)]

        where:
        [async, limit] << [[true, false], [3, -3]].combinations()
    }

    def 'should apply batch size'() {
        given:
        def documents = [new Document('_id', 1), new Document('_id', 2), new Document('_id', 3), new Document('_id', 4),
                         new Document('_id', 5)]
        getCollectionHelper().insertDocuments(new DocumentCodec(), documents);
        def operation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .sort(new BsonDocument('_id', new BsonInt32(1)))
                .batchSize(batchSize)

        when:
        def cursor = execute(operation, async)
        def firstBatch = {
            if (async) {
                def futureResultCallback = new FutureResultCallback()
                cursor.next(futureResultCallback)
                futureResultCallback.get(5, SECONDS)
            } else {
                cursor.next()
            }
        }()
        def hasAnotherBatch = {
            if (async) {
                def futureResultCallback = new FutureResultCallback()
                cursor.next(futureResultCallback)
                futureResultCallback.get(5, SECONDS) != null
            } else {
                cursor.hasNext()
            }
        }()

        then:
        firstBatch == [new Document('_id', 1), new Document('_id', 2), new Document('_id', 3)]
        hasAnotherBatch == hasNext

        where:
        batchSize | hasNext | async
        3         | true    | true
        3         | true    | false
        -3        | false   | true
        -3        | false   | false
    }

    def 'should throw query exception'() {
        given:
        def operation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .filter(new BsonDocument('x', new BsonDocument('$thisIsNotAnOperator', BsonBoolean.TRUE)))

        when:
        execute(operation, async)

        then:
        thrown(MongoQueryException)

        where:
        async << [true, false]
    }

    def 'should throw query exception from explain'() {
        given:
        def operation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .filter(new BsonDocument('x', new BsonDocument('$thisIsNotAnOperator', BsonBoolean.TRUE)))
        operation = async ? operation.asExplainableOperationAsync(QUERY_PLANNER) :
                operation.asExplainableOperation(QUERY_PLANNER)

        when:
        execute(operation, async)

        then:
        thrown(MongoQueryException)

        where:
        async << [true, false]
    }

    @IgnoreIf({ isSharded() })
    def 'should throw execution timeout exception from execute'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document())

        enableMaxTimeFailPoint()

        when:
        execute(operation, async)

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()

        where:
        [async, operation] << [
                [true, false],
                [new FindOperation<Document>(getNamespace(), new DocumentCodec()).maxTime(1000, MILLISECONDS),
                 new FindOperation<Document>(getNamespace(), new DocumentCodec()).
                         modifiers(new BsonDocument('$maxTimeMS', new BsonInt32(1000)))]
        ].combinations()
    }

    def '$max should limit items returned'() {
        given:
        (1..100).each {
            collectionHelper.insertDocuments(new DocumentCodec(), new Document('x', 'y').append('count', it))
        }
        collectionHelper.createIndex(new BsonDocument('count', new BsonInt32(1)))
        def operation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .modifiers(new BsonDocument('$max', new BsonDocument('count', new BsonInt32(11))))

        when:
        def count = executeAndCollectBatchCursorResults(operation, async).size()

        then:
        count == 10

        where:
        async << [true, false]
    }

    def '$min should limit items returned'() {
        given:
        (1..100).each {
            collectionHelper.insertDocuments(new DocumentCodec(), new Document('x', 'y').append('count', it))
        }
        collectionHelper.createIndex(new BsonDocument('count', new BsonInt32(1)))
        def operation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .modifiers(new BsonDocument('$min', new BsonDocument('count', new BsonInt32(10))))

        when:
        def count = executeAndCollectBatchCursorResults(operation, async).size()

        then:
        count == 91

        where:
        async << [true, false]
    }

    def '$maxScan should limit items returned'() {
        given:
        (1..100).each {
            collectionHelper.insertDocuments(new DocumentCodec(), new Document('x', 'y'))
        }
        def operation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .modifiers(new BsonDocument('$maxScan', new BsonInt32(34)))

        when:
        def count = executeAndCollectBatchCursorResults(operation, async).size()

        then:
        count == 34

        where:
        async << [true, false]
    }

    def '$returnKey should only return the field that was in an index used to perform the find'() {
        given:
        (1..13).each {
            collectionHelper.insertDocuments(new DocumentCodec(), new Document('x', it))
        }
        collectionHelper.createIndex(new BsonDocument('x', new BsonInt32(1)))

        def operation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .filter(new BsonDocument('x', new BsonInt32(7)))
                .modifiers(new BsonDocument('$returnKey', BsonBoolean.TRUE))

        when:
        def results = executeAndCollectBatchCursorResults(operation, async)

        then:
        results == [new Document('x', 7)]

        where:
        async << [true, false]
    }

    def 'should apply $hint'() {
        given:
        def hint = new BsonDocument('a', new BsonInt32(1))
        collectionHelper.createIndex(hint)

        def operation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .modifiers(new BsonDocument('$hint', hint))
        operation = async ? operation.asExplainableOperationAsync(QUERY_PLANNER) :
                operation.asExplainableOperation(QUERY_PLANNER)

        when:
        def explainPlan = execute(operation, async)

        then:
        if (serverVersionAtLeast(3, 0)) {
            assertEquals(hint, QueryOperationHelper.getKeyPattern(explainPlan))
        } else {
            assertEquals(new BsonString('BtreeCursor a_1'), explainPlan.cursor)
        }

        where:
        async << [true, false]
    }

    @IgnoreIf({ isSharded() })
    def 'should apply comment'() {
        given:
        def profileCollectionHelper = getCollectionHelper(new MongoNamespace(getDatabaseName(), 'system.profile'))
        new CommandWriteOperation(getDatabaseName(), new BsonDocument('profile', new BsonInt32(2)), new BsonDocumentCodec())
                .execute(getBinding())
        def expectedComment = 'this is a comment'
        def operation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .modifiers(new BsonDocument('$comment', new BsonString(expectedComment)))

        when:
        execute(operation, async)

        then:
        Document profileDocument = profileCollectionHelper.find().get(0)
        if (serverVersionAtLeast(asList(3, 5, 8))) {
            assertEquals(expectedComment, ((Document) profileDocument.get('command')).get('comment'))
        } else if (serverVersionAtLeast(3, 2)) {
            assertEquals(expectedComment, ((Document) profileDocument.get('query')).get('comment'))
        } else {
            assertEquals(expectedComment, ((Document) profileDocument.get('query')).get('$comment'))
        }

        cleanup:
        new CommandWriteOperation(getDatabaseName(), new BsonDocument('profile', new BsonInt32(0)), new BsonDocumentCodec())
                .execute(getBinding())
        profileCollectionHelper.drop();

        where:
        async << [true, false]
    }

    def 'should apply $showDiskLoc'() {
        given:
        String fieldName = serverVersionAtLeast(3, 2) ? '$recordId' : '$diskLoc';
        collectionHelper.insertDocuments(new BsonDocument())

        def operation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .modifiers(new BsonDocument('$showDiskLoc', BsonBoolean.TRUE))

        when:
        def result = executeAndCollectBatchCursorResults(operation, async).head()

        then:
        result[fieldName]

        where:
        async << [true, false]
    }

    @IgnoreIf({ !ClusterFixture.isDiscoverableReplicaSet() })
    def 'should read from a secondary'() {
        given:
        collectionHelper.insertDocuments(new DocumentCodec(), new Document())
        def operation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
        def syncBinding = new ClusterBinding(getCluster(), ReadPreference.secondary())
        def asyncBinding = new AsyncClusterBinding(getAsyncCluster(), ReadPreference.secondary())

        when:
        def result = async ? executeAsync(operation, asyncBinding) : executeSync(operation, syncBinding)

        then:
        result != null // if it didn't throw, the query was executed

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
        def commandDocument = new BsonDocument('find', new BsonString(getCollectionName()))
        appendReadConcernToCommand(ReadConcern.MAJORITY, sessionContext, commandDocument)

        def operation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
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
        def commandDocument = new BsonDocument('find', new BsonString(getCollectionName()))
        appendReadConcernToCommand(ReadConcern.MAJORITY, sessionContext, commandDocument)

        def operation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
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

    def 'should call query on Connection with no $query when there are no other meta operators'() {
        given:
        def operation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .projection(new BsonDocument('x', new BsonInt32(1)))
                .filter(new BsonDocument('z', new BsonString('val')))
        def binding = Stub(ReadBinding)
        def source = Stub(ConnectionSource)
        def connection = Mock(Connection)
        binding.readPreference >> ReadPreference.primary()
        binding.readConnectionSource >> source
        source.connection >> connection
        source.retain() >> source

        when:
        operation.execute(binding)

        then:
        _ * connection.description >> new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
                new ServerVersion(2, 6), STANDALONE, 1000, 100000, 100000, [])


        1 * connection.query(getNamespace(), operation.filter, operation.projection, 0, 0, 0, false, false, false, false,
                false, false, _) >> new QueryResult(getNamespace(), [new BsonDocument('n', new BsonInt32(1))], 0, new ServerAddress())
        1 * connection.release()
    }

    def 'should call query on Connection with correct arguments for an explain'() {
        given:
        def operation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .limit(20)
                .batchSize(2)
                .projection(new BsonDocument('x', new BsonInt32(1)))
                .sort(new BsonDocument('y', new BsonInt32(-1)))
                .filter(new BsonDocument('z', new BsonString('val')))
        def binding = Stub(ReadBinding)
        def source = Stub(ConnectionSource)
        def connection = Mock(Connection)
        binding.readPreference >> ReadPreference.primary()
        binding.readConnectionSource >> source
        source.connection >> connection
        source.retain() >> source

        when:
        operation.asExplainableOperation(QUERY_PLANNER).execute(binding)

        then:
        _ * connection.description >> new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
                new ServerVersion(2, 6), STANDALONE, 1000, 100000, 100000, [])

        1 * connection.query(getNamespace(), new BsonDocument('$query', operation.filter)
                .append('$explain', BsonBoolean.TRUE).append('$orderby', operation.sort),
                operation.projection, 0, -20, 0, false, false, false, false, false, false, _) >>
                new QueryResult(getNamespace(), [new BsonDocument('n', new BsonInt32(1))], 0, new ServerAddress())

        _ * connection.retain() >> connection
        _ * connection.release()
    }

    def 'should explain'() {
        given:
        def operation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .modifiers(modifiers)
        operation = async ? operation.asExplainableOperationAsync(QUERY_PLANNER) :
                operation.asExplainableOperation(QUERY_PLANNER)

        when:
        BsonDocument result = execute(operation, async)

        then:
        !result.isEmpty()

        where:
        [async, modifiers] << [
                [true, false],
                [null, new BsonDocument('$explain', BsonBoolean.TRUE), new BsonDocument('$explain', BsonBoolean.FALSE)]
        ].combinations()
    }


    def 'should explain with $explain modifier'() {
        given:
        def operation = new FindOperation<BsonDocument>(getNamespace(), new BsonDocumentCodec())
                .modifiers(new BsonDocument('$explain', BsonBoolean.TRUE))
        def explainOperation = async ?
                new FindOperation<BsonDocument>(getNamespace(), new BsonDocumentCodec()).asExplainableOperationAsync(QUERY_PLANNER) :
                new FindOperation<BsonDocument>(getNamespace(), new BsonDocumentCodec()).asExplainableOperation(QUERY_PLANNER)

        when:
        def cursorResult = executeAndCollectBatchCursorResults(operation, async).head()
        def explainResult = execute(explainOperation, async)

        then:
        QueryOperationHelper.sanitizeExplainResult(cursorResult) == QueryOperationHelper.sanitizeExplainResult(explainResult)

        where:
        async << [true, false]
    }

    //  sanity check that the server accepts tailable and await data flags
    def 'should pass tailable and await data flags through'() {
        given:
        def (cursorType, maxAwaitTimeMS, maxTimeMSForCursor) = cursorDetails
        collectionHelper.create(getCollectionName(), new CreateCollectionOptions().capped(true).sizeInBytes(1000))
        def operation = new FindOperation<BsonDocument>(namespace, new BsonDocumentCodec())
                .cursorType(cursorType)
                .maxAwaitTime(maxAwaitTimeMS, MILLISECONDS)

        when:
        def cursor = execute(operation, async)

        then:
        cursor.maxTimeMS == maxTimeMSForCursor

        where:
        [async, cursorDetails] << [
                [true, false],
                [[NonTailable, 100, 0], [Tailable, 100, 0], [TailableAwait, 100, 100]]
        ].combinations()
    }

    // sanity check that the server accepts the miscallaneous flags
    def 'should pass miscallaneous flags through'() {
        given:
        def operation = new FindOperation<BsonDocument>(namespace, new BsonDocumentCodec())
                .noCursorTimeout(true)
                .partial(true)
                .oplogReplay(true)

        when:
        execute(operation, async)

        then:
        true

        where:
        async << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) })
    def 'should support collation'() {
        given:
        def document = BsonDocument.parse('{_id: 1, str: "foo"}')
        getCollectionHelper().insertDocuments(document)
        def operation = new FindOperation<BsonDocument>(getNamespace(), new BsonDocumentCodec())
                .filter(BsonDocument.parse('{str: "FOO"}'))
                .collation(caseInsensitiveCollation)

        when:
        def result = executeAndCollectBatchCursorResults(operation, async)

        then:
        result == [document]

        where:
        async << [true, false]
    }


}
