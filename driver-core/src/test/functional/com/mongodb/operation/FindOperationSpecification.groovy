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
import com.mongodb.async.SingleResultCallback
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
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.Decoder
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

    @IgnoreIf({ isSharded() || !serverVersionAtLeast([2, 6, 0]) })
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
        if (serverVersionAtLeast(asList(3, 0, 0))) {
            assertEquals(hint, getKeyPattern(explainPlan))
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
        if (serverVersionAtLeast(asList(3, 1, 8))) {
            assertEquals(expectedComment, ((Document) profileDocument.get('query')).get('comment'));
        } else {
            assertEquals(expectedComment, ((Document) profileDocument.get('query')).get('$comment'));
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
        String fieldName = serverVersionAtLeast([3, 1, 8]) ? '$recordId' : '$diskLoc';
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
                new ServerVersion(2, 6), STANDALONE, 1000, 100000, 100000)


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
                new ServerVersion(2, 6), STANDALONE, 1000, 100000, 100000)

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
        result

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
        sanitizeExplainResult(cursorResult) == sanitizeExplainResult(explainResult)

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

    // Note that this is a unit test
    def 'should query with correct arguments'() {
        given:
        def serverVersion = new ServerVersion(3, 0)
        def dbName = 'db'
        def collectionName = 'coll'
        def namespace = new MongoNamespace(dbName, collectionName)
        def decoder = new BsonDocumentCodec()
        def readPreference = ReadPreference.secondary()
        def connectionDescription = new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
                serverVersion, STANDALONE, 1000, 16000000, 48000000)
        def connection = Mock(Connection)
        def connectionSource = Stub(ConnectionSource) {
            getConnection() >> connection
        }
        def readBinding = Stub(ReadBinding) {
            getReadPreference() >> readPreference
            getReadConnectionSource() >> connectionSource
        }
        def queryResult = new QueryResult(namespace, [], 0, new ServerAddress());
        def operation = new FindOperation<BsonDocument>(namespace, decoder)
                .filter(new BsonDocument('a', BsonBoolean.TRUE))
                .projection(new BsonDocument('x', new BsonInt32(1)))
                .skip(2)
                .limit(100)
                .batchSize(10)
                .cursorType(TailableAwait)
                .oplogReplay(true)
                .noCursorTimeout(true)
                .partial(true)
                .oplogReplay(true)

        when:
        operation.execute(readBinding)

        then:
        _ * connection.getDescription() >> connectionDescription
        1 * connection.query(namespace,
                operation.getFilter(),
                operation.getProjection(),
                operation.getSkip(),
                operation.getLimit(),
                operation.getBatchSize(),
                readPreference.isSlaveOk(),
                true, true, true, true, true,
                decoder) >> queryResult
        1 * connection.release()
    }

    // Note that this is a unit test
    def 'should find with correct command'() {
        when:
        def operation = new FindOperation<BsonDocument>(namespace, new DocumentCodec())
        def expectedCommand = new BsonDocument('find', new BsonString(collectionName))

        then:
        testOperation(operation, [3, 2, 0], expectedCommand, async, helper.commandResult)

        when:
        operation.filter(new BsonDocument('a', BsonBoolean.TRUE))
                .projection(new BsonDocument('x', new BsonInt32(1)))
                .skip(2)
                .limit(limit)
                .batchSize(batchSize)
                .cursorType(TailableAwait)
                .oplogReplay(true)
                .noCursorTimeout(true)
                .partial(true)
                .oplogReplay(true)
                .modifiers(new BsonDocument('$snapshot', BsonBoolean.TRUE))
                .maxTime(10, MILLISECONDS)
                .readConcern(ReadConcern.MAJORITY)

        expectedCommand.append('filter', operation.getFilter())
                .append('projection', operation.getProjection())
                .append('skip', new BsonInt32(operation.getSkip()))
                .append('tailable', BsonBoolean.TRUE)
                .append('awaitData', BsonBoolean.TRUE)
                .append('allowPartialResults', BsonBoolean.TRUE)
                .append('noCursorTimeout', BsonBoolean.TRUE)
                .append('oplogReplay', BsonBoolean.TRUE)
                .append('snapshot', BsonBoolean.TRUE)
                .append('maxTimeMS', new BsonInt64(operation.getMaxTime(MILLISECONDS)))
                .append('readConcern', new BsonDocument('level', new BsonString('majority')))

        if (commandLimit != null) {
            expectedCommand.append('limit', new BsonInt32(commandLimit))
        }
        if (commandBatchSize != null) {
            expectedCommand.append('batchSize', new BsonInt32(commandBatchSize))
        }
        if (commandSingleBatch != null) {
            expectedCommand.append('singleBatch', BsonBoolean.valueOf(commandSingleBatch))
        }

        then:
        testOperation(operation, [3, 2, 0], expectedCommand, async, helper.commandResult)

        where:
        async << [true, true, true, true, true, false, false, false, false, false]
        limit << [100, -100, 100, 0, 100, 100, -100, 100, 0, 100]
        batchSize << [10, 10, -10, 10, 0, 10, 10, -10, 10, 0]
        commandLimit << [100, 100, 10, null, 100, 100, 100, 10, null, 100]
        commandBatchSize << [10, null, null, 10, null, 10, null, null, 10, null]
        commandSingleBatch << [null, true, true, null, null, null, true, true, null, null]
    }

    def 'should use the readPreference to set slaveOK for commands'() {
        when:
        def operation = new FindOperation<BsonDocument>(namespace, new DocumentCodec())

        then:
        testOperationSlaveOk(operation, [3, 2, 0], readPreference, async, helper.commandResult)

        where:
        [async, readPreference] << [[true, false], [ReadPreference.primary(), ReadPreference.secondary()]].combinations()
    }

    def 'should use the ReadBindings readPreference to set slaveOK'() {
        given:
        def dbName = 'db'
        def collectionName = 'coll'
        def namespace = new MongoNamespace(dbName, collectionName)
        def decoder = Stub(Decoder)
        def connectionDescription = new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
                new ServerVersion(3, 0), STANDALONE, 1000, 16000000, 48000000)
        def connection = Mock(Connection) {
            _ * getDescription() >> connectionDescription
        }
        def readBinding = Stub(ReadBinding) {
            getReadConnectionSource() >> Stub(ConnectionSource) {
                getConnection() >> connection
            }
            getReadPreference() >> Stub(ReadPreference) {
                isSlaveOk() >> slaveOk
            }
        }
        def queryResult = Mock(QueryResult) {
            _ * getNamespace() >> namespace
            _ * getResults() >> []
        }
        def operation = new FindOperation<BsonDocument>(namespace, decoder)

        when:
        operation.execute(readBinding)

        then:
        1 * connection.query(namespace, _, _, _, _, _, slaveOk, _, _, _, _, _, _) >> queryResult
        1 * connection.release()

        where:
        slaveOk << [true, false]
    }

    def 'should use the AsyncReadBindings readPreference to set slaveOK'() {
        given:
        def dbName = 'db'
        def collectionName = 'coll'
        def namespace = new MongoNamespace(dbName, collectionName)
        def decoder = Stub(Decoder)
        def connectionDescription = new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
                new ServerVersion(3, 0), STANDALONE, 1000, 16000000, 48000000)

        def connection = Mock(AsyncConnection) {
            _ * getDescription() >> connectionDescription
        }
        def connectionSource = Stub(AsyncConnectionSource) {
            getConnection(_) >> { it[0].onResult(connection, null) }
        }
        def readBinding = Stub(AsyncReadBinding) {
            getReadConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
            getReadPreference() >> Stub(ReadPreference) {
                isSlaveOk() >> slaveOk
            }
        }
        def queryResult = Mock(QueryResult) {
            _ * getNamespace() >> namespace
            _ * getResults() >> []
        }
        def operation = new FindOperation<BsonDocument>(namespace, decoder)

        when:
        operation.executeAsync(readBinding, Stub(SingleResultCallback))

        then:
        1 * connection.queryAsync(namespace, _, _, _, _, _, slaveOk, _, _, _, _, _, _, _) >> { it[13].onResult(queryResult, null) }
        1 * connection.release()

        where:
        slaveOk << [true, false]
    }

    def 'should throw an exception when using an unsupported Collation'() {
        given:
        def operation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .filter(BsonDocument.parse('{str: "FOO"}'))
                .collation(defaultCollation)

        when:
        testOperationThrows(operation, [3, 2, 0], async)

        then:
        def exception = thrown(IllegalArgumentException)
        exception.getMessage().startsWith('Collation not supported by server version:')

        where:
        async << [false, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 3, 10)) })
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

    def helper = [
        namespace: new MongoNamespace('db', 'coll'),
        queryResult: Stub(QueryResult),
        connectionDescription: Stub(ConnectionDescription),
        commandResult:  new BsonDocument('cursor', new BsonDocument('id', new BsonInt64(0))
            .append('ns', new BsonString('db.coll'))
            .append('firstBatch', new BsonArrayWrapper([])))
    ]

    static BsonDocument sanitizeExplainResult(BsonDocument document) {
        document.remove('ok')
        document.remove('millis')
        document.remove('executionStats')
        document.remove('serverInfo')
        document
    }

    static BsonDocument getKeyPattern(BsonDocument explainPlan) {
        def winningPlan = explainPlan.getDocument('queryPlanner').getDocument('winningPlan')
        if (winningPlan.containsKey('inputStage')) {
            return winningPlan.getDocument('inputStage').getDocument('keyPattern')
        } else if (winningPlan.containsKey('shards')) {
            return winningPlan.getArray('shards')[0].asDocument().getDocument('winningPlan')
                    .getDocument('inputStage').getDocument('keyPattern')
        }
    }

}
