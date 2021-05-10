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
import com.mongodb.client.test.CollectionHelper
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ConnectionId
import com.mongodb.connection.ServerId
import com.mongodb.internal.binding.AsyncConnectionSource
import com.mongodb.internal.binding.AsyncReadBinding
import com.mongodb.internal.binding.ConnectionSource
import com.mongodb.internal.binding.ReadBinding
import com.mongodb.internal.connection.AsyncConnection
import com.mongodb.internal.connection.Connection
import com.mongodb.internal.session.SessionContext
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonDouble
import org.bson.BsonInt32
import org.bson.BsonJavaScript
import org.bson.BsonString
import org.bson.BsonTimestamp
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.DocumentCodec
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.DEFAULT_CSOT_FACTORY
import static com.mongodb.ClusterFixture.NO_CSOT_FACTORY
import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.connection.ServerType.STANDALONE
import static com.mongodb.internal.operation.OperationReadConcernHelper.appendReadConcernToCommand

class MapReduceWithInlineResultsOperationSpecification extends OperationFunctionalSpecification {
    private final bsonDocumentCodec = new BsonDocumentCodec()
    def mapReduceOperation = new MapReduceWithInlineResultsOperation<BsonDocument>(
            DEFAULT_CSOT_FACTORY,
            getNamespace(),
            new BsonJavaScript('function(){ emit( this.name , 1 ); }'),
            new BsonJavaScript('function(key, values){ return values.length; }'),
            bsonDocumentCodec)

    def expectedResults = [new BsonDocument('_id', new BsonString('Pete')).append('value', new BsonDouble(2.0)),
                           new BsonDocument('_id', new BsonString('Sam')).append('value', new BsonDouble(1.0))] as Set

    def setup() {
        CollectionHelper<BsonDocument> helper = new CollectionHelper<BsonDocument>(bsonDocumentCodec, getNamespace())
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        helper.insertDocuments(new DocumentCodec(), pete, sam, pete2)
    }

    def 'should have the correct defaults'() {
        when:
        def mapF = new BsonJavaScript('function(){ }')
        def reduceF = new BsonJavaScript('function(key, values){ }')
        def operation = new MapReduceWithInlineResultsOperation<BsonDocument>(DEFAULT_CSOT_FACTORY, helper.namespace, mapF, reduceF,
                bsonDocumentCodec)

        then:
        operation.getMapFunction() == mapF
        operation.getReduceFunction() == reduceF
        operation.getFilter() == null
        operation.getFinalizeFunction() == null
        operation.getScope() == null
        operation.getSort() == null
        operation.getLimit() == 0
        operation.getCollation() == null
        !operation.isJsMode()
        !operation.isVerbose()
    }

    def 'should set optional values correctly'(){
        when:
        def filter = new BsonDocument('filter', new BsonInt32(1))
        def scope = new BsonDocument('scope', new BsonInt32(1))
        def sort = new BsonDocument('sort', new BsonInt32(1))
        def finalizeF = new BsonJavaScript('function(key, value){}')
        def mapF = new BsonJavaScript('function(){ }')
        def reduceF = new BsonJavaScript('function(key, values){ }')
        def operation = new MapReduceWithInlineResultsOperation<BsonDocument>(DEFAULT_CSOT_FACTORY, helper.namespace, mapF, reduceF,
                bsonDocumentCodec)
                .filter(filter)
                .finalizeFunction(finalizeF)
                .scope(scope)
                .sort(sort)
                .jsMode(true)
                .verbose(true)
                .limit(20)
                .collation(defaultCollation)

        then:
        operation.getMapFunction() == mapF
        operation.getReduceFunction() == reduceF
        operation.getFilter() == filter
        operation.getFinalizeFunction() == finalizeF
        operation.getScope() == scope
        operation.getSort() == sort
        operation.getLimit() == 20
        operation.getCollation() == defaultCollation
        operation.isJsMode()
        operation.isVerbose()
    }

    def 'should return the correct results'() {
        given:
        def operation = mapReduceOperation

        when:
        def results = executeAndCollectBatchCursorResults(operation, async) as Set

        then:
        results == expectedResults

        where:
        async << [true, false]
    }

    def 'should use the ReadBindings readPreference to set slaveOK'() {
        when:
        def operation = new MapReduceWithInlineResultsOperation<Document>(DEFAULT_CSOT_FACTORY, helper.namespace,
                new BsonJavaScript('function(){ }'), new BsonJavaScript('function(key, values){ }'), bsonDocumentCodec)

        then:
        testOperationSlaveOk(operation, [3, 4, 0], readPreference, async, helper.commandResult)

        where:
        [async, readPreference] << [[true, false], [ReadPreference.primary(), ReadPreference.secondary()]].combinations()
    }

    def 'should create the expected command'() {
        when:
        def operation = new MapReduceWithInlineResultsOperation<Document>(NO_CSOT_FACTORY, helper.namespace,
                new BsonJavaScript('function(){ }'), new BsonJavaScript('function(key, values){ }'), bsonDocumentCodec)
        def expectedCommand = new BsonDocument('mapreduce', new BsonString(helper.namespace.getCollectionName()))
            .append('map', operation.getMapFunction())
            .append('reduce', operation.getReduceFunction())
            .append('out', new BsonDocument('inline', new BsonInt32(1)))

        then:
        testOperation(operation, serverVersion, expectedCommand, async, helper.commandResult)

        when:
        operation.filter(new BsonDocument('filter', new BsonInt32(1)))
                .scope(new BsonDocument('scope', new BsonInt32(1)))
                .sort(new BsonDocument('sort', new BsonInt32(1)))
                .finalizeFunction(new BsonJavaScript('function(key, value){}'))
                .jsMode(true)
                .verbose(true)
                .limit(20)


        expectedCommand.append('query', operation.getFilter())
                .append('scope', operation.getScope())
                .append('sort', operation.getSort())
                .append('finalize', operation.getFinalizeFunction())
                .append('jsMode', BsonBoolean.TRUE)
                .append('verbose', BsonBoolean.TRUE)
                .append('limit', new BsonInt32(20))

        if (includeCollation) {
            operation.collation(defaultCollation)
            expectedCommand.append('collation', defaultCollation.asDocument())
        }

        then:
        testOperation(operation, serverVersion, expectedCommand, async, helper.commandResult)

        where:
        serverVersion | includeCollation | async
        [3, 4, 0]     | true             | true
        [3, 4, 0]     | true             | false
        [3, 0, 0]     | false            | true
        [3, 0, 0]     | false            | false
    }

    def 'should throw an exception when using an unsupported ReadConcern'() {
        given:
        def operation = new MapReduceWithInlineResultsOperation<BsonDocument>(DEFAULT_CSOT_FACTORY, helper.namespace,
                new BsonJavaScript('function(){ }'), new BsonJavaScript('function(key, values){ }'), bsonDocumentCodec)

        when:
        testOperationThrows(operation, [3, 0, 0], readConcern, async)

        then:
        def exception = thrown(IllegalArgumentException)
        exception.getMessage().startsWith('ReadConcern not supported by wire version:')

        where:
        [async, readConcern] << [[true, false], [ReadConcern.MAJORITY, ReadConcern.LOCAL]].combinations()
    }

    def 'should throw an exception when using an unsupported Collation'() {
        given:
        def operation = new MapReduceWithInlineResultsOperation<BsonDocument>(DEFAULT_CSOT_FACTORY, helper.namespace,
                new BsonJavaScript('function(){ }'), new BsonJavaScript('function(key, values){ }'), bsonDocumentCodec)
                .collation(defaultCollation)

        when:
        testOperationThrows(operation, [3, 2, 0], async)

        then:
        def exception = thrown(IllegalArgumentException)
        exception.getMessage().startsWith('Collation not supported by wire version:')

        where:
        async << [false, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) })
    def 'should support collation'() {
        given:
        def document = Document.parse('{_id: 1, str: "foo"}')
        getCollectionHelper().insertDocuments(document)
        def operation = new MapReduceWithInlineResultsOperation<BsonDocument>(
                DEFAULT_CSOT_FACTORY,
                namespace,
                new BsonJavaScript('function(){ emit( this.str, 1 ); }'),
                new BsonJavaScript('function(key, values){ return Array.sum(values); }'),
                bsonDocumentCodec)
                .filter(BsonDocument.parse('{str: "FOO"}'))
                .collation(caseInsensitiveCollation)

        when:
        def results = executeAndCollectBatchCursorResults(operation, async)

        then:
        results == [new BsonDocument('_id', new BsonString('foo')).append('value', new BsonDouble(1))]

        where:
        async << [true, false]
    }

    def 'should add read concern to command'() {
        given:
        def binding = Stub(ReadBinding)
        def source = Stub(ConnectionSource)
        def connection = Mock(Connection)
        binding.readPreference >> ReadPreference.primary()
        binding.serverApi >> null
        binding.readConnectionSource >> source
        binding.sessionContext >> sessionContext
        source.connection >> connection
        source.retain() >> source
        def commandDocument = BsonDocument.parse('''
            { "mapreduce" : "coll",
              "map" : { "$code" : "function(){ }" },
              "reduce" : { "$code" : "function(key, values){ }" },
              "out" : { "inline" : 1 },
              }''')
        appendReadConcernToCommand(sessionContext, commandDocument)

        def operation = new MapReduceWithInlineResultsOperation<BsonDocument>(NO_CSOT_FACTORY, helper.namespace,
                new BsonJavaScript('function(){ }'), new BsonJavaScript('function(key, values){ }'), bsonDocumentCodec)

        when:
        operation.execute(binding)

        then:
        _ * connection.description >> new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
                 6, STANDALONE, 1000, 100000, 100000, [])
        1 * connection.command(_, commandDocument, _, _, _, sessionContext, null) >>
                new BsonDocument('results', new BsonArrayWrapper([]))
                        .append('counts',
                        new BsonDocument('input', new BsonInt32(0))
                                .append('output', new BsonInt32(0))
                                .append('emit', new BsonInt32(0)))
                        .append('timeMillis', new BsonInt32(0))
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
        binding.serverApi >> null
        binding.getReadConnectionSource(_) >> { it[0].onResult(source, null) }
        binding.sessionContext >> sessionContext
        source.serverApi >> null
        source.getConnection(_) >> { it[0].onResult(connection, null) }
        source.retain() >> source
        def commandDocument = BsonDocument.parse('''
            { "mapreduce" : "coll",
              "map" : { "$code" : "function(){ }" },
              "reduce" : { "$code" : "function(key, values){ }" },
              "out" : { "inline" : 1 },
              }''')
        appendReadConcernToCommand(sessionContext, commandDocument)

        def operation = new MapReduceWithInlineResultsOperation<BsonDocument>(NO_CSOT_FACTORY, helper.namespace,
                new BsonJavaScript('function(){ }'), new BsonJavaScript('function(key, values){ }'), bsonDocumentCodec)

        when:
        executeAsync(operation, binding)

        then:
        _ * connection.description >> new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
                 6, STANDALONE, 1000, 100000, 100000, [])
        1 * connection.commandAsync(_, commandDocument, _, _, _, sessionContext, _, _) >> {
            it.last().onResult(new BsonDocument('results', new BsonArrayWrapper([]))
                    .append('counts',
                    new BsonDocument('input', new BsonInt32(0))
                            .append('output', new BsonInt32(0))
                            .append('emit', new BsonInt32(0)))
                    .append('timeMillis', new BsonInt32(0)),
                    null)
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
            namespace: new MongoNamespace('db', 'coll'),
            commandResult: BsonDocument.parse('{ok: 1.0, counts: {input: 1, emit: 1, output: 1}, timeMillis: 1}')
                    .append('results', new BsonArrayWrapper([]))
    ]
}
