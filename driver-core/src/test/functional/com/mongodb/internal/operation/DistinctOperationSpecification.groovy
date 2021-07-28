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

import com.mongodb.MongoExecutionTimeoutException
import com.mongodb.MongoNamespace
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.client.test.Worker
import com.mongodb.client.test.WorkerCodec
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
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonInvalidOperationException
import org.bson.BsonString
import org.bson.BsonTimestamp
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.Decoder
import org.bson.codecs.DocumentCodec
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.StringCodec
import org.bson.codecs.ValueCodecProvider
import org.bson.types.ObjectId
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.serverVersionLessThan
import static com.mongodb.connection.ServerType.STANDALONE
import static com.mongodb.internal.operation.OperationReadConcernHelper.appendReadConcernToCommand
import static com.mongodb.internal.operation.ServerVersionHelper.MIN_WIRE_VERSION
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
        operation.getCollation() == null
    }

    def 'should set optional values correctly'(){
        given:
        def filter = new BsonDocument('filter', new BsonInt32(1))

        when:
        DistinctOperation operation = new DistinctOperation(getNamespace(), 'name', stringDecoder)
                .maxTime(10, MILLISECONDS)
                .filter(filter)
                .collation(defaultCollation)

        then:
        operation.getFilter() == filter
        operation.getMaxTime(MILLISECONDS) == 10
        operation.getCollation() == defaultCollation
    }

    def 'should be able to distinct by name'() {
        given:
        Document pete = new Document('name', 'Pete').append('age', 38)
        Document sam = new Document('name', 'Sam').append('age', 21)
        Document pete2 = new Document('name', 'Pete').append('age', 25)
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)
        DistinctOperation operation = new DistinctOperation(getNamespace(), 'name', stringDecoder)

        when:
        def results = executeAndCollectBatchCursorResults(operation, async)

        then:
        results  == ['Pete', 'Sam']

        where:
        async << [true, false]
    }

    def 'should be able to distinct by name with find'() {
        given:
        Document pete = new Document('name', 'Pete').append('age', 38)
        Document sam = new Document('name', 'Sam').append('age', 21)
        Document pete2 = new Document('name', 'Pete').append('age', 25)
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)
        def operation = new DistinctOperation(getNamespace(), 'name', stringDecoder)
                .filter(new BsonDocument('age', new BsonInt32(25)))

        when:
        def results = executeAndCollectBatchCursorResults(operation, async)

        then:
        results  == ['Pete']

        where:
        async << [true, false]
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
        DistinctOperation operation = new DistinctOperation(getNamespace(), 'worker', new WorkerCodec())

        when:
        def results = executeAndCollectBatchCursorResults(operation, async)

        then:
        results  == [pete, sam]

        where:
        async << [true, false]
    }


    def 'should throw if invalid decoder passed to distinct'() {
        given:
        Document pete = new Document('name', 'Pete')
        Document sam = new Document('name', 1)
        Document pete2 = new Document('name', new Document('earle', 'Jones'))
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)
        DistinctOperation operation = new DistinctOperation(getNamespace(), 'name', stringDecoder)

        when:
        execute(operation, async)

        then:
        thrown(BsonInvalidOperationException)

        where:
        async << [true, false]
    }

    def 'should throw execution timeout exception from execute'() {
        given:
        def operation = new DistinctOperation(getNamespace(), 'name', stringDecoder).maxTime(1, SECONDS)
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

    def 'should use the ReadBindings readPreference to set secondaryOk'() {
        when:
        def operation = new DistinctOperation(helper.namespace, 'name', helper.decoder)

        then:
        testOperationSecondaryOk(operation, [3, 4, 0], readPreference, async, helper.commandResult)

        where:
        [async, readPreference] << [[true, false], [ReadPreference.primary(), ReadPreference.secondary()]].combinations()
    }

    def 'should create the expected command'() {
        when:
        def operation = new DistinctOperation(helper.namespace, 'name', new BsonDocumentCodec())
                .filter(new BsonDocument('a', BsonBoolean.TRUE))
                .maxTime(10, MILLISECONDS)
                .collation(defaultCollation)

        def expectedCommand = new BsonDocument('distinct', new BsonString(helper.namespace.getCollectionName()))
                .append('key', new BsonString('name'))
                .append('query', operation.getFilter())
                .append('maxTimeMS', new BsonInt64(operation.getMaxTime(MILLISECONDS)))
                .append('collation', defaultCollation.asDocument())

        then:
        testOperation(operation, [3, 4, 0], expectedCommand, async, helper.commandResult)

        where:
        async << [false, false]
    }


    def 'should throw an exception when using an unsupported ReadConcern'() {
        given:
        def operation = new DistinctOperation(helper.namespace, 'name', helper.decoder)

        when:
        testOperationThrows(operation, [3, 0, 0], readConcern, async)

        then:
        def exception = thrown(IllegalArgumentException)
        exception.getMessage().startsWith('ReadConcern not supported by wire version:')

        where:
        [async, readConcern] << [[true, false], [ReadConcern.MAJORITY, ReadConcern.LOCAL]].combinations()
    }

    def 'should throw an exception when using an unsupported Collation'() {
        def operation = new DistinctOperation(helper.namespace, 'name', helper.decoder)
                .collation(defaultCollation)

        when:
        testOperationThrows(operation, [3, 2, 0], async)

        then:
        def exception = thrown(IllegalArgumentException)
        exception.getMessage().startsWith('Collation not supported by wire version:')

        where:
        async << [false, false]
    }

    @IgnoreIf({ serverVersionLessThan(3, 4) })
    def 'should support collation'() {
        given:
        def document = Document.parse('{str: "foo"}')
        getCollectionHelper().insertDocuments(document)
        def operation = new DistinctOperation(namespace, 'str', stringDecoder).filter(BsonDocument.parse('{str: "FOO"}}'))
                .collation(caseInsensitiveCollation)

        when:
        def result = executeAndCollectBatchCursorResults(operation, async)

        then:
        result == ['foo']

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
        def commandDocument = new BsonDocument('distinct', new BsonString(getCollectionName()))
                .append('key', new BsonString('str'))
        appendReadConcernToCommand(sessionContext, MIN_WIRE_VERSION, commandDocument)

        def operation = new DistinctOperation<String>(getNamespace(), 'str', new StringCodec())

        when:
        operation.execute(binding)

        then:
        _ * connection.description >> new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
                6, STANDALONE, 1000, 100000, 100000, [])
        1 * connection.command(_, commandDocument, _, _, _, sessionContext, null) >>
                new BsonDocument('values', new BsonArrayWrapper([]))
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
        binding.serverApi >> null
        binding.readPreference >> ReadPreference.primary()
        binding.getReadConnectionSource(_) >> { it[0].onResult(source, null) }
        binding.sessionContext >> sessionContext
        source.serverApi >> null
        source.getConnection(_) >> { it[0].onResult(connection, null) }
        source.retain() >> source
        def commandDocument = new BsonDocument('distinct', new BsonString(getCollectionName()))
                .append('key', new BsonString('str'))
        appendReadConcernToCommand(sessionContext, MIN_WIRE_VERSION, commandDocument)

        def operation = new DistinctOperation<String>(getNamespace(), 'str', new StringCodec())

        when:
        executeAsync(operation, binding)

        then:
        _ * connection.description >> new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
                 6, STANDALONE, 1000, 100000, 100000, [])
        1 * connection.commandAsync(_, commandDocument, _, _, _, sessionContext, *_) >> {
            it.last().onResult(new BsonDocument('values', new BsonArrayWrapper([])), null)
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
        decoder: Stub(Decoder),
        commandResult: BsonDocument.parse('{ok: 1.0}').append('values', new BsonArrayWrapper([]))
    ]
}
