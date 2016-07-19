/*
 * Copyright (c) 2008-2016 MongoDB, Inc.
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
import com.mongodb.MongoCommandException
import com.mongodb.MongoNamespace
import com.mongodb.MongoWriteConcernException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.WriteConcern
import com.mongodb.async.SingleResultCallback
import com.mongodb.binding.AsyncConnectionSource
import com.mongodb.binding.AsyncWriteBinding
import com.mongodb.binding.ConnectionSource
import com.mongodb.binding.WriteBinding
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.model.ValidationOptions
import com.mongodb.client.test.CollectionHelper
import com.mongodb.connection.AsyncConnection
import com.mongodb.connection.Connection
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ServerVersion
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonJavaScript
import org.bson.BsonNull
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.client.model.Filters.gte
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.MILLISECONDS

class MapReduceToCollectionOperationSpecification extends OperationFunctionalSpecification {
    def mapReduceInputNamespace = new MongoNamespace(getDatabaseName(), 'mapReduceInput')
    def mapReduceOutputNamespace = new MongoNamespace(getDatabaseName(), 'mapReduceOutput')
    def mapReduceOperation = new MapReduceToCollectionOperation(mapReduceInputNamespace,
                                                                new BsonJavaScript('function(){ emit( this.name , 1 ); }'),
                                                                new BsonJavaScript('function(key, values){ return values.length; }'),
                                                                mapReduceOutputNamespace.getCollectionName())
    def expectedResults = [['_id': 'Pete', 'value': 2.0] as Document,
                           ['_id': 'Sam', 'value': 1.0] as Document]
    def helper = new CollectionHelper<Document>(new DocumentCodec(), mapReduceOutputNamespace)

    def setup() {
        CollectionHelper<Document> helper = new CollectionHelper<Document>(new DocumentCodec(), mapReduceInputNamespace)
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        helper.insertDocuments(new DocumentCodec(), pete, sam, pete2)
    }

    def cleanup() {
        new DropCollectionOperation(mapReduceInputNamespace).execute(getBinding())
        new DropCollectionOperation(mapReduceOutputNamespace).execute(getBinding())
    }

    def 'should have the correct defaults'() {
        given:
        def mapF = new BsonJavaScript('function(){ emit( "level" , 1 ); }')
        def reduceF = new BsonJavaScript('function(key, values){ return values.length; }')
        def out = 'outCollection'

        when:
        def operation =  new MapReduceToCollectionOperation(getNamespace(), mapF, reduceF, out)

        then:
        operation.getMapFunction() == mapF
        operation.getReduceFunction() == reduceF
        operation.getAction() == 'replace'
        operation.getCollectionName() == out
        operation.getWriteConcern() == null
        operation.getDatabaseName() == null
        operation.getFilter() == null
        operation.getFinalizeFunction() == null
        operation.getLimit() == 0
        operation.getScope() == null
        operation.getSort() == null
        operation.getMaxTime(MILLISECONDS) == 0
        operation.getBypassDocumentValidation() == null
        !operation.isJsMode()
        !operation.isVerbose()
        !operation.isSharded()
        !operation.isNonAtomic()
    }

    def 'should set optional values correctly'(){
        given:
        def mapF = new BsonJavaScript('function(){ emit( "level" , 1 ); }')
        def reduceF = new BsonJavaScript('function(key, values){ return values.length; }')
        def finalizeF = new BsonJavaScript('function(key, value) { return value }')
        def filter = BsonDocument.parse('{level: {$gte: 5}}')
        def sort = BsonDocument.parse('{level: 1}')
        def scope = BsonDocument.parse('{level: 1}')
        def out = 'outCollection'
        def action = 'merge'
        def dbName = 'dbName'
        def writeConcern = WriteConcern.MAJORITY

        when:
        def operation =  new MapReduceToCollectionOperation(getNamespace(), mapF, reduceF, out, writeConcern)
                .action(action).databaseName(dbName)
                .finalizeFunction(finalizeF).filter(filter).limit(10).scope(scope).sort(sort).maxTime(1, MILLISECONDS)
                .bypassDocumentValidation(true)

        then:
        operation.getMapFunction() == mapF
        operation.getReduceFunction() == reduceF
        operation.getAction() == action
        operation.getCollectionName() == out
        operation.getWriteConcern() == writeConcern
        operation.getDatabaseName() == dbName
        operation.getFilter() == filter
        operation.getLimit() == 10
        operation.getScope() == scope
        operation.getSort() == sort
        operation.getMaxTime(MILLISECONDS) == 1
        operation.getBypassDocumentValidation() == true
    }

    def 'should return the correct statistics and save the results'() {
        when:
        MapReduceStatistics results = mapReduceOperation.execute(getBinding())

        then:
        results.emitCount == 3
        results.inputCount == 3
        results.outputCount == 2
        helper.count() == 2
        helper.find() == expectedResults
    }

    @Category(Async)
    def 'should return the correct statistics and save the results asynchronously'() {

        when:
        MapReduceStatistics results = executeAsync(mapReduceOperation)

        then:
        results.emitCount == 3
        results.inputCount == 3
        results.outputCount == 2
        helper.count() == 2
        helper.find() == expectedResults
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 1, 8)) })
    def 'should support bypassDocumentValidation'() {
        given:
        def collectionOutHelper = getCollectionHelper(new MongoNamespace(getDatabaseName(), 'collectionOut'))
        collectionOutHelper.create('collectionOut', new CreateCollectionOptions().validationOptions(
                new ValidationOptions().validator(gte('level', 10))))
        getCollectionHelper().insertDocuments(new BsonDocument())

        when:
        def operation = new MapReduceToCollectionOperation(mapReduceInputNamespace,
                new BsonJavaScript('function(){ emit( "level" , 1 ); }'),
                new BsonJavaScript('function(key, values){ return values.length; }'),
                'collectionOut')
        operation.execute(getBinding())

        then:
        thrown(MongoCommandException)

        when:
        operation.bypassDocumentValidation(false).execute(getBinding())

        then:
        thrown(MongoCommandException)

        when:
        operation.bypassDocumentValidation(true).execute(getBinding())

        then:
        notThrown(MongoCommandException)

        cleanup:
        collectionOutHelper?.drop()
    }

    @Category(Async)
    @IgnoreIf({ !serverVersionAtLeast(asList(3, 1, 8)) })
    def 'should support bypassDocumentValidation asynchronously'() {
        given:
        def collectionOutHelper = getCollectionHelper(new MongoNamespace(getDatabaseName(), 'collectionOut'))
        collectionOutHelper.create('collectionOut', new CreateCollectionOptions().validationOptions(
                new ValidationOptions().validator(gte('level', 10))))
        getCollectionHelper().insertDocuments(new BsonDocument())

        when:
        def operation = new MapReduceToCollectionOperation(mapReduceInputNamespace,
                new BsonJavaScript('function(){ emit( "level" , 1 ); }'),
                new BsonJavaScript('function(key, values){ return values.length; }'),
                'collectionOut')
        executeAsync(operation)

        then:
        thrown(MongoCommandException)

        when:
        executeAsync(operation.bypassDocumentValidation(false))

        then:
        thrown(MongoCommandException)

        when:
        executeAsync(operation.bypassDocumentValidation(true))

        then:
        notThrown(MongoCommandException)

        cleanup:
        collectionOutHelper?.drop()
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 3, 8)) || !isDiscoverableReplicaSet() })
    def 'should throw on write concern error'() {
        given:
        getCollectionHelper().insertDocuments(new BsonDocument())
        def operation = new MapReduceToCollectionOperation(mapReduceInputNamespace,
                new BsonJavaScript('function(){ emit( "level" , 1 ); }'),
                new BsonJavaScript('function(key, values){ return values.length; }'),
                'collectionOut', new WriteConcern(5))

        when:
        async ? executeAsync(operation) : operation.execute(getBinding())

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 100
        ex.writeResult.wasAcknowledged()

        where:
        async << [true, false]
    }

    def 'should create the expected command'() {
        given:
        def connection = Mock(Connection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getServerVersion() >> serverVersion
            }
        }
        def connectionSource = Stub(ConnectionSource) {
            getConnection() >> connection
        }
        def writeBinding = Stub(WriteBinding) {
            getWriteConnectionSource() >> connectionSource
        }

        def cannedResults = BsonDocument.parse('''{result : "outCollection", timeMillis: 11,
                                                   counts: {input: 3, emit: 3, reduce: 1, output: 2 }, ok: 1.0 }''')
        def mapF = new BsonJavaScript('function(){ emit( "level" , 1 ); }')
        def reduceF = new BsonJavaScript('function(key, values){ return values.length; }')
        def finalizeF = new BsonJavaScript('function(key, value) { return value }')
        def filter = BsonDocument.parse('{level: {$gte: 5}}')
        def sort = BsonDocument.parse('{level: 1}')
        def scope = BsonDocument.parse('{level: 1}')
        def out = 'outCollection'
        def action = 'merge'
        def dbName = 'dbName'

        def expectedCommand = new BsonDocument('mapreduce', new BsonString(getCollectionName()))
                .append('map', mapF)
                .append('reduce', reduceF)
                .append('out', BsonDocument.parse('{replace: "outCollection", sharded: false, nonAtomic: false}'))
                .append('query', BsonNull.VALUE)
                .append('sort', BsonNull.VALUE)
                .append('finalize', BsonNull.VALUE)
                .append('scope', BsonNull.VALUE)
                .append('verbose', BsonBoolean.FALSE)

        if (includeWriteConcern) {
            expectedCommand.append('writeConcern', WriteConcern.MAJORITY.asDocument())
        }

        def operation =  new MapReduceToCollectionOperation(getNamespace(), mapF, reduceF, out, WriteConcern.MAJORITY)

        when:
        operation.execute(writeBinding)

        then:
        1 * connection.command(getNamespace().getDatabaseName(), expectedCommand, _, _, _) >> cannedResults
        1 * connection.release()

        when:
        operation.action(action)
                .databaseName(dbName)
                .finalizeFunction(finalizeF)
                .filter(filter)
                .limit(10)
                .scope(scope)
                .sort(sort)
                .maxTime(10, MILLISECONDS)
                .bypassDocumentValidation(true)
                .verbose(true)

        expectedCommand.append('out', BsonDocument.parse('{merge: "outCollection", sharded: false, nonAtomic: false, db: "dbName"}'))
                .append('query', filter)
                .append('sort', sort)
                .append('finalize', finalizeF)
                .append('scope', scope)
                .append('verbose', BsonBoolean.TRUE)
                .append('limit', new BsonInt32(10))
                .append('maxTimeMS', new BsonInt64(10))

        if (includeBypassValidation) {
            expectedCommand.append('bypassDocumentValidation', BsonBoolean.TRUE)
        }

        operation.execute(writeBinding)

        then:
        1 * connection.command(getNamespace().getDatabaseName(), expectedCommand, _, _, _) >> cannedResults
        1 * connection.release()

        where:
        serverVersion                   | includeBypassValidation | includeWriteConcern
        new ServerVersion([3, 4, 0])    | true                    | true
        new ServerVersion([3, 2, 0])    | true                    | false
        new ServerVersion([3, 0, 0])    | false                   | false
    }

    def 'should create the expected command asynchronously'() {
        given:
        def connection = Mock(AsyncConnection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getServerVersion() >> serverVersion
            }
        }
        def connectionSource = Stub(AsyncConnectionSource) {
            getConnection(_) >> { it[0].onResult(connection, null) }
        }
        def writeBinding = Stub(AsyncWriteBinding) {
            getWriteConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
        }
        def cannedResults = BsonDocument.parse('''{result : "outCollection", timeMillis: 11,
                                                   counts: {input: 3, emit: 3, reduce: 1, output: 2 }, ok: 1.0 }''')
        def mapF = new BsonJavaScript('function(){ emit( "level" , 1 ); }')
        def reduceF = new BsonJavaScript('function(key, values){ return values.length; }')
        def finalizeF = new BsonJavaScript('function(key, value) { return value }')
        def filter = BsonDocument.parse('{level: {$gte: 5}}')
        def sort = BsonDocument.parse('{level: 1}')
        def scope = BsonDocument.parse('{level: 1}')
        def out = 'outCollection'
        def action = 'merge'
        def dbName = 'dbName'

        def expectedCommand = new BsonDocument('mapreduce', new BsonString(getCollectionName()))
                .append('map', mapF)
                .append('reduce', reduceF)
                .append('out', BsonDocument.parse('{replace: "outCollection", sharded: false, nonAtomic: false}'))
                .append('query', BsonNull.VALUE)
                .append('sort', BsonNull.VALUE)
                .append('finalize', BsonNull.VALUE)
                .append('scope', BsonNull.VALUE)
                .append('verbose', BsonBoolean.FALSE)

        if (includeWriteConcern) {
            expectedCommand.append('writeConcern', WriteConcern.MAJORITY.asDocument())
        }

        def operation =  new MapReduceToCollectionOperation(getNamespace(), mapF, reduceF, out, WriteConcern.MAJORITY)

        when:
        operation.executeAsync(writeBinding, Stub(SingleResultCallback))

        then:
        1 * connection.commandAsync(getNamespace().getDatabaseName(), expectedCommand, _, _, _, _) >> {
            it[5].onResult(cannedResults, null)
        }
        1 * connection.release()

        when:
        operation.action(action)
                .databaseName(dbName)
                .finalizeFunction(finalizeF)
                .filter(filter)
                .limit(10)
                .scope(scope)
                .sort(sort)
                .maxTime(10, MILLISECONDS)
                .bypassDocumentValidation(true)
                .verbose(true)

        expectedCommand.append('out', BsonDocument.parse('{merge: "outCollection", sharded: false, nonAtomic: false, db: "dbName"}'))
                .append('query', filter)
                .append('sort', sort)
                .append('finalize', finalizeF)
                .append('scope', scope)
                .append('verbose', BsonBoolean.TRUE)
                .append('limit', new BsonInt32(10))
                .append('maxTimeMS', new BsonInt64(10))

        if (includeBypassValidation) {
            expectedCommand.append('bypassDocumentValidation', BsonBoolean.TRUE)
        }

        operation.executeAsync(writeBinding, Stub(SingleResultCallback))

        then:
        1 * connection.commandAsync(getNamespace().getDatabaseName(), expectedCommand, _, _, _, _) >> {
            it[5].onResult(cannedResults, null)
        }
        1 * connection.release()

        where:
        serverVersion                   | includeBypassValidation | includeWriteConcern
        new ServerVersion([3, 4, 0])    | true                    | true
        new ServerVersion([3, 2, 0])    | true                    | false
        new ServerVersion([3, 0, 0])    | false                   | false
    }

}
