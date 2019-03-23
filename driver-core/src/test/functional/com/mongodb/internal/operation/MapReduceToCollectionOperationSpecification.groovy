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

import com.mongodb.MongoCommandException
import com.mongodb.MongoNamespace
import com.mongodb.MongoWriteConcernException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadPreference
import com.mongodb.WriteConcern
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.model.ValidationOptions
import com.mongodb.client.test.CollectionHelper
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonJavaScript
import org.bson.BsonNull
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.DocumentCodec
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.client.model.Filters.gte
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
        operation.getCollation() == null
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
                .action(action)
                .databaseName(dbName)
                .finalizeFunction(finalizeF)
                .filter(filter)
                .limit(10)
                .scope(scope)
                .sort(sort)
                .maxTime(1, MILLISECONDS)
                .bypassDocumentValidation(true)
                .collation(defaultCollation)

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
        operation.getCollation() == defaultCollation
    }

    def 'should return the correct statistics and save the results'() {
        when:
        MapReduceStatistics results = execute(mapReduceOperation, async)

        then:
        results.emitCount == 3
        results.inputCount == 3
        results.outputCount == 2
        helper.count() == 2
        helper.find() == expectedResults

        where:
        async << [true, false]
    }


    @IgnoreIf({ !serverVersionAtLeast(3, 2) })
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
        execute(operation, async)

        then:
        thrown(MongoCommandException)

        when:
        operation.bypassDocumentValidation(false)
        execute(operation, async)

        then:
        thrown(MongoCommandException)

        when:
        operation.bypassDocumentValidation(true)
        execute(operation, async)

        then:
        notThrown(MongoCommandException)

        cleanup:
        collectionOutHelper?.drop()

        where:
        async << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) || !isDiscoverableReplicaSet() })
    def 'should throw on write concern error'() {
        given:
        getCollectionHelper().insertDocuments(new BsonDocument())
        def operation = new MapReduceToCollectionOperation(mapReduceInputNamespace,
                new BsonJavaScript('function(){ emit( "level" , 1 ); }'),
                new BsonJavaScript('function(key, values){ return values.length; }'),
                'collectionOut', new WriteConcern(5))

        when:
        execute(operation, async)

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 100
        ex.writeResult.wasAcknowledged()

        where:
        async << [true, false]
    }

    def 'should create the expected command'() {
        given:
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

        when:
        def operation =  new MapReduceToCollectionOperation(getNamespace(), mapF, reduceF, out, WriteConcern.MAJORITY)
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

        then:
        testOperation(operation, serverVersion, expectedCommand, async, cannedResults, true, false,
                ReadPreference.primary(), false)

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

        if (includeCollation) {
            operation.collation(defaultCollation)
            expectedCommand.append('collation', defaultCollation.asDocument())
        }
        if (includeBypassValidation) {
            expectedCommand.append('bypassDocumentValidation', BsonBoolean.TRUE)
        }

        then:
        testOperation(operation, serverVersion, expectedCommand, async, cannedResults, true, false,
                ReadPreference.primary(), false)

        where:
        serverVersion | includeBypassValidation | includeWriteConcern | includeCollation | async
        [3, 4, 0]     | true                    | true                | true             | true
        [3, 4, 0]     | true                    | true                | true             | false
        [3, 2, 0]     | true                    | false               | false            | true
        [3, 2, 0]     | true                    | false               | false            | false
        [3, 0, 0]     | false                   | false               | false            | true
        [3, 0, 0]     | false                   | false               | false            | false
    }

    def 'should throw an exception when passing an unsupported collation'() {
        given:
        def operation = mapReduceOperation.collation(defaultCollation)

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
        def outCollectionHelper = getCollectionHelper(new MongoNamespace(mapReduceInputNamespace.getDatabaseName(), 'collectionOut'))
        outCollectionHelper.drop()

        def document = Document.parse('{_id: 1, str: "foo"}')
        getCollectionHelper(mapReduceInputNamespace).insertDocuments(document)
        def operation = new MapReduceToCollectionOperation(mapReduceInputNamespace,
                new BsonJavaScript('function(){ emit( this._id, this.str ); }'),
                new BsonJavaScript('function(key, values){ return values; }'),
                'collectionOut')
                .filter(BsonDocument.parse('{str: "FOO"}'))
                .collation(caseInsensitiveCollation)

        when:
        execute(operation, async)

        then:
        outCollectionHelper.count() == 1

        where:
        async << [true, false]
    }

}
