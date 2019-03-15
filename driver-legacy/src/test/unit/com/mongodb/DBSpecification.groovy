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

package com.mongodb


import com.mongodb.client.internal.TestOperationExecutor
import com.mongodb.client.model.Collation
import com.mongodb.client.model.CollationAlternate
import com.mongodb.client.model.CollationCaseFirst
import com.mongodb.client.model.CollationMaxVariable
import com.mongodb.client.model.CollationStrength
import com.mongodb.client.model.DBCreateViewOptions
import com.mongodb.client.model.ValidationAction
import com.mongodb.client.model.ValidationLevel
import com.mongodb.operation.BatchCursor
import com.mongodb.operation.CreateCollectionOperation
import com.mongodb.operation.CreateViewOperation
import com.mongodb.operation.ListCollectionsOperation
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonDouble
import spock.lang.Specification

import static Fixture.getMongoClient
import static com.mongodb.CustomMatchers.isTheSameAs
import static spock.util.matcher.HamcrestSupport.expect

class DBSpecification extends Specification {

    def 'should throw IllegalArgumentException if name is invalid'() {
        when:
        new DB(getMongoClient(), 'a.b', new TestOperationExecutor())

        then:
        thrown(IllegalArgumentException)
    }

    def 'should get and set read concern'() {
        when:
        def db = new DB(getMongoClient(), 'test', new TestOperationExecutor([]))

        then:
        db.readConcern == ReadConcern.DEFAULT

        when:
        db.setReadConcern(ReadConcern.MAJORITY)

        then:
        db.readConcern == ReadConcern.MAJORITY

        when:
        db.setReadConcern(null)

        then:
        db.readConcern == ReadConcern.DEFAULT
    }

    def 'should execute CreateCollectionOperation'() {
        given:
        def mongo = Stub(MongoClient)
        mongo.mongoClientOptions >> MongoClientOptions.builder().build()
        def executor = new TestOperationExecutor([1L, 2L, 3L])
        def db = new DB(mongo, 'test', executor)
        db.setReadConcern(ReadConcern.MAJORITY)
        db.setWriteConcern(WriteConcern.MAJORITY)

        when:
        db.createCollection('ctest', new BasicDBObject())

        then:
        def operation = executor.getWriteOperation() as CreateCollectionOperation
        expect operation, isTheSameAs(new CreateCollectionOperation('test', 'ctest', db.getWriteConcern()))
        executor.getReadConcern() == ReadConcern.MAJORITY

        when:
        def options = new BasicDBObject()
                .append('size', 100000)
                .append('max', 2000)
                .append('capped', true)
                .append('autoIndexId', true)
                .append('storageEngine', BasicDBObject.parse('{ wiredTiger: {}}'))
                .append('indexOptionDefaults', BasicDBObject.parse('{storageEngine: { mmapv1: {}}}'))
                .append('validator', BasicDBObject.parse('{level : { $gte: 10 } }'))
                .append('validationLevel', ValidationLevel.MODERATE.getValue())
                .append('validationAction', ValidationAction.WARN.getValue())


        db.createCollection('ctest', options)
        operation = executor.getWriteOperation() as CreateCollectionOperation

        then:
        expect operation, isTheSameAs(new CreateCollectionOperation('test', 'ctest', db.getWriteConcern())
                .sizeInBytes(100000)
                .maxDocuments(2000)
                .capped(true)
                .autoIndex(true)
                .storageEngineOptions(BsonDocument.parse('{ wiredTiger: {}}'))
                .indexOptionDefaults(BsonDocument.parse('{storageEngine: { mmapv1: {}}}'))
                .validator(BsonDocument.parse('{level : { $gte: 10 } }'))
                .validationLevel(ValidationLevel.MODERATE)
                .validationAction(ValidationAction.WARN))
        executor.getReadConcern() == ReadConcern.MAJORITY

        when:
        def collation = Collation.builder()
                .locale('en')
                .caseLevel(true)
                .collationCaseFirst(CollationCaseFirst.OFF)
                .collationStrength(CollationStrength.IDENTICAL)
                .numericOrdering(true)
                .collationAlternate(CollationAlternate.SHIFTED)
                .collationMaxVariable(CollationMaxVariable.SPACE)
                .backwards(true)
                .build()

        db.createCollection('ctest', new BasicDBObject('collation', BasicDBObject.parse(collation.asDocument().toJson())))
        operation = executor.getWriteOperation() as CreateCollectionOperation

        then:
        expect operation, isTheSameAs(new CreateCollectionOperation('test', 'ctest', db.getWriteConcern()).collation(collation))
        executor.getReadConcern() == ReadConcern.MAJORITY
    }

    def 'should execute CreateViewOperation'() {
        given:
        def mongo = Stub(MongoClient) {
            getCodecRegistry() >> MongoClient.defaultCodecRegistry
        }
        mongo.mongoClientOptions >> MongoClientOptions.builder().build()
        def executor = new TestOperationExecutor([1L, 2L, 3L])

        def databaseName = 'test'
        def viewName = 'view1'
        def viewOn = 'collection1'
        def pipeline = [new BasicDBObject('$match', new BasicDBObject('x', true))]
        def writeConcern = WriteConcern.JOURNALED
        def collation = Collation.builder().locale('en').build()

        def db = new DB(mongo, databaseName, executor)
        db.setWriteConcern(writeConcern)
        db.setReadConcern(ReadConcern.MAJORITY)

        when:
        db.createView(viewName, viewOn, pipeline)

        then:
        def operation = executor.getWriteOperation() as CreateViewOperation
        expect operation, isTheSameAs(new CreateViewOperation(databaseName, viewName, viewOn,
                [new BsonDocument('$match', new BsonDocument('x', BsonBoolean.TRUE))], writeConcern))
        executor.getReadConcern() == ReadConcern.MAJORITY

        when:
        db.createView(viewName, viewOn, pipeline, new DBCreateViewOptions().collation(collation))
        operation = executor.getWriteOperation() as CreateViewOperation

        then:
        expect operation, isTheSameAs(new CreateViewOperation(databaseName, viewName, viewOn,
                [new BsonDocument('$match', new BsonDocument('x', BsonBoolean.TRUE))], writeConcern).collation(collation))
        executor.getReadConcern() == ReadConcern.MAJORITY
    }

    def 'should execute ListCollectionsOperation'() {
        given:
        def mongo = Stub(MongoClient)
        mongo.mongoClientOptions >> MongoClientOptions.builder().build()
        def executor = new TestOperationExecutor([Stub(BatchCursor), Stub(BatchCursor)])

        def databaseName = 'test'

        def db = new DB(mongo, databaseName, executor)

        when:
        db.getCollectionNames()
        def operation = executor.getReadOperation() as ListCollectionsOperation

        then:
        expect operation, isTheSameAs(new ListCollectionsOperation(databaseName, MongoClient.getCommandCodec()).nameOnly(true))

        when:
        db.collectionExists('someCollection')
        operation = executor.getReadOperation() as ListCollectionsOperation

        then:
        expect operation, isTheSameAs(new ListCollectionsOperation(databaseName, MongoClient.getCommandCodec()).nameOnly(true))
    }

    def 'should use provided read preference for obedient commands'() {
        given:
        def mongo = Stub(MongoClient)
        mongo.mongoClientOptions >> MongoClientOptions.builder().build()
        def executor = new TestOperationExecutor([new BsonDocument('ok', new BsonDouble(1.0))])
        def database = new DB(mongo, 'test', executor)
        database.setReadPreference(ReadPreference.secondary())
        database.setReadConcern(ReadConcern.MAJORITY)

        when:
        database.command(cmd)

        then:
        executor.getReadPreference() == expectedReadPreference
        executor.getReadConcern() == ReadConcern.MAJORITY

        where:
        expectedReadPreference     | cmd
        ReadPreference.secondary() | new BasicDBObject('listCollections', 1)
        ReadPreference.secondary() | new BasicDBObject('collStats', 1)
        ReadPreference.secondary() | new BasicDBObject('dbStats', 1)
        ReadPreference.secondary() | new BasicDBObject('distinct', 1)
        ReadPreference.secondary() | new BasicDBObject('geoNear', 1)
        ReadPreference.secondary() | new BasicDBObject('geoSearch', 1)
        ReadPreference.secondary() | new BasicDBObject('group', 1)
        ReadPreference.secondary() | new BasicDBObject('listCollections', 1)
        ReadPreference.secondary() | new BasicDBObject('listIndexes', 1)
        ReadPreference.secondary() | new BasicDBObject('parallelCollectionScan', 1)
        ReadPreference.secondary() | new BasicDBObject('text', 1)
    }

    def 'should use primary read preference for non obedient commands'() {
        given:
        def mongo = Stub(MongoClient)
        mongo.mongoClientOptions >> MongoClientOptions.builder().build()
        def executor = new TestOperationExecutor([new BsonDocument('ok', new BsonDouble(1.0))])
        def database = new DB(mongo, 'test', executor)
        database.setReadPreference(ReadPreference.secondary())
        database.setReadConcern(ReadConcern.MAJORITY)

        when:
        database.command(cmd)

        then:
        executor.getReadPreference() == expectedReadPreference
        executor.getReadConcern() == ReadConcern.MAJORITY

        where:
        expectedReadPreference      | cmd
        ReadPreference.primary()    | new BasicDBObject('command', 1)
    }
}
