/*
 * Copyright 2014-2016 MongoDB, Inc.
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

import com.mongodb.client.model.ValidationAction
import com.mongodb.client.model.ValidationLevel
import com.mongodb.operation.CreateCollectionOperation
import org.bson.BsonDocument
import org.bson.BsonDouble
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.Fixture.getMongoClient
import static spock.util.matcher.HamcrestSupport.expect

class DBSpecification extends Specification {

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
        def mongo = Stub(Mongo)
        mongo.mongoClientOptions >> MongoClientOptions.builder().build()
        def executor = new TestOperationExecutor([1L, 2L, 3L])
        def db = new DB(mongo, 'test', executor)

        when:
        db.createCollection('ctest', new BasicDBObject())

        then:
        def operation = executor.getWriteOperation() as CreateCollectionOperation
        expect operation, isTheSameAs(new CreateCollectionOperation('test', 'ctest', db.getWriteConcern()))

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
    }


    def 'should use provided read preference for obedient commands'() {
        given:
        def mongo = Stub(Mongo)
        mongo.mongoClientOptions >> MongoClientOptions.builder().build()
        def executor = new TestOperationExecutor([new BsonDocument('ok', new BsonDouble(1.0))])
        def database = new DB(mongo, 'test', executor)
        database.setReadPreference(ReadPreference.secondary())

        when:
        database.command(cmd)

        then:
        executor.getReadPreference() == expectedReadPreference

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
        def mongo = Stub(Mongo)
        mongo.mongoClientOptions >> MongoClientOptions.builder().build()
        def executor = new TestOperationExecutor([new BsonDocument('ok', new BsonDouble(1.0))])
        def database = new DB(mongo, 'test', executor)
        database.setReadPreference(ReadPreference.secondary())

        when:
        database.command(cmd)

        then:
        executor.getReadPreference() == expectedReadPreference

        where:
        expectedReadPreference      | cmd
        ReadPreference.primary()    | new BasicDBObject('command', 1)
    }
}
