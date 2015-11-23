/*
 * Copyright 2015 MongoDB, Inc.
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

import com.mongodb.operation.BatchCursor
import com.mongodb.operation.CountOperation
import com.mongodb.operation.FindOperation
import org.bson.BsonDocument
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.Fixture.getMongoClient
import static spock.util.matcher.HamcrestSupport.expect


class DBCursorSpecification extends Specification {

    def 'should get and set read preference'() {
        when:
        def collection = new DB(getMongoClient(), 'myDatabase', new TestOperationExecutor([])).getCollection('test')
        collection.setReadPreference(ReadPreference.nearest())
        def cursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.nearest())

        then:
        cursor.readPreference == ReadPreference.nearest()

        when:
        cursor.setReadPreference(ReadPreference.secondary())

        then:
        cursor.readPreference == ReadPreference.secondary()

        when:
        cursor.setReadPreference(null)

        then:
        cursor.readPreference == ReadPreference.nearest()
    }

    def 'should get and set read concern'() {
        when:
        def collection = new DB(getMongoClient(), 'myDatabase', new TestOperationExecutor([])).getCollection('test')
        collection.setReadConcern(ReadConcern.MAJORITY)
        def cursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary())

        then:
        cursor.readConcern == ReadConcern.MAJORITY

        when:
        cursor.setReadConcern(ReadConcern.LOCAL)

        then:
        cursor.readConcern == ReadConcern.LOCAL

        when:
        cursor.setReadConcern(null)

        then:
        cursor.readConcern == ReadConcern.MAJORITY
    }

    def 'find should create the correct FindOperation'() {
        given:
        def dbObject = new BasicDBObject('_id', 1)
        def executor = new TestOperationExecutor([Stub(BatchCursor) {
            def count = 0
            next() >> {
                count++
                [dbObject]
            }
            hasNext() >> {
                count == 0
            }
            getServerCursor() >> {
                null
            }
        }]);
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        def cursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary())
        cursor.setReadConcern(ReadConcern.MAJORITY)

        when:
        def results = cursor.toArray()

        then:
        results == [dbObject]
        expect executor.getReadOperation(), isTheSameAs(new FindOperation(collection.getNamespace(), collection.getObjectCodec())
                                                                .readConcern(ReadConcern.MAJORITY)
                                                                .filter(new BsonDocument())
                                                                .projection(new BsonDocument())
                                                                .modifiers(new BsonDocument()))
    }

    def 'one should create the correct FindOperation'() {
        given:
        def dbObject = new BasicDBObject('_id', 1)
        def executor = new TestOperationExecutor([Stub(BatchCursor) {
            def count = 0
            next() >> {
                count++
                [dbObject]
            }
            hasNext() >> {
                count == 0
            }
        }]);
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        def cursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary())
        cursor.setReadConcern(ReadConcern.MAJORITY)

        when:
        def result = cursor.one()

        then:
        result == dbObject
        expect executor.getReadOperation(), isTheSameAs(new FindOperation(collection.getNamespace(), collection.getObjectCodec())
                                                                .readConcern(ReadConcern.MAJORITY)
                                                                .filter(new BsonDocument())
                                                                .limit(-1)
                                                                .projection(new BsonDocument()))
    }

    def 'count should create the correct CountOperation'() {
        def executor = new TestOperationExecutor([42L]);
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        def cursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary())
        cursor.setReadConcern(ReadConcern.MAJORITY)

        when:
        def result = cursor.count()

        then:
        result == 42
        expect executor.getReadOperation(), isTheSameAs(new CountOperation(collection.getNamespace())
                                                                .readConcern(ReadConcern.MAJORITY)
                                                                .filter(new BsonDocument()))
    }

    def 'size should create the correct CountOperation'() {
        def executor = new TestOperationExecutor([42L]);
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        def cursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary())
        cursor.setReadConcern(ReadConcern.MAJORITY)

        when:
        def result = cursor.size()

        then:
        result == 42
        expect executor.getReadOperation(), isTheSameAs(new CountOperation(collection.getNamespace())
                                                                .readConcern(ReadConcern.MAJORITY)
                                                                .filter(new BsonDocument()))
    }
}
