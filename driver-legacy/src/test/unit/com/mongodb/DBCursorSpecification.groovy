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
import com.mongodb.client.model.DBCollectionFindOptions
import com.mongodb.operation.BatchCursor
import com.mongodb.operation.CountOperation
import com.mongodb.operation.FindOperation
import org.bson.BsonDocument
import spock.lang.Specification

import java.util.concurrent.TimeUnit

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

    def 'should get and set collation'() {
        when:
        def collection = new DB(getMongoClient(), 'myDatabase', new TestOperationExecutor([])).getCollection('test')
        def collation = Collation.builder().locale('en').build()
        def cursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary())

        then:
        cursor.getCollation() == null

        when:
        cursor.setCollation(collation)

        then:
        cursor.getCollation() == collation

        when:
        cursor.setCollation(null)

        then:
        cursor.getCollation() == null
    }

    def 'should copy as expected'() {
        when:
        def collection = new DB(getMongoClient(), 'myDatabase', new TestOperationExecutor([])).getCollection('test')
        def cursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.nearest())
            .setReadConcern(ReadConcern.LOCAL)
            .setCollation(Collation.builder().locale('en').build())

        then:
        expect(cursor, isTheSameAs(cursor.copy()))
    }

    def 'find should create the correct FindOperation'() {
        given:
        def executor = new TestOperationExecutor([stubBatchCursor()]);
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        def cursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary())
        cursor.setReadConcern(ReadConcern.MAJORITY)

        when:
        cursor.toArray()

        then:
        expect executor.getReadOperation(), isTheSameAs(new FindOperation(collection.getNamespace(), collection.getObjectCodec())
                                                                .readConcern(ReadConcern.MAJORITY)
                                                                .filter(new BsonDocument())
                                                                .projection(new BsonDocument())
                                                                .modifiers(new BsonDocument()))
    }


    def 'one should create the correct FindOperation'() {
        given:
        def executor = new TestOperationExecutor([stubBatchCursor()]);
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        def cursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary())
        cursor.setReadConcern(ReadConcern.MAJORITY)

        when:
        cursor.one()

        then:
        expect executor.getReadOperation(), isTheSameAs(new FindOperation(collection.getNamespace(), collection.getObjectCodec())
                                                                .readConcern(ReadConcern.MAJORITY)
                                                                .limit(-1)
                                                                .filter(new BsonDocument())
                                                                .projection(new BsonDocument())
                                                                .modifiers(new BsonDocument()))
    }

    def 'DBCollectionFindOptions should be used to create the expected operation'() {
        given:
        def executor = new TestOperationExecutor([stubBatchCursor()]);
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        def collation = Collation.builder().locale('en').build()
        def cursorType = CursorType.NonTailable
        def filter = new BasicDBObject()
        def projection = BasicDBObject.parse('{a: 1, _id: 0}')
        def sort = BasicDBObject.parse('{a: 1}')
        def modifiers = BasicDBObject.parse('{$comment: 1}')
        def bsonFilter = new BsonDocument()
        def bsonModifiers = BsonDocument.parse(modifiers.toJson())
        def bsonProjection = BsonDocument.parse(projection.toJson())
        def bsonSort = BsonDocument.parse(sort.toJson())
        def readConcern = ReadConcern.LOCAL
        def readPreference = ReadPreference.nearest()
        def findOptions = new DBCollectionFindOptions()
                .batchSize(1)
                .collation(collation)
                .cursorType(cursorType)
                .limit(1)
                .maxAwaitTime(1, TimeUnit.MILLISECONDS)
                .maxTime(1, TimeUnit.MILLISECONDS)
                .modifiers(modifiers)
                .noCursorTimeout(true)
                .oplogReplay(true)
                .partial(true)
                .projection(projection)
                .readConcern(readConcern)
                .readPreference(readPreference)
                .skip(1)
                .sort(sort)
        def cursor = new DBCursor(collection, filter, findOptions)

        when:
        cursor.toArray()

        then:
        expect executor.getReadOperation(), isTheSameAs(new FindOperation(collection.getNamespace(), collection.getObjectCodec())
                .batchSize(1)
                .collation(collation)
                .cursorType(cursorType)
                .filter(bsonFilter)
                .limit(1)
                .maxAwaitTime(1, TimeUnit.MILLISECONDS)
                .maxTime(1, TimeUnit.MILLISECONDS)
                .modifiers(bsonModifiers)
                .noCursorTimeout(true)
                .oplogReplay(true)
                .partial(true)
                .projection(bsonProjection)
                .readConcern(readConcern)
                .skip(1)
                .sort(bsonSort)
        )

        executor.getReadPreference() == findOptions.getReadPreference()
    }

    def 'DBCursor options should override DBCollectionFindOptions'() {
        given:
        def executor = new TestOperationExecutor([stubBatchCursor()]);
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        def cursorType = CursorType.NonTailable
        def readPreference = ReadPreference.primary()
        def filter = new BasicDBObject()
        def bsonFilter = new BsonDocument()
        def findOptions = new DBCollectionFindOptions()
                .cursorType(cursorType)
                .noCursorTimeout(false)
                .oplogReplay(false)
                .partial(false)
                .readPreference(readPreference)

        def cursor = new DBCursor(collection, filter , findOptions)
                .addOption(Bytes.QUERYOPTION_AWAITDATA)
                .addOption(Bytes.QUERYOPTION_NOTIMEOUT)
                .addOption(Bytes.QUERYOPTION_OPLOGREPLAY)
                .addOption(Bytes.QUERYOPTION_PARTIAL)
                .addOption(Bytes.QUERYOPTION_SLAVEOK)
                .addOption(Bytes.QUERYOPTION_TAILABLE)

        when:
        cursor.toArray()

        then:
        expect executor.getReadOperation(), isTheSameAs(new FindOperation(collection.getNamespace(), collection.getObjectCodec())
                .modifiers(new BsonDocument())
                .cursorType(CursorType.TailableAwait)
                .filter(bsonFilter)
                .noCursorTimeout(true)
                .oplogReplay(true)
                .partial(true)
        )

        executor.getReadPreference() == ReadPreference.secondaryPreferred()
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

    private stubBatchCursor() {
        Stub(BatchCursor) {
            def count = 0
            next() >> {
                count++
                [new BasicDBObject('_id', 1)]
            }
            hasNext() >> {
                count == 0
            }
            getServerCursor() >> new ServerCursor(12L, new ServerAddress())
        };
    }
}
