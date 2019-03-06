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

import static Fixture.getMongoClient
import static com.mongodb.CustomMatchers.isTheSameAs
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
                                                                .filter(new BsonDocument())
                                                                .projection(new BsonDocument())
                                                                .retryReads(true))
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
                                                                .limit(-1)
                                                                .filter(new BsonDocument())
                                                                .projection(new BsonDocument())
                                                                .retryReads(true))
    }

    def 'DBCursor methods should be used to create the expected operation'() {
        given:
        def executor = new TestOperationExecutor([stubBatchCursor()]);
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        def collation = Collation.builder().locale('en').build()
        def cursorType = CursorType.NonTailable
        def filter = new BasicDBObject()
        def sort = BasicDBObject.parse('{a: 1}')
        def bsonFilter = new BsonDocument()
        def bsonSort = BsonDocument.parse(sort.toJson())
        def readConcern = ReadConcern.LOCAL
        def readPreference = ReadPreference.nearest()
        def findOptions = new DBCollectionFindOptions()
        def cursor = new DBCursor(collection, filter, findOptions)
                .setReadConcern(readConcern)
                .setReadPreference(readPreference)
                .setCollation(collation)
                .batchSize(1)
                .cursorType(cursorType)
                .limit(1)
                .maxTime(1, TimeUnit.MILLISECONDS)
                .noCursorTimeout(true)
                .oplogReplay(true)
                .partial(true)
                .skip(1)
                .sort(sort)

        when:
        cursor.toArray()

        then:
        expect executor.getReadOperation(), isTheSameAs(new FindOperation(collection.getNamespace(), collection.getObjectCodec())
                .batchSize(1)
                .collation(collation)
                .cursorType(cursorType)
                .filter(bsonFilter)
                .limit(1)
                .maxTime(1, TimeUnit.MILLISECONDS)
                .noCursorTimeout(true)
                .oplogReplay(true)
                .partial(true)
                .skip(1)
                .sort(bsonSort)
                .retryReads(true)
        )

        executor.getReadPreference() == readPreference
        executor.getReadConcern() == readConcern
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
        def bsonFilter = new BsonDocument()
        def bsonProjection = BsonDocument.parse(projection.toJson())
        def bsonSort = BsonDocument.parse(sort.toJson())
        def comment = 'comment'
        def hint = BasicDBObject.parse('{x : 1}')
        def min = BasicDBObject.parse('{y : 1}')
        def max = BasicDBObject.parse('{y : 100}')
        def bsonHint = BsonDocument.parse(hint.toJson())
        def bsonMin = BsonDocument.parse(min.toJson())
        def bsonMax = BsonDocument.parse(max.toJson())
        def readConcern = ReadConcern.LOCAL
        def readPreference = ReadPreference.nearest()
        def findOptions = new DBCollectionFindOptions()
                .batchSize(1)
                .collation(collation)
                .cursorType(cursorType)
                .limit(1)
                .maxAwaitTime(1, TimeUnit.MILLISECONDS)
                .maxTime(1, TimeUnit.MILLISECONDS)
                .noCursorTimeout(true)
                .oplogReplay(true)
                .partial(true)
                .projection(projection)
                .readConcern(readConcern)
                .readPreference(readPreference)
                .skip(1)
                .sort(sort)
                .comment(comment)
                .hint(hint)
                .max(max)
                .min(min)
                .returnKey(true)
                .showRecordId(true)

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
                .noCursorTimeout(true)
                .oplogReplay(true)
                .partial(true)
                .projection(bsonProjection)
                .skip(1)
                .sort(bsonSort)
                .comment(comment)
                .hint(bsonHint)
                .max(bsonMax)
                .min(bsonMin)
                .returnKey(true)
                .showRecordId(true)
                .retryReads(true)
        )

        executor.getReadPreference() == findOptions.getReadPreference()
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
                                                                .filter(new BsonDocument()).retryReads(true))
        executor.getReadConcern() == ReadConcern.MAJORITY
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
                                                                .filter(new BsonDocument()).retryReads(true))
        executor.getReadConcern() == ReadConcern.MAJORITY
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
