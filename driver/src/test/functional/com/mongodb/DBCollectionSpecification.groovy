/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import com.mongodb.bulk.IndexRequest
import com.mongodb.operation.AggregateOperation
import com.mongodb.operation.BatchCursor
import com.mongodb.operation.CommandReadOperation
import com.mongodb.operation.CountOperation
import com.mongodb.operation.CreateIndexesOperation
import com.mongodb.operation.DistinctOperation
import com.mongodb.operation.FindOperation
import com.mongodb.operation.MapReduceBatchCursor
import com.mongodb.operation.MapReduceWithInlineResultsOperation
import com.mongodb.operation.ParallelCollectionScanOperation
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonJavaScript
import org.bson.BsonString
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.BsonValueCodec
import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.Fixture.getMongoClient
import static spock.util.matcher.HamcrestSupport.expect

class DBCollectionSpecification extends Specification {

    def 'should get and set read concern'() {
        when:
        def db = new DB(getMongoClient(), 'myDatabase', new TestOperationExecutor([]))
        db.setReadConcern(ReadConcern.MAJORITY)
        def collection = db.getCollection('test')

        then:
        collection.readConcern == ReadConcern.MAJORITY

        when:
        collection.setReadConcern(ReadConcern.LOCAL)

        then:
        collection.readConcern == ReadConcern.LOCAL

        when:
        collection.setReadConcern(null)

        then:
        collection.readConcern == ReadConcern.MAJORITY
    }

    def 'should use CreateIndexOperation properly'() {

        given:
        def executor = new TestOperationExecutor([null, null]);
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        def keys = new BasicDBObject('a', 1);

        when:
        collection.createIndex(keys)
        def request = (executor.getWriteOperation() as CreateIndexesOperation).requests[0]

        then:
        expect request, isTheSameAs(new IndexRequest(new BsonDocument('a', new BsonInt32(1))))

        when:
        def storageEngine = '{ wiredTiger: { configString: "block_compressor=zlib" }}'
        def partialFilterExpression = '{ a: { $gte: 10 } }'
        collection.createIndex(keys, new BasicDBObject(['background': true, 'unique': true, 'sparse': true, 'name': 'aIndex',
                                                      'expireAfterSeconds': 100, 'v': 1, 'weights': new BasicDBObject(['a': 1000]),
                                                      'default_language': 'es', 'language_override': 'language', 'textIndexVersion': 1,
                                                      '2dsphereIndexVersion': 1, 'bits': 1, 'min': new Double(-180.0),
                                                      'max'          : new Double(180.0), 'bucketSize': new Double(200.0), 'dropDups': true,
                                                      'storageEngine': BasicDBObject.parse(storageEngine),
                                                      'partialFilterExpression':  BasicDBObject.parse(partialFilterExpression)]))

        request = (executor.getWriteOperation() as CreateIndexesOperation).requests[0]

        then:
        expect request, isTheSameAs(new IndexRequest(new BsonDocument('a', new BsonInt32(1)))
                                            .background(true)
                                            .unique(true)
                                            .sparse(true)
                                            .name('aIndex')
                                            .expireAfter(100, TimeUnit.SECONDS)
                                            .version(1)
                                            .weights(new BsonDocument('a', new BsonInt32(1000)))
                                            .defaultLanguage('es')
                                            .languageOverride('language')
                                            .textVersion(1)
                                            .sphereVersion(1)
                                            .bits(1)
                                            .min(-180.0)
                                            .max(180.0)
                                            .bucketSize(200.0)
                                            .dropDups(true)
                                            .storageEngine(BsonDocument.parse(storageEngine))
                                            .partialFilterExpression(BsonDocument.parse(partialFilterExpression)))
    }

    def 'should support boolean index options that are numbers'() {
        given:
        def executor = new TestOperationExecutor([null, null]);
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        def options = new BasicDBObject('sparse', value);

        when:
        collection.createIndex(new BasicDBObject('y', 1), options);

        then:
        def operation = executor.getWriteOperation() as CreateIndexesOperation
        operation.requests[0].sparse == expectedValue

        where:
        value | expectedValue
        0     | false
        0F    | false
        0D    | false
        1     | true
        -1    | true
        4L    | true
        4.3F  | true
        4.0D  | true
    }

    def 'should support integer index options that are numbers'() {
        given:
        def executor = new TestOperationExecutor([null, null]);
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        def options = new BasicDBObject('expireAfterSeconds', integerValue);

        when:
        collection.createIndex(new BasicDBObject('y', 1), options);

        then:
        def operation = executor.getWriteOperation() as CreateIndexesOperation
        operation.requests[0].getExpireAfter(TimeUnit.SECONDS) == integerValue

        where:
        integerValue << [4, 4L, (double) 4.0]
    }

    def 'should support double index options that are numbers'() {
        given:
        def executor = new TestOperationExecutor([null, null]);
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        def options = new BasicDBObject('max', doubleValue);

        when:
        collection.createIndex(new BasicDBObject('y', '2d'), options);

        then:
        def operation = executor.getWriteOperation() as CreateIndexesOperation
        operation.requests[0].max == doubleValue

        where:
        doubleValue << [4, 4L, (double) 4.0]
    }

    def 'should throw IllegalArgumentException for unsupported option value type'() {
        given:
        def executor = new TestOperationExecutor([null, null]);
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        def options = new BasicDBObject('sparse', 'true');


        when:
        collection.createIndex(new BasicDBObject('y', '1'), options);

        then:
        thrown(IllegalArgumentException)
    }

    def 'getStats should execute the expected command with the collection default read preference'() {
        given:
        def executor = new TestOperationExecutor([new BsonDocument('ok', new BsonInt32(1))]);
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        collection.setReadPreference(ReadPreference.secondary())

        when:
        collection.getStats()

        then:
        expect executor.getReadOperation(), isTheSameAs(new CommandReadOperation('myDatabase',
                                                                                 new BsonDocument('collStats', new BsonString('test')),
                                                                new BsonDocumentCodec()))
        executor.getReadPreference() == collection.getReadPreference()
    }

    def 'findOne should create the correct FindOperation'() {
        given:
        def dbObject = new BasicDBObject('_id', 1)
        def executor = new TestOperationExecutor([Stub(BatchCursor) {
            next() >> {
                [dbObject]
            }
            hasNext() >> {
                true
            }
        }]);
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        collection.setReadConcern(ReadConcern.MAJORITY)

        when:
        def one = collection.findOne()

        then:
        one == dbObject
        expect executor.getReadOperation(), isTheSameAs(new FindOperation(collection.getNamespace(), collection.getObjectCodec())
                                                                .readConcern(ReadConcern.MAJORITY)
                                                                .filter(new BsonDocument())
                                                                .limit(-1))
    }

    def 'count should create the correct CountOperation'() {
        given:
        def executor = new TestOperationExecutor([42L]);
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        collection.setReadConcern(ReadConcern.MAJORITY)

        when:
        def count = collection.count

        then:
        count == 42L
        expect executor.getReadOperation(), isTheSameAs(new CountOperation(collection.getNamespace())
                                                                .readConcern(ReadConcern.MAJORITY)
                                                                .filter(new BsonDocument())
        )
    }

    def 'distinct should create the correct DistinctOperation'() {
        given:
        def executor = new TestOperationExecutor([Stub(BatchCursor) {
            def count = 0
            next() >> {
                count++
                [new BsonInt32(1), new BsonInt32(2)]
            }
            hasNext() >> {
                count == 0
            }
        }]);
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        collection.setReadConcern(ReadConcern.MAJORITY)

        when:
        def distinctFieldValues = collection.distinct('field1')

        then:
        distinctFieldValues == [1, 2]
        expect executor.getReadOperation(), isTheSameAs(new DistinctOperation(collection.getNamespace(), 'field1', new BsonValueCodec())
                                                                .readConcern(ReadConcern.MAJORITY)
                                                                .filter(new BsonDocument())
        )
    }

    def 'mapReduce should create the correct MapReduceOperation'() {
        given:
        def dbObject = new BasicDBObject('_id', 1)
        def executor = new TestOperationExecutor([Stub(MapReduceBatchCursor) {
            def count = 0
            next() >> {
                count++
                [dbObject]
            }
            hasNext() >> {
                count == 0
            }
        }])
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        collection.setReadConcern(ReadConcern.MAJORITY)

        when:
        def mapReduceOutput = collection.mapReduce('map', 'reduce', null, MapReduceCommand.OutputType.INLINE, new BasicDBObject())

        then:
        mapReduceOutput.results() == [dbObject]
        expect executor.getReadOperation(), isTheSameAs(new MapReduceWithInlineResultsOperation(collection.getNamespace(),
                                                                                                new BsonJavaScript('map'),
                                                                                                new BsonJavaScript('reduce'),
                                                                                                collection.getDefaultDBObjectCodec())
                                                                .readConcern(ReadConcern.MAJORITY)
                                                                .verbose(true)
                                                                .filter(new BsonDocument())
        )
    }

    def 'aggregate should create the correct AggregateOperation'() {
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
        }])
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        collection.setReadConcern(ReadConcern.MAJORITY)

        when:
        def aggregationOutput = collection.aggregate([new BasicDBObject('$match', new BasicDBObject())])

        then:
        aggregationOutput.results() == [dbObject]
        expect executor.getReadOperation(), isTheSameAs(new AggregateOperation(collection.getNamespace(),
                                                                               [new BsonDocument('$match', new BsonDocument())],
                                                                               collection.getObjectCodec())
                                                                .readConcern(ReadConcern.MAJORITY)
                                                                .useCursor(false)
        )
    }

    def 'parallel should create the correct ParallelCollectionScanOperation'() {
        given:
        def executor = new TestOperationExecutor([[Stub(BatchCursor) {
            hasNext() >> {
                false
            }
        }]])
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        collection.setReadConcern(ReadConcern.MAJORITY)

        when:
        def cursors = collection.parallelScan(ParallelScanOptions.builder().build())

        then:
        cursors.size() == 1
        expect executor.getReadOperation(), isTheSameAs(new ParallelCollectionScanOperation(collection.getNamespace(), 1,
                                                                                            collection.getObjectCodec())
                                                                .readConcern(ReadConcern.MAJORITY)
        )
    }

}
