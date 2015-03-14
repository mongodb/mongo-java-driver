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
import com.mongodb.operation.CreateIndexesOperation
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.Fixture.getMongoClient
import static spock.util.matcher.HamcrestSupport.expect

class DBCollectionSpecification extends Specification {

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
        collection.createIndex(keys, new BasicDBObject(['background': true, 'unique': true, 'sparse': true, 'name': 'aIndex',
                                                      'expireAfterSeconds': 100, 'v': 1, 'weights': new BasicDBObject(['a': 1000]),
                                                      'default_language': 'es', 'language_override': 'language', 'textIndexVersion': 1,
                                                      '2dsphereIndexVersion': 1, 'bits': 1, 'min': new Double(-180.0),
                                                      'max'          : new Double(180.0), 'bucketSize': new Double(200.0), 'dropDups': true,
                                                      'storageEngine': new BasicDBObject('wiredTiger',
                                                                                         new BasicDBObject('configString',
                                                                                                           'block_compressor=zlib'))]))
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
                                            .storageEngine(new BsonDocument('wiredTiger',
                                                                            new BsonDocument('configString', new BsonString(
                                                                                    'block_compressor=zlib')))))
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
}
