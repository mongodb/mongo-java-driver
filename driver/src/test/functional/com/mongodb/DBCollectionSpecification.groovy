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

import com.mongodb.operation.CreateIndexOperation
import org.bson.BsonDocument
import org.bson.BsonInt32
import spock.lang.Specification

import static com.mongodb.Fixture.getMongoClient

class DBCollectionSpecification extends Specification {

    def 'should use CreateIndexOperation properly'() {

        given:
        def executor = new TestOperationExecutor([null, null]);
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        def keys = new BasicDBObject('a', 1);

        when:
        collection.createIndex(keys)

        then:
        def operation = executor.getWriteOperation() as CreateIndexOperation
        operation.getKey() == new BsonDocument('a', new BsonInt32(1))
        !operation.isBackground()
        !operation.isUnique()
        !operation.isSparse()
        operation.getName() == null
        operation.getExpireAfterSeconds() == null
        operation.getVersion() == null
        operation.getWeights() == null
        operation.getDefaultLanguage() == null
        operation.getLanguageOverride() == null
        operation.getTextIndexVersion() == null
        operation.getTwoDSphereIndexVersion() == null
        operation.getBits() == null
        operation.getMin() == null
        operation.getMax() == null
        operation.getBucketSize() == null
        !operation.getDropDups()

        when:
        collection.createIndex(keys, new BasicDBObject(['background': true, 'unique': true, 'sparse': true, 'name': 'aIndex',
                                                      'expireAfterSeconds': 100, 'v': 1, 'weights': new BasicDBObject(['a': 1000]),
                                                      'default_language': 'es', 'language_override': 'language', 'textIndexVersion': 1,
                                                      '2dsphereIndexVersion': 1, 'bits': 1, 'min': new Double(-180.0),
                                                      'max': new Double(180.0), 'bucketSize': new Double(200.0), 'dropDups': true]))

        then:
        def operation2 = executor.getWriteOperation() as CreateIndexOperation
        operation2.getKey() == new BsonDocument('a', new BsonInt32(1))
        operation2.isBackground()
        operation2.isUnique()
        operation2.isSparse()
        operation2.getName() == 'aIndex'
        operation2.getExpireAfterSeconds() == 100
        operation2.getVersion() == 1
        operation2.getWeights() == new BsonDocument('a', new BsonInt32(1000))
        operation2.getDefaultLanguage() == 'es'
        operation2.getLanguageOverride() == 'language'
        operation2.getTextIndexVersion() == 1
        operation2.getTwoDSphereIndexVersion() == 1
        operation2.getBits() == 1
        operation2.getMin() == -180.0
        operation2.getMax() == 180.0
        operation2.getBucketSize() == 200.0
        operation2.getDropDups()
    }

    def 'should support boolean index options that are numbers'() {
        given:
        def executor = new TestOperationExecutor([null, null]);
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        def options = new BasicDBObject('sparse', value);

        when:
        collection.createIndex(new BasicDBObject('y', 1), options);

        then:
        def operation = executor.getWriteOperation() as CreateIndexOperation
        operation.sparse == expectedValue

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
        def operation = executor.getWriteOperation() as CreateIndexOperation
        operation.expireAfterSeconds == integerValue

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
        def operation = executor.getWriteOperation() as CreateIndexOperation
        operation.max == doubleValue

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
