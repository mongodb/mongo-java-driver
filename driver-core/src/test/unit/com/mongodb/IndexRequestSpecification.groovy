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

import com.mongodb.bulk.IndexRequest
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import spock.lang.Specification

import java.util.concurrent.TimeUnit

class IndexRequestSpecification extends Specification {
    def 'should set its options correctly'() {
        when:
        def request = new IndexRequest(new BsonDocument('a', new BsonInt32(1)))

        then:
        request.getKeys() == new BsonDocument('a', new BsonInt32(1))
        !request.isBackground()
        !request.isUnique()
        !request.isSparse()
        request.getName() == null
        request.getExpireAfter(TimeUnit.SECONDS) == null
        request.getVersion() == null
        request.getWeights() == null
        request.getDefaultLanguage() == null
        request.getLanguageOverride() == null
        request.getTextVersion() == null
        request.getSphereVersion() == null
        request.getBits() == null
        request.getMin() == null
        request.getMax() == null
        request.getBucketSize() == null
        !request.getDropDups()
        request.getStorageEngine() == null

        when:
        def request2 = new IndexRequest(new BsonDocument('a', new BsonInt32(1)))
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
                .sphereVersion(2)
                .bits(1)
                .min(-180.0)
                .max(180.0)
                .bucketSize(200.0)
                .dropDups(true)
                .storageEngine(new BsonDocument('wiredTiger',
                                                new BsonDocument('configString', new BsonString('block_compressor=zlib'))))

        then:
        request2.getKeys() == new BsonDocument('a', new BsonInt32(1))
        request2.isBackground()
        request2.isUnique()
        request2.isSparse()
        request2.getName() == 'aIndex'
        request2.getExpireAfter(TimeUnit.SECONDS) == 100
        request2.getVersion() == 1
        request2.getWeights() == new BsonDocument('a', new BsonInt32(1000))
        request2.getDefaultLanguage() == 'es'
        request2.getLanguageOverride() == 'language'
        request2.getTextVersion() == 1
        request2.getSphereVersion() == 2
        request2.getBits() == 1
        request2.getMin() == -180.0
        request2.getMax() == 180.0
        request2.getBucketSize() == 200.0
        request2.getDropDups()
        request2.getStorageEngine() == new BsonDocument('wiredTiger',
                                                        new BsonDocument('configString', new BsonString('block_compressor=zlib')))
    }

}