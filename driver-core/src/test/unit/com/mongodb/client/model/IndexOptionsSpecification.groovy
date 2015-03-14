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

package com.mongodb.client.model

import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import spock.lang.Specification

import java.util.concurrent.TimeUnit

class IndexOptionsSpecification extends Specification {

    def 'should set options correctly'() {
        when:
        def options = new IndexOptions()

        then:
        !options.isBackground()
        !options.isUnique()
        !options.isSparse()
        options.getName() == null
        options.getExpireAfter(TimeUnit.SECONDS) == null
        options.getVersion() == null
        options.getWeights() == null
        options.getDefaultLanguage() == null
        options.getLanguageOverride() == null
        options.getTextVersion() == null
        options.getSphereVersion() == null
        options.getBits() == null
        options.getMin() == null
        options.getMax() == null
        options.getBucketSize() == null
        options.getStorageEngine() == null

        when:
        def options2 = new IndexOptions()
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
                .storageEngine(new BsonDocument('wiredTiger',
                                                new BsonDocument('configString', new BsonString('block_compressor=zlib'))))

        then:
        options2.isBackground()
        options2.isUnique()
        options2.isSparse()
        options2.getName() == 'aIndex'
        options2.getExpireAfter(TimeUnit.SECONDS) == 100
        options2.getVersion() == 1
        options2.getWeights() == new BsonDocument('a', new BsonInt32(1000))
        options2.getDefaultLanguage() == 'es'
        options2.getLanguageOverride() == 'language'
        options2.getTextVersion() == 1
        options2.getSphereVersion() == 2
        options2.getBits() == 1
        options2.getMin() == -180.0
        options2.getMax() == 180.0
        options2.getBucketSize() == 200.0
    }

    def 'should validate textIndexVersion'() {
        when:
        new IndexOptions().textVersion(1)

        then:
        notThrown(IllegalArgumentException)

        when:
        new IndexOptions().textVersion(2)

        then:
        notThrown(IllegalArgumentException)

        when:
        new IndexOptions().textVersion(3)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should validate 2dsphereIndexVersion'() {
        when:
        new IndexOptions().sphereVersion(1)

        then:
        notThrown(IllegalArgumentException)

        when:
        new IndexOptions().sphereVersion(2)

        then:
        notThrown(IllegalArgumentException)

        when:
        new IndexOptions().sphereVersion(3)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should convert expireAfter'() {
        when:
        def options = new IndexOptions()

        then:
        !options.getExpireAfter(TimeUnit.SECONDS)

        when:
        options = new IndexOptions().expireAfter(null, null)

        then:
        !options.getExpireAfter(TimeUnit.SECONDS)

        when:
        options = new IndexOptions().expireAfter(4, TimeUnit.MILLISECONDS)

        then:
        options.getExpireAfter(TimeUnit.SECONDS) == 0

        when:
        options = new IndexOptions().expireAfter(1004, TimeUnit.MILLISECONDS)

        then:
        options.getExpireAfter(TimeUnit.SECONDS) == 1

    }
}