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

package com.mongodb.client.model

import org.bson.BsonDocument
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
        options.getPartialFilterExpression() == null
        options.getCollation() == null

        when:
        def weights = BsonDocument.parse('{ a: 1000 }')
        def storageEngine = BsonDocument.parse('{ wiredTiger : { configString : "block_compressor=zlib" }}')
        def partialFilterExpression = BsonDocument.parse('{ a: { $gte: 10 } }')
        def collation = Collation.builder().locale('en').build()
        options.background(true)
                .unique(true)
                .sparse(true)
                .name('aIndex')
                .expireAfter(100, TimeUnit.SECONDS)
                .version(1)
                .weights(weights)
                .defaultLanguage('es')
                .languageOverride('language')
                .textVersion(1)
                .sphereVersion(2)
                .bits(1)
                .min(-180.0)
                .max(180.0)
                .bucketSize(200.0)
                .storageEngine(storageEngine)
                .partialFilterExpression(partialFilterExpression)
                .collation(collation)

        then:
        options.isBackground()
        options.isUnique()
        options.isSparse()
        options.getName() == 'aIndex'
        options.getExpireAfter(TimeUnit.SECONDS) == 100
        options.getVersion() == 1
        options.getWeights() == weights
        options.getDefaultLanguage() == 'es'
        options.getLanguageOverride() == 'language'
        options.getTextVersion() == 1
        options.getSphereVersion() == 2
        options.getBits() == 1
        options.getMin() == -180.0
        options.getMax() == 180.0
        options.getBucketSize() == 200.0
        options.getStorageEngine() == storageEngine
        options.getPartialFilterExpression() == partialFilterExpression
        options.getCollation() == collation
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
