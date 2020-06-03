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

import com.mongodb.client.model.Collation
import com.mongodb.client.model.CollationAlternate
import com.mongodb.client.model.CollationCaseFirst
import com.mongodb.client.model.CollationMaxVariable
import com.mongodb.client.model.CollationStrength
import com.mongodb.internal.bulk.IndexRequest
import org.bson.BsonDocument
import org.bson.BsonInt32
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
        request.getPartialFilterExpression() == null
        request.getCollation() == null
        request.getWildcardProjection() == null
        !request.isHidden()

        when:
        def keys = BsonDocument.parse('{ a: 1 }')
        def weights = BsonDocument.parse('{ a: 1000 }')
        def storageEngine = BsonDocument.parse('{ wiredTiger : { configString : "block_compressor=zlib" }}')
        def partialFilterExpression = BsonDocument.parse('{ a: { $gte: 10 } }')
        def collation = Collation.builder()
                .locale('en')
                .caseLevel(true)
                .collationCaseFirst(CollationCaseFirst.OFF)
                .collationStrength(CollationStrength.IDENTICAL)
                .numericOrdering(true)
                .collationAlternate(CollationAlternate.SHIFTED)
                .collationMaxVariable(CollationMaxVariable.SPACE)
                .backwards(true)
                .build()
        def wildcardProjection = BsonDocument.parse('{a  : 1}')
        def request2 = new IndexRequest(keys)
                .background(true)
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
                .dropDups(true)
                .storageEngine(storageEngine)
                .partialFilterExpression(partialFilterExpression)
                .collation(collation)
                .wildcardProjection(wildcardProjection)
                .hidden(true)

        then:
        request2.getKeys() == keys
        request2.isBackground()
        request2.isUnique()
        request2.isSparse()
        request2.getName() == 'aIndex'
        request2.getExpireAfter(TimeUnit.SECONDS) == 100
        request2.getVersion() == 1
        request2.getWeights() == weights
        request2.getDefaultLanguage() == 'es'
        request2.getLanguageOverride() == 'language'
        request2.getTextVersion() == 1
        request2.getSphereVersion() == 2
        request2.getBits() == 1
        request2.getMin() == -180.0
        request2.getMax() == 180.0
        request2.getBucketSize() == 200.0
        request2.getDropDups()
        request2.getStorageEngine() == storageEngine
        request2.getPartialFilterExpression() == partialFilterExpression
        request2.getCollation() == collation
        request2.getWildcardProjection() == wildcardProjection
        request2.isHidden()
    }


    def 'should validate textIndexVersion'() {
        given:
        def options = new IndexRequest(new BsonDocument('a', new BsonInt32(1)))

        when:
        options.textVersion(1)

        then:
        notThrown(IllegalArgumentException)

        when:
        options.textVersion(2)

        then:
        notThrown(IllegalArgumentException)

        when:
        options.textVersion(3)

        then:
        notThrown(IllegalArgumentException)

        when:
        options.textVersion(4)

        then:
        thrown(IllegalArgumentException)
    }


    def 'should validate 2dsphereIndexVersion'() {
        given:
        def options = new IndexRequest(new BsonDocument('a', new BsonInt32(1)))

        when:
        options.sphereVersion(1)

        then:
        notThrown(IllegalArgumentException)

        when:
        options.sphereVersion(2)

        then:
        notThrown(IllegalArgumentException)

        when:
        options.sphereVersion(3)

        then:
        notThrown(IllegalArgumentException)

        when:
        options.sphereVersion(4)

        then:
        thrown(IllegalArgumentException)
    }

}
