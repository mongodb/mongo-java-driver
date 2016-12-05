/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

class CollationSpecification extends Specification {

    def 'should have null values as default'() {
        when:
        def options = Collation.builder().build()

        then:
        options.getAlternate() == null
        options.getBackwards() == null
        options.getCaseFirst() == null
        options.getCaseLevel() == null
        options.getLocale() == null
        options.getMaxVariable() == null
        options.getNormalization() == null
        options.getNumericOrdering() == null
        options.getStrength() == null
    }

    def 'should have the set values as passed to the builder'() {
        when:
        def options = Collation.builder()
                .locale('en')
                .caseLevel(true)
                .collationCaseFirst(CollationCaseFirst.OFF)
                .collationStrength(CollationStrength.IDENTICAL)
                .numericOrdering(true)
                .collationAlternate(CollationAlternate.SHIFTED)
                .collationMaxVariable(CollationMaxVariable.SPACE)
                .backwards(true)
                .normalization(true)
                .build()

        then:
        options.getAlternate() == CollationAlternate.SHIFTED
        options.getBackwards() == true
        options.getCaseFirst() == CollationCaseFirst.OFF
        options.getCaseLevel() == true
        options.getLocale() == 'en'
        options.getMaxVariable() == CollationMaxVariable.SPACE
        options.getNormalization() == true
        options.getNumericOrdering() == true
        options.getStrength() == CollationStrength.IDENTICAL
    }

    def 'should create the expected BsonDocument'() {
        expect:
        collation.asDocument() == BsonDocument.parse(json)

        where:
        collation                                | json
        Collation.builder().build()              | '{}'
        Collation.builder().locale('en').build() | '{locale: "en"}'
        Collation.builder()
                .locale('en')
                .caseLevel(true)
                .collationCaseFirst(CollationCaseFirst.OFF)
                .collationStrength(CollationStrength.IDENTICAL)
                .numericOrdering(true)
                .collationAlternate(CollationAlternate.SHIFTED)
                .collationMaxVariable(CollationMaxVariable.SPACE)
                .normalization(true)
                .backwards(true)
                .build()                         | '''{locale: "en", caseLevel: true, caseFirst: "off", strength: 5,
                                                       numericOrdering: true, alternate: "shifted",
                                                       maxVariable: "space", normalization: true, backwards: true}'''
    }


}
